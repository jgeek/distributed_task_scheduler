package leader

import (
	"distributed_task_scheduler/conf"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftLeaderElectionStrategy struct {
	raftNode *raft.Raft
	nodeID   string
	dataDir  string
}

func (r *RaftLeaderElectionStrategy) Stop() {
	if r.raftNode != nil {
		// Gracefully shutdown Raft node
		future := r.raftNode.Shutdown()
		if err := future.Error(); err != nil {
			log.Printf("Error shutting down Raft node: %v", err)
		}
	}
}

// RaftFSM implements raft.FSM for our task scheduler
// For HashiCorp Raft, you must implement the raft.FSM interface,
// but for leader election only (without replicated state),
// a minimal implementation is sufficient. Your current RaftFSM implementation is enough for leader election:
// Apply, Snapshot, and Restore can be no-ops or stubs, as you have.
// Leader election will work correctly with this minimal FSM, as Raft only requires FSM for log/state management.
// If you later want to replicate state (e.g., tasks), you must implement these methods fully. For now, your implementation is correct for leader election only.
type RaftFSM struct{}

func (f *RaftFSM) Apply(log *raft.Log) interface{} {
	// For task scheduler, we don't need complex state machine
	// Just acknowledge the log entry
	return nil
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &RaftSnapshot{}, nil
}

func (f *RaftFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}

type RaftSnapshot struct{}

func (s *RaftSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *RaftSnapshot) Release() {}

func NewRaftLeaderElectionStrategy(dataDir, nodeID, bindAddr string, peers []conf.Peer) (*RaftLeaderElectionStrategy, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.CommitTimeout = 500 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// Check if Raft state files exist (logs.dat, stable.dat, snapshots)
	isNewCluster := true
	stateFiles := []string{"logs.dat", "stable.dat"}
	for _, fname := range stateFiles {
		if _, err := os.Stat(filepath.Join(dataDir, fname)); err == nil {
			isNewCluster = false
			break
		}
	}
	// Also check for any snapshot files
	snapshotsDir := filepath.Join(dataDir, "snapshots")
	if files, err := os.ReadDir(snapshotsDir); err == nil && len(files) > 0 {
		isNewCluster = false
	}

	// Create stores
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "logs.dat"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "stable.dat"))
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create FSM
	fsm := &RaftFSM{}

	// Create Raft node
	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// Use peers parameter to determine cluster membership
	if len(peers) > 0 && peers[0].ID == nodeID {
		// This node is the first in the list, bootstrap the cluster
		if isNewCluster {
			var servers []raft.Server
			for _, peer := range peers {
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(peer.ID),
					Address: raft.ServerAddress(peer.Addr),
				})
			}
			cfg := raft.Configuration{Servers: servers}
			future := raftNode.BootstrapCluster(cfg)
			if err := future.Error(); err != nil {
				panic("Failed to bootstrap Raft cluster: " + err.Error())
			}
		} else {
			log.Printf("Raft data dir %s already contains state, skipping bootstrap", dataDir)
		}
	} else if len(peers) > 0 {
		// Join the cluster by contacting the first node (the default contact point)
		contactNode := peers[0]
		if contactNode.ID != nodeID {
			go func() {
				time.Sleep(2 * time.Second)
				log.Printf("Attempting to join Raft cluster via contact node %s (%s)", contactNode.ID, contactNode.Addr)
				addVoterFuture := raftNode.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(bindAddr), 0, 0)
				if err := addVoterFuture.Error(); err != nil {
					log.Printf("Failed to join Raft cluster: %v", err)
				} else {
					log.Printf("Successfully joined Raft cluster as voter: %s", nodeID)
				}
			}()
		}
	}

	return &RaftLeaderElectionStrategy{
		raftNode: raftNode,
		nodeID:   nodeID,
		dataDir:  dataDir,
	}, nil
}

func (r *RaftLeaderElectionStrategy) Start() error {
	log.Println("Waiting for Raft leader election...")
	leader := r.WaitForLeader(10 * time.Second)
	if leader == "" {
		return fmt.Errorf("no leader elected within timeout, node startup failed")
	}
	log.Printf("Raft leader elected: %s", leader)
	return nil
}

func (r *RaftLeaderElectionStrategy) IsLeader() bool {
	return r.raftNode.State() == raft.Leader
}

func (r *RaftLeaderElectionStrategy) GetLeader() string {
	return string(r.raftNode.Leader())
}

func (r *RaftLeaderElectionStrategy) NodeId() string {
	return r.nodeID
}

func (r *RaftLeaderElectionStrategy) AddVoter(id, address string) error {
	future := r.raftNode.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 0)
	return future.Error()
}

func (r *RaftLeaderElectionStrategy) RemoveServer(id string) error {
	future := r.raftNode.RemoveServer(raft.ServerID(id), 0, 0)
	return future.Error()
}

func (r *RaftLeaderElectionStrategy) Shutdown() error {
	return r.raftNode.Shutdown().Error()
}

func (r *RaftLeaderElectionStrategy) WaitForLeader(timeout time.Duration) string {
	log.Printf("[Raft] Waiting for leader election (timeout: %s)...", timeout)
	// Wait for leadership to be established
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			log.Printf("[Raft] No leader elected after %s", timeout)
			return ""
		case <-ticker.C:
			leader := r.GetLeader()
			if leader != "" {
				log.Printf("[Raft] Leader detected: %s", leader)
				return leader
			}
		}
	}
}
