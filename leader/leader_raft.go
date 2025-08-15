package leader

import (
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

func NewRaftLeaderElectionStrategy(dataDir, nodeID, bindAddr string, peers []string) (*RaftLeaderElectionStrategy, error) {
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
	if len(peers) > 0 && peers[0] == nodeID {
		// This node is the first in the list, bootstrap the cluster
		var servers []raft.Server
		for _, peer := range peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(bindAddr), // Assumes all peers use the same bindAddr pattern
			})
		}
		cfg := raft.Configuration{Servers: servers}
		future := raftNode.BootstrapCluster(cfg)
		if err := future.Error(); err != nil {
			log.Printf("Failed to bootstrap cluster: %v", err)
		}
	} else if len(peers) > 0 {
		// Join the cluster by contacting the first node (the default leader)
		leaderID := peers[0]
		if leaderID != nodeID {
			go func() {
				time.Sleep(2 * time.Second)
				log.Printf("Attempting to join Raft cluster at leader %s", leaderID)
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

func (r *RaftLeaderElectionStrategy) Start() {
	go func() {
		log.Println("Waiting for Raft leader election...")
		leader := r.WaitForLeader(10 * time.Second)
		if leader == "" {
			log.Println("No leader elected within timeout, continuing...")
		} else {
			log.Printf("Raft leader elected: %s", leader)
		}
	}()
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
	// Wait for leadership to be established
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return ""
		case <-ticker.C:
			if leader := r.GetLeader(); leader != "" {
				return leader
			}
		}
	}
}

// Example usage:
// dataDir := filepath.Join("/tmp", id)
// peers := []string{"node1", "node2"}
// le, err := NewRaftLeaderElection(dataDir, id, peers)
// if err != nil { log.Fatal(err) }
// leader := le.GetLeader()
