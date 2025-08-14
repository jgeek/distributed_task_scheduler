// Raft-based Leader Election using hashicorp/raft
// File: main/leader_raft.go

package main

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

// RaftLeaderElectionStrategy implements LeaderElection for Raft
// (adapts existing NewRaftLeaderElection)
type RaftLeaderElectionStrategy struct {
	le *RaftLeaderElection
}

func NewRaftLeaderElectionStrategy(dataDir, nodeID, bindAddr string, peers []string) (*RaftLeaderElectionStrategy, error) {
	le, err := NewRaftLeaderElection(dataDir, nodeID, bindAddr, peers)
	if err != nil {
		return nil, err
	}
	return &RaftLeaderElectionStrategy{le: le}, nil
}

func (r *RaftLeaderElectionStrategy) Start() {
	go func() {
		log.Println("Waiting for Raft leader election...")
		leader := r.le.WaitForLeader(10 * time.Second)
		if leader == "" {
			log.Println("No leader elected within timeout, continuing...")
		} else {
			log.Printf("Raft leader elected: %s", leader)
		}
	}()
}

func (r *RaftLeaderElectionStrategy) IsLeader() bool {
	return r.le.IsLeader()
}

func (r *RaftLeaderElectionStrategy) GetLeader() string {
	return r.le.GetLeader()
}

func (r *RaftLeaderElectionStrategy) Stop() {
	if err := r.le.Shutdown(); err != nil {
		log.Printf("Error shutting down Raft: %v", err)
	}
}

type RaftLeaderElection struct {
	raftNode *raft.Raft
	id       string
	dataDir  string
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

func NewRaftLeaderElection(dataDir, id, bindAddr string, peers []string) (*RaftLeaderElection, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
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

	// Bootstrap cluster if this is the first node
	if len(peers) == 0 || (len(peers) == 1 && peers[0] == id) {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(id),
					Address: raft.ServerAddress(bindAddr),
				},
			},
		}
		future := raftNode.BootstrapCluster(cfg)
		if err := future.Error(); err != nil {
			log.Printf("Failed to bootstrap cluster: %v", err)
		}
	}

	return &RaftLeaderElection{
		raftNode: raftNode,
		id:       id,
		dataDir:  dataDir,
	}, nil
}

func (le *RaftLeaderElection) IsLeader() bool {
	return le.raftNode.State() == raft.Leader
}

func (le *RaftLeaderElection) GetLeader() string {
	return string(le.raftNode.Leader())
}

func (le *RaftLeaderElection) AddVoter(id, address string) error {
	future := le.raftNode.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 0)
	return future.Error()
}

func (le *RaftLeaderElection) RemoveServer(id string) error {
	future := le.raftNode.RemoveServer(raft.ServerID(id), 0, 0)
	return future.Error()
}

func (le *RaftLeaderElection) Shutdown() error {
	return le.raftNode.Shutdown().Error()
}

func (le *RaftLeaderElection) WaitForLeader(timeout time.Duration) string {
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
			if leader := le.GetLeader(); leader != "" {
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
