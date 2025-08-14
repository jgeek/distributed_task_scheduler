package main

import (
	"github.com/redis/go-redis/v9"
	"log"
)

// LeaderElection defines the interface for leader election strategies
// (Strategy Pattern)
type LeaderElection interface {
	Start()
	IsLeader() bool
	GetLeader() string
	Stop()
}

// LeaderElectionService wraps the leader election strategy and exposes its API
// for use by the rest of the application.
type LeaderElectionService struct {
	strategy LeaderElection
}

func NewLeaderElectionService(cfg *Config, redisClient *redis.Client) *LeaderElectionService {
	var strategy LeaderElection
	if cfg.LeaderType == "redis" {
		strategy = NewRedisLeaderElectionStrategy(redisClient, cfg.NodeID)
	} else {
		le, err := NewRaftLeaderElectionStrategy(cfg.RaftDataDir, cfg.NodeID, cfg.RaftBindAddr, []string{})
		if err != nil {
			log.Fatalf("Failed to create Raft leader election: %v", err)
		}
		strategy = le
	}
	return &LeaderElectionService{strategy: strategy}
}

func (s *LeaderElectionService) Start() {
	s.strategy.Start()
}

func (s *LeaderElectionService) Stop() {
	s.strategy.Stop()
}

func (s *LeaderElectionService) IsLeader() bool {
	return s.strategy.IsLeader()
}

func (s *LeaderElectionService) GetLeader() string {
	return s.strategy.GetLeader()
}
