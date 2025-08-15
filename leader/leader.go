package leader

import (
	"distributed_task_scheduler/conf"
	"github.com/redis/go-redis/v9"
	"log"
)

type LeaderElection interface {
	Start() error
	IsLeader() bool
	GetLeader() string
	Stop()
	NodeId() string
}

type ElectionService struct {
	strategy LeaderElection
}

func NewLeaderElectionService(cfg *conf.Config, redisClient *redis.Client) *ElectionService {
	var strategy LeaderElection
	if cfg.ConsensusType == "redis" {
		strategy = NewRedisLeaderElectionStrategy(redisClient, cfg.NodeID)
	} else if cfg.ConsensusType == "raft" {
		le, err := NewRaftLeaderElectionStrategy(cfg.RaftDataDir, cfg.NodeID, cfg.RaftBindAddr, cfg.Peers)
		if err != nil {
			log.Fatalf("Failed to create Raft leader election: %v", err)
		}
		strategy = le
	} else {
		log.Fatalf("Unsupported leader election type: %s", cfg.ConsensusType)
	}
	return &ElectionService{strategy: strategy}
}

func (s *ElectionService) Start() error {
	return s.strategy.Start()
}

func (s *ElectionService) Stop() {
	s.strategy.Stop()
}

func (s *ElectionService) IsLeader() bool {
	return s.strategy.IsLeader()
}

func (s *ElectionService) GetLeader() string {
	return s.strategy.GetLeader()
}

func (s *ElectionService) NodeId() string {
	return s.strategy.NodeId()
}
