package leader

import (
	"distributed_task_scheduler/conf"
	"github.com/redis/go-redis/v9"
	"log"
)

type LeaderElection interface {
	Start()
	IsLeader() bool
	GetLeader() string
	Stop()
	NodeId() string
}

type LeaderElectionService struct {
	strategy LeaderElection
}

func NewLeaderElectionService(cfg *conf.Config, redisClient *redis.Client) *LeaderElectionService {
	var strategy LeaderElection
	if cfg.ConsensusType == "redis" {
		strategy = NewRedisLeaderElectionStrategy(redisClient, cfg.NodeID)
	} else if cfg.ConsensusType == "raft" {
		le, err := NewRaftLeaderElectionStrategy(cfg.RaftDataDir, cfg.NodeID, cfg.RaftBindAddr, []string{})
		if err != nil {
			log.Fatalf("Failed to create Raft leader election: %v", err)
		}
		strategy = le
	} else {
		log.Fatalf("Unsupported leader election type: %s", cfg.ConsensusType)
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

func (s *LeaderElectionService) NodeId() string {
	return s.strategy.NodeId()
}
