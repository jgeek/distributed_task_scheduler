package leader

import (
	"distributed_task_scheduler/conf"
	"github.com/redis/go-redis/v9"
	"log"
)

type Strategy interface {
	Start() error
	IsLeader() bool
	GetLeader() string
	Stop()
	NodeId() string
}

func NewLeaderElectionService(cfg *conf.Config, redisClient *redis.Client) Strategy {
	var strategy Strategy
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
	return strategy
}
