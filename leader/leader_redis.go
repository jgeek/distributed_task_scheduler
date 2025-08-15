package leader

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
	"time"
)

const (
	redisLeaderKey = "scheduler:leader"
	leaderTTL      = 5 * time.Second // Leader heartbeat TTL
)

type RedisLeaderElectionStrategy struct {
	client *redis.Client
	nodeID string
	leader bool
	mu     sync.Mutex
	stop   chan struct{}
}

func NewRedisLeaderElectionStrategy(client *redis.Client, nodeID string) *RedisLeaderElectionStrategy {
	return &RedisLeaderElectionStrategy{
		client: client,
		nodeID: nodeID,
		leader: false,
		stop:   make(chan struct{}),
	}
}

// Start launches the leader election loop in a separate goroutine.
// It periodically attempts to become the leader if not already, or sends heartbeats if leader.
// Leadership changes are logged. The loop stops when Stop() is called.
func (r *RedisLeaderElectionStrategy) Start() error {
	tryElection := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		prevLeader := r.leader
		if !prevLeader {
			r.leader = r.TryBecomeLeader()
			if r.leader && !prevLeader {
				log.Printf("Node %s became leader", r.nodeID)
			}
		} else {
			r.leader = r.Heartbeat()
			if !r.leader && prevLeader {
				log.Printf("Node %s lost leadership", r.nodeID)
			}
		}
		return r.leader
	}

	// Try to become leader or follower, but do not fail if not leader
	for i := 0; i < 5; i++ { // Try for up to 10 seconds (5 * 2s)
		select {
		case <-r.stop:
			return nil // Exit the loop if stop signal is received
		default:
			tryElection()
			time.Sleep(2 * time.Second)
		}
	}
	// Continue running election loop in background
	go func() {
		for {
			select {
			case <-r.stop:
				return
			default:
				tryElection()
				time.Sleep(2 * time.Second)
			}
		}
	}()
	return nil
}

func (r *RedisLeaderElectionStrategy) TryBecomeLeader() bool {
	ctx := context.Background()
	ok, err := r.client.SetNX(ctx, redisLeaderKey, r.nodeID, leaderTTL).Result()
	if err != nil {
		return false
	}
	r.leader = ok
	return ok
}

// Heartbeat Renew leader heartbeat if this node is leader
func (r *RedisLeaderElectionStrategy) Heartbeat() bool {
	ctx := context.Background()
	val, err := r.client.Get(ctx, redisLeaderKey).Result()
	if err != nil || val != r.nodeID {
		r.leader = false
		return false
	}
	// Extend TTL
	r.client.Expire(ctx, redisLeaderKey, leaderTTL)
	r.leader = true
	return true
}

func (r *RedisLeaderElectionStrategy) GetLeader() string {
	ctx := context.Background()
	result, err := r.client.Get(ctx, redisLeaderKey).Result()
	//FIXME: Handle error properly
	if err != nil {
		return ""
	}
	return result
}

func (r *RedisLeaderElectionStrategy) StepDown() error {
	ctx := context.Background()
	val, err := r.client.Get(ctx, redisLeaderKey).Result()
	if err == nil && val == r.nodeID {
		return r.client.Del(ctx, redisLeaderKey).Err()
	}
	return nil
}

func (r *RedisLeaderElectionStrategy) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
}

func (r *RedisLeaderElectionStrategy) Stop() {
	close(r.stop)
}

func (r *RedisLeaderElectionStrategy) NodeId() string {
	return r.nodeID
}
