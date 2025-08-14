// Redis-based Leader Election for Distributed Task Scheduler
// File: main/leader_redis.go

package main

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

// RedisLeaderElectionStrategy implements LeaderElection for Redis
// (adapts existing NewRedisLeaderElection)
type RedisLeaderElectionStrategy struct {
	le     *RedisLeaderElection
	mu     sync.Mutex
	leader bool
	stop   chan struct{}
	nodeID string
}

func NewRedisLeaderElectionStrategy(client *redis.Client, nodeID string) *RedisLeaderElectionStrategy {
	return &RedisLeaderElectionStrategy{
		le:     NewRedisLeaderElection(client, nodeID),
		leader: false,
		stop:   make(chan struct{}),
		nodeID: nodeID,
	}
}

func (r *RedisLeaderElectionStrategy) Start() {
	go func() {
		for {
			select {
			case <-r.stop:
				return
			default:
				r.mu.Lock()
				prevLeader := r.leader
				if !r.leader {
					r.leader = r.le.TryBecomeLeader()
					if r.leader && !prevLeader {
						log.Printf("Node %s became leader", r.nodeID)
					}
				} else {
					r.leader = r.le.Heartbeat()
					if !r.leader && prevLeader {
						log.Printf("Node %s lost leadership", r.nodeID)
					}
				}
				r.mu.Unlock()
				time.Sleep(2 * time.Second)
			}
		}
	}()
}

func (r *RedisLeaderElectionStrategy) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
}

func (r *RedisLeaderElectionStrategy) GetLeader() string {
	l, _ := r.le.GetLeader()
	return l
}

func (r *RedisLeaderElectionStrategy) Stop() {
	close(r.stop)
}

type RedisLeaderElection struct {
	client   *redis.Client
	id       string // Node ID
	isLeader bool
}

func NewRedisLeaderElection(client *redis.Client, id string) *RedisLeaderElection {
	return &RedisLeaderElection{client: client, id: id}
}

// Try to become leader by setting redisLeaderKey with NX (set if not exists)
func (le *RedisLeaderElection) TryBecomeLeader() bool {
	ctx := context.Background()
	ok, err := le.client.SetNX(ctx, redisLeaderKey, le.id, leaderTTL).Result()
	if err != nil {
		return false
	}
	le.isLeader = ok
	return ok
}

// Renew leader heartbeat if this node is leader
func (le *RedisLeaderElection) Heartbeat() bool {
	ctx := context.Background()
	val, err := le.client.Get(ctx, redisLeaderKey).Result()
	if err != nil || val != le.id {
		le.isLeader = false
		return false
	}
	// Extend TTL
	le.client.Expire(ctx, redisLeaderKey, leaderTTL)
	le.isLeader = true
	return true
}

// Check current leader
func (le *RedisLeaderElection) GetLeader() (string, error) {
	ctx := context.Background()
	return le.client.Get(ctx, redisLeaderKey).Result()
}

// Step down as leader
func (le *RedisLeaderElection) StepDown() error {
	ctx := context.Background()
	val, err := le.client.Get(ctx, redisLeaderKey).Result()
	if err == nil && val == le.id {
		return le.client.Del(ctx, redisLeaderKey).Err()
	}
	return nil
}
