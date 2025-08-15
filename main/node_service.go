package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type NodeService struct {
	NodeID        string
	taskService   *TaskService
	leaderService *LeaderElectionService
	pool          *WorkerPool
}

func NewNodeService(cfg *Config, nodeId string, redisClient *redis.Client, taskService *TaskService, pool *WorkerPool) *NodeService {
	leaderService := NewLeaderElectionService(cfg, redisClient)
	leaderService.Start()
	return &NodeService{
		NodeID:        nodeId,
		taskService:   taskService,
		leaderService: leaderService,
		pool:          pool,
	}
}

func (n *NodeService) Close() {
	n.leaderService.Stop()
	// Add any other cleanup if needed
}

func (n *NodeService) SubmitTask(priority string, payload []byte) (string, error) {
	return n.taskService.SubmitTask(priority, payload)
}

func (n *NodeService) GetTaskStatus(id string) (string, error) {
	return n.taskService.GetTaskStatus(id)
}

func (n *NodeService) QueueLen() int {
	return n.taskService.QueueLen()
}

func (n *NodeService) IsLeader() bool {
	return n.leaderService.IsLeader()
}

func (n *NodeService) LeaderID() string {
	return n.leaderService.GetLeader()
}

func (n *NodeService) LoadPendingTasks() {
	pendingTasks, err := n.taskService.store.LoadPendingTasks()
	if err == nil {
		for _, task := range pendingTasks {
			n.taskService.queue.SafePush(task)
		}
		log.Printf("Loaded %d pending tasks from Redis", len(pendingTasks))
	}
}

func (n *NodeService) StartWorkerPool(ctx context.Context, nodeID string) {

	// Task processing function
	taskProcessor := func(task *Task) error {
		log.Printf("Processing task %s with priority %d", task.ID, task.Priority)
		// Simulate task processing
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	var workerStarted bool
	var workerMu sync.Mutex
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				workerMu.Lock()
				if n.IsLeader() && !workerStarted {
					log.Printf("Node %s became leader, starting worker pool", nodeID)
					n.pool.Start(taskProcessor)
					workerStarted = true
				} else if !n.IsLeader() && workerStarted {
					log.Printf("Node %s lost leadership, stopping worker pool", nodeID)
					n.pool.Stop()
					workerStarted = false
				}
				workerMu.Unlock()
			}
		}
	}()
}
