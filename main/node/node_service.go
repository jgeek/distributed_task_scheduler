package node

import (
	"context"
	"distributed_task_scheduler/main/leader"
	"distributed_task_scheduler/main/task"
	"log"
	"sync"
	"time"
)

type NodeService struct {
	taskService   *task.TaskService
	leaderService *leader.LeaderElectionService
	pool          *WorkerPool
}

func NewNodeService(taskService *task.TaskService, pool *WorkerPool, leaderService *leader.LeaderElectionService) *NodeService {
	leaderService.Start()
	return &NodeService{
		taskService:   taskService,
		leaderService: leaderService,
		pool:          pool,
	}
}

func (n *NodeService) Close() {
	n.leaderService.Stop()
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
	pendingTasks, err := n.taskService.Store.LoadPendingTasks()
	if err == nil {
		for _, task := range pendingTasks {
			n.taskService.Queue.SafePush(task)
		}
		log.Printf("Loaded %d pending tasks from Redis", len(pendingTasks))
	}
}

func (n *NodeService) StartWorkerPool(ctx context.Context, nodeID string) {

	// Task processing function
	taskProcessor := func(task *task.Task) error {
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
					log.Printf("Node %s became leader, starting worker pool", n.leaderService.NodeId())
					n.pool.Start(taskProcessor)
					workerStarted = true
				} else if !n.IsLeader() && workerStarted {
					log.Printf("Node %s lost leadership, stopping worker pool", n.leaderService.NodeId())
					n.pool.Stop()
					workerStarted = false
				}
				workerMu.Unlock()
			}
		}
	}()
}
