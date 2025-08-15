package node

import (
	"context"
	"distributed_task_scheduler/leader"
	task2 "distributed_task_scheduler/task"
	"log"
	"sync"
	"time"
)

type Service struct {
	taskService   *task2.TaskService
	leaderService *leader.LeaderElectionService
	pool          *WorkerPool
}

func NewService(taskService *task2.TaskService, pool *WorkerPool, leaderService *leader.LeaderElectionService) *Service {
	leaderService.Start()
	return &Service{
		taskService:   taskService,
		leaderService: leaderService,
		pool:          pool,
	}
}

func (s *Service) Close() {
	s.leaderService.Stop()
}

func (s *Service) SubmitTask(priority string, payload []byte) (string, error) {
	return s.taskService.SubmitTask(priority, payload)
}

func (s *Service) GetTaskStatus(id string) (string, error) {
	return s.taskService.GetTaskStatus(id)
}

func (s *Service) QueueLen() int {
	return s.taskService.QueueLen()
}

func (s *Service) IsLeader() bool {
	return s.leaderService.IsLeader()
}

func (s *Service) LeaderID() string {
	return s.leaderService.GetLeader()
}

func (s *Service) LoadPendingTasks() {
	pendingTasks, err := s.taskService.Store.LoadPendingTasks()
	if err == nil {
		for _, t := range pendingTasks {
			s.taskService.Queue.SafePush(t)
		}
		log.Printf("Loaded %d pending tasks from Redis", len(pendingTasks))
	}
}

func (s *Service) StartWorkerPool(ctx context.Context, nodeID string) {

	var workerStarted bool
	var workerMu sync.Mutex

	taskProcessor := func(task *task2.Task) error {
		log.Printf("Processing task %s with priority %d", task.ID, task.Priority)
		// Simulate task processing
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				workerMu.Lock()
				if s.IsLeader() && !workerStarted {
					log.Printf("Node %s became leader, starting worker pool", s.leaderService.NodeId())
					s.pool.Start(taskProcessor)
					workerStarted = true
				} else if !s.IsLeader() && workerStarted {
					log.Printf("Node %s lost leadership, stopping worker pool", s.leaderService.NodeId())
					s.pool.Stop()
					workerStarted = false
				}
				workerMu.Unlock()
			}
		}
	}()

	s.loadNewTaskFromRedis(ctx)
}

func (s *Service) loadNewTaskFromRedis(ctx context.Context) {
	// Track task IDs in queue to avoid re-enqueueing
	taskIDsInQueue := make(map[string]struct{})
	var taskIDsMu sync.Mutex

	pushIfNotExists := func(task *task2.Task) bool {
		taskIDsMu.Lock()
		defer taskIDsMu.Unlock()
		if _, exists := taskIDsInQueue[task.ID]; exists {
			return false
		}
		taskIDsInQueue[task.ID] = struct{}{}
		s.taskService.Queue.SafePush(task)
		return true
	}

	// Periodically poll Redis for new tasks if leader
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.IsLeader() {
					pendingTasks, err := s.taskService.Store.LoadPendingTasks()
					if err == nil {
						added := 0
						for _, t := range pendingTasks {
							if pushIfNotExists(t) {
								added++
							}
						}
						if added > 0 {
							log.Printf("Polled and loaded %d new pending tasks from Redis", added)
						}
					}
				}
			}
		}
	}()
}
