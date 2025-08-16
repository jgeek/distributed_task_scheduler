package node

import (
	"context"
	"distributed_task_scheduler/leader"
	"distributed_task_scheduler/task"
	"log"
	"sync"
	"time"
)

type Service struct {
	taskService      task.Service
	electionStrategy leader.Strategy
	pool             *WorkerPool
}

func NewService(taskService task.Service, pool *WorkerPool, leaderService leader.Strategy) *Service {
	return &Service{
		taskService:      taskService,
		electionStrategy: leaderService,
		pool:             pool,
	}
}

func (s *Service) Close() {
	s.electionStrategy.Stop()
}

func (s *Service) SubmitTask(priority string, payload []byte) (string, error) {
	return s.taskService.SubmitTask(priority, payload)
}

func (s *Service) GetTaskStatus(id string) (task.Status, error) {
	return s.taskService.GetTaskStatus(id)
}

func (s *Service) QueueLen() int {
	return s.taskService.QueueLen()
}

func (s *Service) IsLeader() bool {
	return s.electionStrategy.IsLeader()
}

func (s *Service) LeaderID() string {
	return s.electionStrategy.GetLeader()
}

func (s *Service) LoadPendingTasks() {
	if !s.IsLeader() {
		log.Println("Not leader, skipping loading pending tasks from Redis")
		return
	}
	pendingTasks, err := s.taskService.LoadPendingTasks()
	if err == nil {
		for _, t := range pendingTasks {
			s.taskService.SafePush(t)
		}
		log.Printf("Loaded %d pending tasks from Redis", len(pendingTasks))
	}
}

func (s *Service) Start(ctx context.Context) error {
	nodeID := s.electionStrategy.NodeId()
	log.Printf("Starting node service with ID: %s", nodeID)

	err := s.electionStrategy.Start()
	if err != nil {
		return err
	}
	s.LoadPendingTasks()
	s.StartWorkerPool(ctx)
	s.LoadNewlyAddedTaskFromRedis(ctx)
	return nil
}

func (s *Service) StartWorkerPool(ctx context.Context) {

	var workerStarted bool
	var workerMu sync.Mutex

	taskProcessor := func(task *task.Task) error {
		log.Printf("Node: %s, processing task %s with priority %d", s.electionStrategy.NodeId(), task.ID, task.Priority)
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
					log.Printf("Node %s became leader, starting worker pool", s.electionStrategy.NodeId())
					s.pool.Start(taskProcessor)
					workerStarted = true
				} else if !s.IsLeader() && workerStarted {
					log.Printf("Node %s lost leadership, stopping worker pool", s.electionStrategy.NodeId())
					s.pool.Stop()
					workerStarted = false
				}
				workerMu.Unlock()
			}
		}
	}()
}

func (s *Service) LoadNewlyAddedTaskFromRedis(ctx context.Context) {
	// Track task IDs in queue to avoid re-enqueueing
	taskIDsInQueue := make(map[string]struct{})
	var taskIDsMu sync.Mutex

	pushIfNotExists := func(task *task.Task) bool {
		taskIDsMu.Lock()
		defer taskIDsMu.Unlock()
		if _, exists := taskIDsInQueue[task.ID]; exists {
			return false
		}
		taskIDsInQueue[task.ID] = struct{}{}
		s.taskService.SafePush(task)
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
					pendingTasks, err := s.taskService.LoadPendingTasks()
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
