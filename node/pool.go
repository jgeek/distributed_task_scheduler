package node

import (
	task2 "distributed_task_scheduler/task"
	"log"
	"sync"
	"time"
)

type WorkerPool struct {
	NumWorkers int
	Queue      *task2.PriorityQueue
	Store      *task2.TaskStore
	stopCh     chan struct{}
	isRunning  bool
	mu         sync.Mutex
}

func NewWorkerPool(num int, queue *task2.PriorityQueue, store *task2.TaskStore) *WorkerPool {
	return &WorkerPool{
		NumWorkers: num,
		Queue:      queue,
		Store:      store,
		stopCh:     make(chan struct{}),
		isRunning:  false,
	}
}

func (wp *WorkerPool) Start(process func(*task2.Task) error) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.isRunning {
		return // Already running
	}

	wp.isRunning = true
	log.Printf("Starting %d workers", wp.NumWorkers)

	for i := 0; i < wp.NumWorkers; i++ {
		go func(workerID int) {
			log.Printf("Worker %d started", workerID)
			for {
				select {
				case <-wp.stopCh:
					log.Printf("Worker %d stopping", workerID)
					return
				default:
					task := wp.Queue.SafePop()
					if task == nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					log.Printf("Worker %d processing task %s", workerID, task.ID)
					if err := process(task); err == nil {
						// Task completed successfully, remove from Redis
						if deleteErr := wp.Store.DeleteTask(task.ID); deleteErr != nil {
							log.Printf("Failed to delete completed task %s: %v", task.ID, deleteErr)
						}
					} else {
						log.Printf("Worker %d: Task %s failed: %v", workerID, task.ID, err)
						// Re-queue failed task (simple retry mechanism)
						wp.Queue.SafePush(task)
					}
				}
			}
		}(i)
	}
}

func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if !wp.isRunning {
		return
	}

	log.Println("Stopping worker pool...")
	close(wp.stopCh)
	wp.isRunning = false

	// Create new stop channel for next start
	wp.stopCh = make(chan struct{})
}
