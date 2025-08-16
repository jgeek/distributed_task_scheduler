package node

import (
	"distributed_task_scheduler/task"
	"log"
	"sync"
	"time"
)

type WorkerPool struct {
	NumWorkers int
	Queue      *task.PriorityQueue
	Store      task.Store
	stopCh     chan struct{}
	isRunning  bool
	mu         sync.Mutex
}

func NewWorkerPool(num int, queue *task.PriorityQueue, store task.Store) *WorkerPool {
	return &WorkerPool{
		NumWorkers: num,
		Queue:      queue,
		Store:      store,
		stopCh:     make(chan struct{}),
		isRunning:  false,
	}
}

func (wp *WorkerPool) Start(process func(*task.Task) error) {
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
					t := wp.Queue.SafePop()
					if t == nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					err := wp.Store.UpdateStatus(t.ID, task.StatusInProgress)
					if err != nil {
						log.Printf("Worker %d failed to update status for task %s: %v", workerID, t.ID, err)
						return
					}
					log.Printf("Worker %d processing task %s", workerID, t.ID)
					if err := process(t); err == nil {
						// Task completed successfully, update status to completed instead of deleting
						if updateErr := wp.Store.UpdateStatus(t.ID, task.StatusCompleted); updateErr != nil {
							log.Printf("Failed to update status for completed task %s: %v", t.ID, updateErr)
						}
					} else {
						log.Printf("Worker %d: Task %s failed: %v", workerID, t.ID, err)
						if updateErr := wp.Store.UpdateStatus(t.ID, task.StatusFailed); updateErr != nil {
							log.Printf("Failed to update status for failed task %s: %v", t.ID, updateErr)
						}
						// Re-queue failed task (simple retry mechanism)
						//wp.Queue.SafePush(t)
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
