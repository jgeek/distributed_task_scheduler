package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
	"time"
)

// Priority levels
const (
	PriorityHigh = iota
	PriorityMedium
	PriorityLow
)

// TaskPriority type
// Use int for easy comparison
// You can use custom type if preferred

type TaskPriority int

// Task represents a unit of work
// ID: unique identifier
// Priority: High/Medium/Low
// Payload: arbitrary JSON
// CreatedAt: timestamp

type Task struct {
	ID        string          `json:"id"`
	Priority  TaskPriority    `json:"priority"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// TaskItem wraps Task for heap operations
// Lower index = higher priority

type TaskItem struct {
	Task  *Task
	Index int // heap index
}

// PriorityQueue implements heap.Interface and holds taskItems

type PriorityQueue struct {
	items []*TaskItem
	lock  sync.Mutex
}

func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	// Lower value = higher priority
	return pq.items[i].Task.Priority < pq.items[j].Task.Priority
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*TaskItem)
	item.Index = len(pq.items)
	pq.items = append(pq.items, item)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) SafePush(task *Task) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	heap.Push(pq, &TaskItem{Task: task})
}

func (pq *PriorityQueue) SafePop() *Task {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	if pq.Len() == 0 {
		return nil
	}
	item := heap.Pop(pq).(*TaskItem)
	return item.Task
}

// NewPriorityQueue returns an initialized queue
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	heap.Init(pq)
	return pq
}

// ------------------ Persistence Layer (Redis) ------------------

const redisTaskPrefix = "task:"

// TaskStore uses Redis for persistence

type TaskStore struct {
	client *redis.Client
}

func NewTaskStore(addr, password string, db int) *TaskStore {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &TaskStore{client: client}
}

func (ts *TaskStore) SaveTask(task *Task) error {
	ctx := context.Background()
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return ts.client.Set(ctx, redisTaskPrefix+task.ID, data, 0).Err()
}

func (ts *TaskStore) DeleteTask(id string) error {
	ctx := context.Background()
	return ts.client.Del(ctx, redisTaskPrefix+id).Err()
}

func (ts *TaskStore) LoadPendingTasks() ([]*Task, error) {
	ctx := context.Background()
	keys, err := ts.client.Keys(ctx, redisTaskPrefix+"*").Result()
	if err != nil {
		return nil, err
	}
	tasks := []*Task{}
	for _, key := range keys {
		val, err := ts.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		t := &Task{}
		if err := json.Unmarshal([]byte(val), t); err != nil {
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (ts *TaskStore) GetTaskStatus(id string) (string, error) {
	ctx := context.Background()
	_, err := ts.client.Get(ctx, redisTaskPrefix+id).Result()
	if err != nil {
		return "completed", nil
	}
	return "pending", nil
}

// ------------------ Worker Pool Logic ------------------

type WorkerPool struct {
	NumWorkers int
	Queue      *PriorityQueue
	Store      *TaskStore
	stopCh     chan struct{}
	isRunning  bool
	mu         sync.Mutex
}

func NewWorkerPool(num int, queue *PriorityQueue, store *TaskStore) *WorkerPool {
	return &WorkerPool{
		NumWorkers: num,
		Queue:      queue,
		Store:      store,
		stopCh:     make(chan struct{}),
		isRunning:  false,
	}
}

func (wp *WorkerPool) Start(process func(*Task) error) {
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
