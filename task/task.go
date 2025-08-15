package task

import (
	"container/heap"
	"encoding/json"
	"sync"
	"time"
)

const (
	PriorityHigh = iota
	PriorityMedium
	PriorityLow
)

type TaskPriority int
type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in_progress"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Task struct {
	ID        string          `json:"id"`
	Priority  TaskPriority    `json:"priority"`
	Payload   json.RawMessage `json:"payload"`
	Status    Status          `json:"status"`
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
