package task

import (
	"fmt"
	"time"
)

// Service defines the interface for task service operations
type Service interface {
	SubmitTask(priorityStr string, payload []byte) (string, error)
	GetTaskStatus(id string) (Status, error)
	QueueLen() int
	Add(task *Task)
	LoadPendingTasks() ([]*Task, error)
}

// TaskService implements the Service interface
type TaskService struct {
	Queue *PriorityQueue
	Store *Store
}

func (s *TaskService) LoadPendingTasks() ([]*Task, error) {
	return s.Store.LoadPendingTasks()
}

func (s *TaskService) Add(task *Task) {
	s.Queue.SafePush(task)
}

// NewTaskService creates a new task service that implements Service interface
func NewTaskService(queue *PriorityQueue, store *Store) Service {
	return &TaskService{Queue: queue, Store: store}
}

func (s *TaskService) SubmitTask(priorityStr string, payload []byte) (string, error) {
	var priority Priority
	switch priorityStr {
	case "high":
		priority = PriorityHigh
	case "medium":
		priority = PriorityMedium
	case "low":
		priority = PriorityLow
	default:
		priority = PriorityMedium
	}

	task := &Task{
		ID:        fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Priority:  priority,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}
	if err := s.Store.SaveTask(task); err != nil {
		return "", err
	}
	s.Queue.SafePush(task)
	return task.ID, nil
}

func (s *TaskService) GetTaskStatus(id string) (Status, error) {
	return s.Store.GetTaskStatus(id)
}

func (s *TaskService) QueueLen() int {
	return s.Queue.Len()
}
