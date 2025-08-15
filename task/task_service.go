package task

import (
	"fmt"
	"time"
)

type TaskService struct {
	Queue *PriorityQueue
	Store *TaskStore
}

func NewTaskService(queue *PriorityQueue, store *TaskStore) *TaskService {
	return &TaskService{Queue: queue, Store: store}
}

func (s *TaskService) SubmitTask(priorityStr string, payload []byte) (string, error) {
	var priority TaskPriority
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
