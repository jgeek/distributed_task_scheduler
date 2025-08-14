package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type TaskService struct {
	queue       *PriorityQueue
	store       *TaskStore
	redisClient *redis.Client
}

func NewTaskService(queue *PriorityQueue, store *TaskStore, redisClient *redis.Client) *TaskService {
	return &TaskService{queue: queue, store: store, redisClient: redisClient}
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
		CreatedAt: time.Now(),
	}
	if err := s.store.SaveTask(task); err != nil {
		return "", err
	}
	s.queue.SafePush(task)
	return task.ID, nil
}

func (s *TaskService) GetTaskStatus(id string) (string, error) {
	return s.store.GetTaskStatus(id)
}

func (s *TaskService) QueueLen() int {
	return s.queue.Len()
}
