package task

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

// ------------------ Persistence Layer ------------------

const redisTaskPrefix = "task:"

// Store defines the interface for task storage operations
type Store interface {
	SaveTask(task *Task) error
	DeleteTask(id string) error
	LoadPendingTasks() ([]*Task, error)
	Get(id string) (*Task, error)
	GetTaskStatus(id string) (Status, error)
	UpdateStatus(id string, status Status) error
}

// RedisStore implements the Store interface using Redis
type RedisStore struct {
	client *redis.Client
}

func NewTaskStore(c *redis.Client) Store {
	return &RedisStore{client: c}
}

func (s *RedisStore) SaveTask(task *Task) error {
	ctx := context.Background()
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, redisTaskPrefix+task.ID, data, 0).Err()
}

func (s *RedisStore) DeleteTask(id string) error {
	ctx := context.Background()
	return s.client.Del(ctx, redisTaskPrefix+id).Err()
}

func (s *RedisStore) LoadPendingTasks() ([]*Task, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, redisTaskPrefix+"*").Result()
	if err != nil {
		return nil, err
	}
	tasks := []*Task{}
	for _, key := range keys {
		val, err := s.client.Get(ctx, key).Result()
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

func (s *RedisStore) Get(id string) (*Task, error) {
	ctx := context.Background()
	val, err := s.client.Get(ctx, redisTaskPrefix+id).Result()
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return nil, err
	}
	return t, nil
}
func (s *RedisStore) GetTaskStatus(id string) (Status, error) {
	ctx := context.Background()
	val, err := s.client.Get(ctx, redisTaskPrefix+id).Result()
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return "", err
	}
	return t.Status, nil
}

// UpdateStatus updates the status of a task by its ID.
func (s *RedisStore) UpdateStatus(id string, status Status) error {
	ctx := context.Background()
	key := redisTaskPrefix + id
	// Get the task
	val, err := s.client.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return err
	}
	t.Status = status
	return s.SaveTask(t)
}
