package task

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

// ------------------ Persistence Layer (Redis) ------------------

const redisTaskPrefix = "task:"

type Store struct {
	client *redis.Client
}

func NewTaskStore(c *redis.Client) *Store {
	return &Store{client: c}
}

func (s *Store) SaveTask(task *Task) error {
	ctx := context.Background()
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, redisTaskPrefix+task.ID, data, 0).Err()
}

func (s *Store) DeleteTask(id string) error {
	ctx := context.Background()
	return s.client.Del(ctx, redisTaskPrefix+id).Err()
}

func (s *Store) LoadPendingTasks() ([]*Task, error) {
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

func (s *Store) Get(id string) (*Task, error) {
	ctx := context.Background()
	val, err := s.client.Get(ctx, redisTaskPrefix+id).Result()
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return nil, err
	}
	return t, nil
}
func (s *Store) GetTaskStatus(id string) (Status, error) {
	ctx := context.Background()
	val, err := s.client.Get(ctx, redisTaskPrefix+id).Result()
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return "", err
	}
	return t.Status, nil
}

// UpdateStatus updates the status of a task by its ID.
func (s *Store) UpdateStatus(id string, status Status) error {
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
