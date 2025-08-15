package task

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

// ------------------ Persistence Layer (Redis) ------------------

const redisTaskPrefix = "task:"

type TaskStore struct {
	client *redis.Client
}

func NewTaskStore(c *redis.Client) *TaskStore {
	return &TaskStore{client: c}
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

func (ts *TaskStore) Get(id string) (*Task, error) {
	ctx := context.Background()
	val, err := ts.client.Get(ctx, redisTaskPrefix+id).Result()
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return nil, err
	}
	return t, nil
}
func (ts *TaskStore) GetTaskStatus(id string) (Status, error) {
	ctx := context.Background()
	val, err := ts.client.Get(ctx, redisTaskPrefix+id).Result()
	t := &Task{}
	if err = json.Unmarshal([]byte(val), t); err != nil {
		return "", err
	}
	return t.Status, nil
}

// UpdateStatus updates the status of a task by its ID.
func (s *TaskStore) UpdateStatus(id string, status Status) error {
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
