package main

import (
	"context"
	"distributed_task_scheduler/conf"
	"distributed_task_scheduler/handlers"
	"distributed_task_scheduler/leader"
	"distributed_task_scheduler/node"
	"distributed_task_scheduler/task"
	"errors"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := conf.LoadConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := task.NewPriorityQueue()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	store := task.NewTaskStore(redisClient)
	taskService := task.NewTaskService(queue, store)
	workerPool := node.NewWorkerPool(cfg.WorkerCount, queue, store)
	strategy := leader.NewLeaderElectionService(cfg, redisClient)
	nodeService := node.NewService(taskService, workerPool, strategy)

	defer nodeService.Close()

	err := nodeService.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start node : %v", err)
	}

	handlers.RegisterRESTEndpoints(cfg, nodeService)
	server := setupServer(cfg)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	workerPool.Stop()
	cancel()
	_ = server.Shutdown(context.Background())
}

func setupServer(cfg *conf.Config) *http.Server {
	server := &http.Server{Addr: cfg.ListenAddr}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()
	return server
}
