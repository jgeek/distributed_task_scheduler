package main

import (
	"context"
	"distributed_task_scheduler/conf"
	"distributed_task_scheduler/handlers"
	"distributed_task_scheduler/leader"
	node2 "distributed_task_scheduler/node"
	task2 "distributed_task_scheduler/task"
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

	queue := task2.NewPriorityQueue()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	store := task2.NewTaskStore(redisClient)
	taskService := task2.NewTaskService(queue, store)
	workerPool := node2.NewWorkerPool(cfg.WorkerCount, queue, store)
	leaderService := leader.NewLeaderElectionService(cfg, redisClient)
	nodeService := node2.NewService(taskService, workerPool, leaderService)

	defer nodeService.Close()

	nodeService.LoadPendingTasks()

	nodeService.StartWorkerPool(ctx, cfg.NodeID)

	handlers.RegisterRESTEndpoints(cfg, nodeService)

	log.Printf("Node %s starting on %s (leader election: %s)", cfg.NodeID, cfg.ListenAddr, cfg.LeaderType)

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
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()
	return server
}
