package main

import (
	"context"
	"distributed_task_scheduler/main/conf"
	"distributed_task_scheduler/main/leader"
	"distributed_task_scheduler/main/node"
	"distributed_task_scheduler/main/task"
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
	leaderService := leader.NewLeaderElectionService(cfg, redisClient)
	nodeService := node.NewNodeService(taskService, workerPool, leaderService)

	defer nodeService.Close()

	nodeService.LoadPendingTasks()

	nodeService.StartWorkerPool(ctx, cfg.NodeID)

	RegisterRESTEndpoints(cfg, nodeService)

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
