package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := LoadConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := NewPriorityQueue()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	store := NewTaskStore(cfg.RedisAddr, "", 0)
	taskService := NewTaskService(queue, store, redisClient)
	nodeService := NewNodeService(cfg, redisClient, taskService)

	defer nodeService.Close()

	nodeService.LoadPendingTasks()

	workerPool := NewWorkerPool(cfg.WorkerCount, queue, store)
	nodeService.StartWorkerPool(ctx, workerPool, cfg.NodeID)

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

func setupServer(cfg *Config) *http.Server {
	server := &http.Server{Addr: cfg.ListenAddr}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()
	return server
}
