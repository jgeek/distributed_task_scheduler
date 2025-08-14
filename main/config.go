package main

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	RedisAddr    string
	ListenAddr   string
	NodeID       string
	LeaderType   string
	WorkerCount  int
	RaftBindAddr string
	RaftDataDir  string
}

func LoadConfig() *Config {
	return &Config{
		RedisAddr:    getenv("REDIS_ADDR", "localhost:6379"),
		ListenAddr:   getenv("LISTEN_ADDR", ":8080"),
		NodeID:       getenv("NODE_ID", "node1"),
		LeaderType:   getenv("LEADER_TYPE", "redis"),
		WorkerCount:  getenvInt("WORKER_COUNT", 4),
		RaftBindAddr: getenv("RAFT_BIND_ADDR", ":9000"),
		RaftDataDir:  getenv("RAFT_DATA_DIR", fmt.Sprintf("/tmp/raft-%s", getenv("NODE_ID", "node1"))),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		i, _ := strconv.Atoi(v)
		return i
	}
	return def
}
