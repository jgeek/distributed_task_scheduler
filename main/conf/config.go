package conf

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
	nodeID := getEnv("NODE_ID", "node1")
	return &Config{
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		ListenAddr:   getEnv("LISTEN_ADDR", ":8080"),
		NodeID:       nodeID,
		LeaderType:   getEnv("LEADER_TYPE", "raft"), // "redis" or "raft"
		WorkerCount:  getEnvInt("WORKER_COUNT", 4),
		RaftBindAddr: getEnv("RAFT_BIND_ADDR", "127.0.0.1:9000"), // Use full address instead of just port
		RaftDataDir:  getEnv("RAFT_DATA_DIR", fmt.Sprintf("/tmp/raft-%s", nodeID)),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		i, _ := strconv.Atoi(v)
		return i
	}
	return def
}
