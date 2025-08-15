package conf

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	RedisAddr     string
	ListenAddr    string
	NodeID        string
	ConsensusType string
	WorkerCount   int
	RaftBindAddr  string
	RaftDataDir   string
	RaftBootstrap bool
}

func LoadConfig() *Config {
	nodeID := getEnv("NODE_ID", "node1")
	return &Config{
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		ListenAddr:    getEnv("LISTEN_ADDR", ":8080"),
		NodeID:        nodeID,
		ConsensusType: getEnv("CONSENSUS_TYPE", "raft"), // "redis" or "raft"
		RaftBootstrap: getBoolEnv("RAFT_BOOTSTRAP", false),
		WorkerCount:   getEnvInt("WORKER_COUNT", 4),
		RaftBindAddr:  getEnv("RAFT_BIND_ADDR", "127.0.0.1:9000"), // Use full address instead of just port
		RaftDataDir:   getEnv("RAFT_DATA_DIR", fmt.Sprintf("/tmp/raft-%s", nodeID)),
	}
}

func getBoolEnv(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if v == "true" || v == "1" {
			return true
		}
		return false
	}
	return def
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
