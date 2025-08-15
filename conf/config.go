package conf

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	RedisAddr     string
	ListenAddr    string
	NodeID        string
	ConsensusType string
	WorkerCount   int
	RaftBindAddr  string
	RaftDataDir   string
	Peers         []Peer // For Raft, list of initial nodes
}
type Peer struct {
	ID   string
	Addr string
}

func LoadConfig() *Config {
	nodeID := getEnv("NODE_ID", "node1")
	return &Config{
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		ListenAddr:    getEnv("LISTEN_ADDR", ":8080"),
		NodeID:        nodeID,
		ConsensusType: getEnv("CONSENSUS_TYPE", "raft"), // "redis" or "raft"
		WorkerCount:   getEnvInt("WORKER_COUNT", 4),
		RaftBindAddr:  getEnv("RAFT_BIND_ADDR", "127.0.0.1:9000"), // Use full address instead of just port
		RaftDataDir:   getEnv("RAFT_DATA_DIR", fmt.Sprintf("/tmp/raft-%s", nodeID)),
		Peers:         getPeers(getStringSliceEnv("RAFT_NODES", "")),
	}
}

func getPeers(peers []string) []Peer {
	if len(peers) == 0 {
		return nil
	}
	peerList := make([]Peer, 0, len(peers))
	for _, p := range peers {
		parts := strings.SplitN(p, ":", 2)
		if len(parts) != 2 {
			fmt.Printf("Invalid peer format: %s, expected format is ID:Addr\n", p)
			continue
		}
		peerList = append(peerList, Peer{ID: strings.TrimSpace(parts[0]), Addr: strings.TrimSpace(parts[1])})
	}
	return peerList
}

func getStringSliceEnv(key string, def string) []string {
	if v := os.Getenv(key); v != "" {
		return splitString(v)
	}
	if def != "" {
		return splitString(def)
	}
	return nil
}

func splitString(v string) []string {
	if v == "" {
		return nil
	}
	parts := make([]string, 0)
	for _, part := range strings.Split(v, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
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
