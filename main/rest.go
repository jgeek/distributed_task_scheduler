package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func RegisterRESTEndpoints(cfg *Config, nodeService *NodeService) {
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Priority string          `json:"priority"`
			Payload  json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		id, err := nodeService.SubmitTask(req.Priority, req.Payload)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status, err := nodeService.GetTaskStatus(id)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": status})
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "# HELP scheduler_tasks_total Total tasks in queue")
		fmt.Fprintln(w, "# TYPE scheduler_tasks_total gauge")
		fmt.Fprintf(w, "scheduler_tasks_total %d\n", nodeService.QueueLen())
		fmt.Fprintln(w, "# HELP scheduler_is_leader Node is leader")
		fmt.Fprintln(w, "# TYPE scheduler_is_leader gauge")
		if nodeService.IsLeader() {
			fmt.Fprintln(w, "scheduler_is_leader 1")
		} else {
			fmt.Fprintln(w, "scheduler_is_leader 0")
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		leader := nodeService.LeaderID()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id":    cfg.NodeID,
			"is_leader":  nodeService.IsLeader(),
			"leader_id":  leader,
			"queue_size": nodeService.QueueLen(),
			"timestamp":  time.Now(),
		})
	})

	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		leader := nodeService.LeaderID()
		if leader == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "no leader elected"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"leader": leader})
	})
}
