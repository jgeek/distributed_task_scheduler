package handlers

import (
	"distributed_task_scheduler/conf"
	"distributed_task_scheduler/node"
	"encoding/json"
	"fmt"
	httpSwagger "github.com/swaggo/http-swagger"
	"net/http"
	"time"
)

// DTOs for Swagger documentation
// SubmitRequest represents the request payload for submitting a task
// swagger:model SubmitRequest
type SubmitRequest struct {
	Priority string          `json:"priority"`
	Payload  json.RawMessage `json:"payload"`
}

// SubmitResponse represents the response with created task ID
// swagger:model SubmitResponse
type SubmitResponse struct {
	ID string `json:"id"`
}

// StatusResponse represents the task status response
// swagger:model StatusResponse
type StatusResponse struct {
	Status string `json:"status"`
}

// HealthResponse represents the health endpoint response
// swagger:model HealthResponse
type HealthResponse struct {
	NodeID    string    `json:"node_id"`
	IsLeader  bool      `json:"is_leader"`
	LeaderID  string    `json:"leader_id"`
	QueueSize int       `json:"queue_size"`
	Timestamp time.Time `json:"timestamp"`
}

// LeaderResponse represents the leader endpoint response
// swagger:model LeaderResponse
type LeaderResponse struct {
	Leader string `json:"leader"`
}

// ErrorResponse generic error payload
// swagger:model ErrorResponse
type ErrorResponse struct {
	Error string `json:"error"`
}

// SubmitHandler handles POST /submit
// @Summary Submit a task
// @Description Submits a new task to the scheduler
// @Tags tasks
// @Accept json
// @Produce json
// @Param request body handlers.SubmitRequest true "Task payload"
// @Success 200 {object} handlers.SubmitResponse
// @Failure 400 {object} handlers.ErrorResponse
// @Failure 500 {object} handlers.ErrorResponse
// @Router /submit [post]
func SubmitHandler(nodeService *node.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req SubmitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid request body"})
			return
		}
		id, err := nodeService.SubmitTask(req.Priority, req.Payload)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to submit task"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(SubmitResponse{ID: id})
	}
}

// StatusHandler handles GET /status
// @Summary Get task status
// @Description Returns the status of a task by ID
// @Tags tasks
// @Produce json
// @Param id query string true "Task ID"
// @Success 200 {object} handlers.StatusResponse
// @Failure 400 {object} handlers.ErrorResponse
// @Failure 500 {object} handlers.ErrorResponse
// @Router /status [get]
func StatusHandler(nodeService *node.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "missing id"})
			return
		}
		status, err := nodeService.GetTaskStatus(id)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to get status"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(StatusResponse{Status: string(status)})
	}
}

// MetricsHandler handles GET /metrics
// @Summary Prometheus metrics
// @Description Exposes Prometheus-compatible metrics
// @Tags metrics
// @Produce plain
// @Success 200 {string} string "Prometheus metrics"
// @Router /metrics [get]
func MetricsHandler(cfg *conf.Config, nodeService *node.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "# HELP scheduler_tasks_total Total tasks in queue")
		fmt.Fprintln(w, "# TYPE scheduler_tasks_total gauge")
		fmt.Fprintf(w, "scheduler_tasks_total %d\n", nodeService.QueueLen())
		fmt.Fprintln(w, "# HELP scheduler_is_leader Node is leader")
		fmt.Fprintln(w, "# TYPE scheduler_is_leader gauge")
		fmt.Fprintf(w, "scheduler_is_leader{node=\"%s\"} %d\n", cfg.NodeID, boolToInt(nodeService.IsLeader()))
	}
}

// HealthHandler handles GET /health
// @Summary Health check
// @Description Returns node health and leadership information
// @Tags health
// @Produce json
// @Success 200 {object} handlers.HealthResponse
// @Router /health [get]
func HealthHandler(cfg *conf.Config, nodeService *node.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		leader := nodeService.LeaderID()
		_ = json.NewEncoder(w).Encode(HealthResponse{
			NodeID:    cfg.NodeID,
			IsLeader:  nodeService.IsLeader(),
			LeaderID:  leader,
			QueueSize: nodeService.QueueLen(),
			Timestamp: time.Now(),
		})
	}
}

// LeaderHandler handles GET /leader
// @Summary Get current leader
// @Description Returns the current leader node ID
// @Tags cluster
// @Produce json
// @Success 200 {object} handlers.LeaderResponse
// @Failure 503 {object} handlers.ErrorResponse
// @Router /leader [get]
func LeaderHandler(nodeService *node.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		leader := nodeService.LeaderID()
		if leader == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "no leader elected"})
			return
		}
		_ = json.NewEncoder(w).Encode(LeaderResponse{Leader: leader})
	}
}

func RegisterRESTEndpoints(cfg *conf.Config, nodeService *node.Service) {
	http.HandleFunc("/submit", SubmitHandler(nodeService))
	http.HandleFunc("/status", StatusHandler(nodeService))
	http.HandleFunc("/metrics", MetricsHandler(cfg, nodeService))
	http.HandleFunc("/health", HealthHandler(cfg, nodeService))
	http.HandleFunc("/leader", LeaderHandler(nodeService))

	// Swagger: serve the spec at /swagger/doc.json and the UI at /swagger/
	http.HandleFunc("/swagger/doc.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.ServeFile(w, r, "docs/swagger.json")
	})
	http.Handle("/swagger/", httpSwagger.WrapHandler)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
