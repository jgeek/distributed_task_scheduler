# Distributed Task Scheduler

A lightweight distributed task scheduler with pluggable leader election (Redis and Raft). Nodes expose a simple REST API to submit and query tasks. Only the elected leader runs workers and pulls tasks from Redis for execution; followers just accept submissions and persist them so the leader can process them.

- Leader election strategies: Redis (simple lock) or Raft (consensus)
- Task persistence: Redis
- Worker pool runs on leader only
- REST API with Swagger UI
- Prometheus metrics

## Project layout

- conf: configuration (env-driven)
- handlers: REST endpoints (+ Swagger annotations)
- leader: leader election strategies (redis/raft) behind a strategy interface
- node: node service and worker pool; encapsulates leadership, polling, and execution
- task: task service and Redis-backed store
- docs: generated Swagger files (swagger.json, swagger.yaml)
- main: application entrypoint

## How it works

- All nodes expose REST endpoints. Any node can accept task submissions via POST /submit.
- Each task is saved to Redis immediately.
- Only the current leader executes tasks. The leader:
  - Loads pending tasks from Redis at startup
  - Continuously polls Redis for newly added tasks (without re-adding tasks already queued)
  - Feeds a worker pool that processes tasks
- If leadership changes, the worker pool is stopped on the old leader and started on the new leader.

## REST API

- POST /submit
  - Body: { "priority": "high|medium|low", "payload": <json> }
  - Response: { "id": "<task-id>" }
- GET /status?id=<task-id>
  - Response: { "status": "pending|completed" }
- GET /leader
  - Response: { "leader": "<node-id>" }
- GET /health
  - Response: { node_id, is_leader, leader_id, queue_size, timestamp }
- GET /metrics
  - Prometheus metrics including scheduler_tasks_total and scheduler_is_leader
- Swagger UI: /swagger/
  - Spec: /swagger/doc.json (served from docs/swagger.json)

## Configuration (env vars)

- NODE_ID: unique ID for the node (default: node1)
- REDIS_ADDR: host:port for Redis (default: localhost:6379)
- LISTEN_ADDR: HTTP listen address (default: :8080)
- CONSENSUS_TYPE: redis or raft (default: raft)
- WORKER_COUNT: number of workers (default: 4)
- RAFT_BIND_ADDR: host:port Raft bind/advertise addr (default: 127.0.0.1:9000)
- RAFT_DATA_DIR: data directory for Raft (default: /tmp/raft-<NODE_ID>)
- RAFT_NODES: comma-separated list of initial peers, format ID:host:port (example: raft-node1:raft-node1:9900,raft-node2:raft-node2:9900)
- (Optional) RAFT_BOOTSTRAP: if used, only for the first node on a new cluster (compose sets it for raft-node1)

## Run with Docker Compose

Prerequisites: Docker and Docker Compose v2.

### Redis leader election (default profile: redis)

This runs 3 nodes (node1..node3) plus Redis. Leader is elected using Redis.

```
docker compose --profile redis up --build
```

Ports:
- node1: http 8081 -> 8080, metrics/health on same port
- node2: http 8082 -> 8080
- node3: http 8083 -> 8080
- redis: 6379

Swagger UI:
- http://localhost:8081/swagger/
- http://localhost:8082/swagger/
- http://localhost:8083/swagger/

Submit a task (to any node):
```
curl -X POST http://localhost:8081/submit \
  -H 'Content-Type: application/json' \
  -d '{"priority":"high","payload":{"job":"demo"}}'
```
Check status:
```
curl 'http://localhost:8081/status?id=<task-id>'
```

### Raft leader election (profile: raft)

This runs 2 nodes (raft-node1, raft-node2) plus Redis. The nodes form a Raft cluster.

```
docker compose --profile raft up --build
```

Ports:
- raft-node1: http 8091 -> 8080, raft port 9911 -> 9900
- raft-node2: http 8092 -> 8080, raft port 9912 -> 9900

Swagger UI:
- http://localhost:8091/swagger/
- http://localhost:8092/swagger/

Notes:
- On a brand new cluster, ensure RAFT_DATA_DIR volumes are empty when bootstrapping.
- RAFT_NODES must list peers as ID:host:port that are reachable inside the compose network.

## Local development

Requirements: Go 1.24+

Run a single node locally with Redis:
```
export NODE_ID=node-dev
export REDIS_ADDR=localhost:6379
export LISTEN_ADDR=:8080
export CONSENSUS_TYPE=redis

go run ./main
```

Run two nodes locally (different ports):
```
# Terminal 1
export NODE_ID=node-a
export REDIS_ADDR=localhost:6379
export LISTEN_ADDR=:8081
export CONSENSUS_TYPE=redis

go run ./main

# Terminal 2
export NODE_ID=node-b
export REDIS_ADDR=localhost:6379
export LISTEN_ADDR=:8082
export CONSENSUS_TYPE=redis

go run ./main
```

### Swagger generation

Install swag if needed: `go install github.com/swaggo/swag/cmd/swag@latest`

Generate/update spec (writes docs/swagger.json and docs/swagger.yaml):
```
swag init -g main/cmd.go -o docs
```
Swagger UI is available at /swagger/, which fetches /swagger/doc.json. In Docker images, ensure the docs directory is included (it is by default because the binary runs from the repo root and we serve docs/swagger.json).

## Metrics

- GET /metrics exposes Prometheus text metrics:
  - scheduler_tasks_total: tasks in in-memory queue
  - scheduler_is_leader{node="<NODE_ID>"}: 1 if leader, else 0

## Troubleshooting

- Swagger UI shows “Fetch error Internal Server Error doc.json”
  - Ensure docs/swagger.json exists. Run `swag init -g main/cmd.go -o docs`.
  - Verify /swagger/doc.json returns JSON in the browser.

- Raft: “bootstrap only works on new clusters”
  - Stop containers, remove RAFT_DATA_DIR volumes, then start again.

- Two Raft leaders at startup
  - Ensure RAFT_NODES lists the same peers on each node and addresses are resolvable in the compose network.

- Port conflicts locally
  - Set a unique LISTEN_ADDR and RAFT_BIND_ADDR per process.

## License

MIT (or your preferred license).

