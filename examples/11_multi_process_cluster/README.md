# Distributed Task Queue

All nodes are symmetric workers. Each submits tasks that get sharded across
the cluster, processed on whichever node owns the shard, and results queried
back from any node.

Tasks are sharded entities with a state machine: `pending` -> `completed`.
Each task receives a payload, computes an iterative SHA-256 hash (simulating
CPU work), and stores the result. Any worker can query any task's result,
even if it lives on a different node.

## Configuration

Cluster settings live in `casty.toml`. Per-node host/port/seed are passed
via CLI args, overriding the TOML defaults.

## Running locally

Open separate terminals from the project root:

```bash
# Terminal 1 — first node (starts the cluster)
uv run python examples/11_multi_process_cluster/main.py --port 25520

# Terminal 2 — joins via seed
uv run python examples/11_multi_process_cluster/main.py --port 25521 --seed 127.0.0.1:25520

# Terminal 3 — joins via seed
uv run python examples/11_multi_process_cluster/main.py --port 25522 --seed 127.0.0.1:25520
```

## Running with Docker Compose

From this directory:

```bash
docker compose up --build
```

This starts 3 identical nodes. All use `node-1:25520` as seed — each service
name is DNS-resolvable within the Docker network. `node-1` receives its own
join request and discards it; the other nodes join through it. Gossip then
propagates the full cluster state.

To add more nodes, add entries to `docker-compose.yml`:

```yaml
  node-4:
    <<: *node
    hostname: node-4
  node-5:
    <<: *node
    hostname: node-5
```

## CLI reference

```
usage: main.py [-h] [--port PORT] [--host HOST] [--bind-host BIND_HOST]
               [--seed SEED] [--tasks TASKS]

  --port         TCP port (default: 25520)
  --host         Identity hostname; 'auto' for container IP (default: 127.0.0.1)
  --bind-host    Address to bind TCP listener (default: same as --host)
  --seed         Seed node address (host:port) to join existing cluster
  --tasks        Tasks per worker (default: 5)

The final report automatically detects how many nodes are in the cluster
by querying the cluster membership state — no need to specify the node count.
```
