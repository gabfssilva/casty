# Multi-Process Cluster Example

Runs multiple Casty nodes as separate processes, each on its own TCP port.
Node 1 acts as the seed/coordinator. Other nodes join through it.

The example spawns a sharded counter entity and demonstrates
sending increments and reading values across the cluster.

## Running locally

Open separate terminals from the project root:

```bash
# Terminal 1 — seed node (port 25520)
uv run python examples/11_multi_process_cluster/main.py 1

# Terminal 2 — worker (port 25521)
uv run python examples/11_multi_process_cluster/main.py 2

# Terminal 3 — worker (port 25522)
uv run python examples/11_multi_process_cluster/main.py 3
```

Each node starts an interactive prompt with the following commands:

```
inc <entity> <amount>   — increment a counter entity
get <entity>            — read the current value
quit                    — shut down the node
```

## Running with Docker Compose

From this directory:

```bash
docker compose up --build
```

This starts one seed and one worker in `--auto` mode (non-interactive).
The seed increments a counter 10 times, then both nodes read the result.

To scale to more workers:

```bash
docker compose up --build --scale node=4
```

## CLI reference

```
usage: main.py [-h] [--base-port BASE_PORT] [--port PORT] [--host HOST]
               [--bind-host BIND_HOST] [--seed-host SEED_HOST]
               [--seed-port SEED_PORT] [--auto]
               node_id

  node_id              Node number (1 = seed)
  --base-port          Base port (default: 25520)
  --port               Explicit port (overrides base-port + node_id)
  --host               Identity hostname; use 'auto' for container IP (default: 127.0.0.1)
  --bind-host          Address to bind TCP listener (default: same as --host)
  --seed-host          Hostname of the seed node (default: same as --host)
  --seed-port          Port of the seed node (default: same as --base-port)
  --auto               Non-interactive mode for Docker
```
