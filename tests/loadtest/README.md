# Casty Load + Resilience Test

Continuous HTTP load with programmatic fault injection against a real Casty cluster on AWS. Crashes nodes, kills the leader, partitions the network, and does rolling restarts — all while measuring throughput, latency, and recovery time.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    VPC 10.0.0.0/16                      │
│                                                         │
│  ┌─────────────┐    ┌──────────┐    ┌──────────┐       │
│  │ Load        │───▶│ Node 0   │    │ Node 1   │ ...   │
│  │ Generator   │    │ 10.0.1.10│◄──▶│ 10.0.1.11│       │
│  │ 10.0.1.100  │    │ :8000    │    │ :8000    │       │
│  │             │    │ :25520   │    │ :25520   │       │
│  │ httpx ──────┼───▶├──────────┤    ├──────────┤       │
│  │ asyncssh ───┼───▶│ SSH :22  │    │ SSH :22  │       │
│  └─────────────┘    └──────────┘    └──────────┘       │
│                           ▲                             │
│                     ┌─────┴─────┐                       │
│                     │    ALB    │◄── external access     │
│                     │   :80    │                        │
│                     └───────────┘                       │
└─────────────────────────────────────────────────────────┘
```

- **10 cluster nodes** (`10.0.1.10`–`10.0.1.19`): each runs a Casty + FastAPI container with Counter and KV sharded entities (20 shards each)
- **1 load generator** (`10.0.1.100`): runs httpx workers hammering the cluster + asyncssh for fault injection
- **ALB**: optional, for external access and quick smoke tests

## Prerequisites

- AWS CLI configured (`aws configure`)
- [Pulumi](https://www.pulumi.com/docs/install/) installed
- Docker running locally (for image build)
- An EC2 key pair for SSH access (fault injection)
- [uv](https://docs.astral.sh/uv/) installed

## Quick Start

### Make it happen (one command)

```bash
./tests/loadtest/scripts/run.sh
```

That's it. This single command:

1. Deploys the full AWS infrastructure (VPC, 10 EC2 nodes, ALB, load generator)
2. Waits for the cluster to be healthy
3. Copies the test suite to the load generator
4. Runs load + all 4 fault scenarios
5. Fetches the JSON report back to your machine
6. Tears down all infrastructure

Options:

```bash
./tests/loadtest/scripts/run.sh --no-faults              # load only, no fault injection
./tests/loadtest/scripts/run.sh --duration 120            # custom duration
./tests/loadtest/scripts/run.sh --num-workers 100         # more pressure
./tests/loadtest/scripts/run.sh --keep                    # don't destroy infra after
```

Set `SSH_KEY` if your key isn't at `~/.ssh/casty.pem`:

```bash
SSH_KEY=~/.ssh/my-key.pem ./tests/loadtest/scripts/run.sh
```

### Manual steps (if you prefer)

```bash
# Deploy infra
./tests/loadtest/scripts/deploy.sh

# SSH into load generator and run manually
ssh -i ~/.ssh/casty.pem ec2-user@<LOADGEN_PUBLIC_IP>
python3.13 -m loadtest --target-url http://<ALB_URL> --ssh-key ~/.ssh/casty.pem

# Tear down
./tests/loadtest/scripts/teardown.sh
```

### Local iteration (against existing ALB)

```bash
uv run python -m tests.loadtest \
  --target-url http://<ALB_URL> \
  --num-workers 10 \
  --duration 60 \
  --no-faults
```

## CLI Reference

```
uv run python -m tests.loadtest [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--target-url` | **(required)** | Cluster URL (e.g. ALB URL) |
| `--ssh-key` | — | Path to SSH private key for fault injection |
| `--ssh-user` | `ec2-user` | SSH username |
| `--num-workers` | `50` | Concurrent HTTP workers |
| `--warmup` | `30` | Warmup duration (seconds) |
| `--duration` | `480` | Total test duration (seconds) |
| `--no-faults` | `false` | Load only, skip fault injection |

Fault injection requires `--ssh-key` (use `--no-faults` to skip).

Node IPs are automatically discovered by calling `GET /cluster/status` on the target URL at startup — no need to know individual node IPs.

## Fault Scenarios

The test runs 4 scenarios sequentially under continuous load:

```
 0s─────30s──────────120s──────────210s──────────280s──────────430s───460s
 │ warmup │ crash+recover │ leader kill │ partition │ rolling restart │ cool │
```

### Scenario 1 — Node Crash + Recovery (~90s)

1. Pick a non-leader node, hard-kill its container (`docker kill`)
2. Measure failure detection time via `/cluster/status`
3. Wait 10s under degraded load
4. Restart the container, measure cluster convergence time

### Scenario 2 — Leader Kill (~90s)

1. Identify the current leader (lowest address among "up" members)
2. Hard-kill the leader's container
3. Measure new leader election time
4. Restart old leader, measure rejoin + reconvergence

### Scenario 3 — Network Partition (~70s)

1. Block Casty TCP port 25520 via `iptables` (HTTP stays alive for monitoring)
2. Wait 20s for phi accrual failure detector to trigger
3. Heal the partition, measure reconvergence time

### Scenario 4 — Rolling Restart (~150s for 10 nodes)

1. For each node sequentially: stop container → wait 5s → start → wait for healthy → next
2. Verify continuous availability throughout

## HTTP Load Profile

50 concurrent workers (configurable) in a tight loop with weighted operations:

| Operation | Weight | Entity Pool |
|-----------|--------|-------------|
| `POST /counter/{id}/increment` | 40% | `entity-0` to `entity-999` |
| `GET /counter/{id}` | 30% | `entity-0` to `entity-999` |
| `PUT /kv/{id}/{key}` | 15% | `store-0` to `store-99`, `key-0` to `key-19` |
| `GET /kv/{id}/{key}` | 15% | `store-0` to `store-99`, `key-0` to `key-19` |

All traffic goes through the target URL (ALB). Connection pooling: 200 max connections.

## Report Output

Real-time stats every 5s during the test:

```
[ 35.0s] 312 rps | 1560 reqs (0 err) | p50=11.2ms
[ 40.0s] 298 rps | 1490 reqs (3 err) | p50=14.1ms
```

Final report (stdout + `report-<timestamp>.json`):

```
=== Casty Load + Resilience Report ===
Duration: 458s | Requests: 145,230 | Throughput: 317 rps
Success: 143,891 (99.1%) | Errors: 1,339 (0.9%)
Latency: p50=12.3ms  p95=45.1ms  p99=123.4ms  max=5012.3ms

By Operation:
  counter_increment        42,100 reqs  p50=14.1ms  p99=89.2ms
  counter_read             31,200 reqs  p50=11.2ms  p99=67.1ms
  kv_put                   10,800 reqs  p50=9.8ms   p99=45.3ms
  kv_get                   10,600 reqs  p50=8.9ms   p99=41.2ms

Fault Impact (10s window before vs after):
  [ 30.0s] node_crash         10.0.1.15 -> err +23, p50 12->34ms, rps 315->289
  [120.0s] leader_kill        10.0.1.10 -> err +45, p50 12->89ms, rps 310->201
  [210.0s] partition_start    10.0.1.19 -> err +12, p50 12->28ms, rps 308->295
  [280.0s] rolling_restart    10.0.1.10 -> err +5, p50 12->18ms, rps 312->301
```

The JSON report includes every individual request metric and fault event for post-hoc analysis.

## API Endpoints

The cluster app exposes:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check (node index + status) |
| `GET` | `/cluster/status` | Cluster members and unreachable nodes |
| `POST` | `/counter/{entity_id}/increment` | Increment counter (body: `{"amount": N}`) |
| `GET` | `/counter/{entity_id}` | Get counter value |
| `PUT` | `/kv/{entity_id}/{key}` | Set KV entry (body: `{"value": "..."}`) |
| `GET` | `/kv/{entity_id}/{key}` | Get KV entry |

All responses include the `X-Casty-Node` header identifying which node handled the request.

## File Structure

```
tests/loadtest/
  __init__.py
  __main__.py             # entry point: uv run python -m tests.loadtest
  config.py               # LoadTestConfig + argparse CLI
  load.py                 # async httpx workers generating HTTP traffic
  faults.py               # fault injection via asyncssh
  cluster.py              # cluster health polling, leader detection
  metrics.py              # MetricsCollector, RequestMetric, FaultEvent
  report.py               # stdout + JSON report generation
  timeline.py             # orchestrator: warmup → faults → cooldown → report
  app/
    main.py               # FastAPI app (Counter + KV sharded entities)
    entities.py            # Counter, KV actors (behavior recursion)
    config.py              # AppConfig from env vars
  infra/
    __main__.py            # Pulumi program
    Pulumi.yaml
    Pulumi.dev.yaml
    network.py             # VPC, subnets, security groups
    compute.py             # N cluster nodes + 1 load generator
    container.py           # ECR + Docker build
    load_balancer.py       # ALB
  Dockerfile              # cluster node image
  pyproject.toml          # isolated deps: httpx, asyncssh, pulumi, fastapi
  scripts/
    deploy.sh             # pulumi up + scp loadtest to load generator
    teardown.sh           # pulumi destroy
```

## Dependencies

The loadtest has its own `pyproject.toml`, independent from the main casty package:

| Group | Packages |
|-------|----------|
| Core | `httpx`, `asyncssh` |
| `[infra]` | `pulumi`, `pulumi-aws`, `pulumi-docker` |
| `[app]` | `casty`, `fastapi`, `uvicorn`, `pydantic` |

## Pulumi Configuration

Stack config in `infra/Pulumi.dev.yaml`:

| Key | Default | Description |
|-----|---------|-------------|
| `aws:region` | `us-east-1` | AWS region |
| `instance_type` | `t3.small` | EC2 instance type for cluster nodes |
| `node_count` | `10` | Number of cluster nodes |
| `key_name` | `""` | EC2 key pair name (required for SSH) |

The load generator is always a `t3.medium` at `10.0.1.100`.
