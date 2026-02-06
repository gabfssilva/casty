# 12 — AWS Cluster

10 Casty nodes on AWS EC2 behind an ALB, each running FastAPI with sharded Counter + KV entities.

## Architecture

```
Internet → ALB (:80) → 10x EC2 (FastAPI :8000 + Casty TCP :25520)
                         └─ Sharded Counter + KV entities
```

- Node 0 = seed node (IP `10.0.1.10`), nodes 1-9 join via seed
- Any node can handle any request (routes to correct shard via proxy)
- Docker image built locally, pushed to ECR, pulled by EC2 user-data

## Prerequisites

- AWS CLI configured (`aws configure`)
- [Pulumi CLI](https://www.pulumi.com/docs/install/) installed
- Docker running locally

## Local Testing

```bash
cd examples/12_aws_cluster

# Run a single node locally
NODE_INDEX=0 SEED_IPS=127.0.0.1 uv run uvicorn app.main:app --host 0.0.0.0 --port 8000

# Test endpoints
http GET localhost:8000/health
http POST localhost:8000/counter/order-1/increment amount:=5
http GET localhost:8000/counter/order-1
http PUT localhost:8000/kv/user-1/name value="Alice"
http GET localhost:8000/kv/user-1/name
```

## Deploy to AWS

```bash
cd examples/12_aws_cluster/infra

# Install infra dependencies
uv sync --extra infra

# Initialize Pulumi stack
uv run pulumi stack init dev

# (Optional) Set SSH key for debugging
uv run pulumi config set casty-aws-cluster:key_name your-key-name

# Preview
uv run pulumi preview

# Deploy
uv run pulumi up

# Get ALB URL
uv run pulumi stack output alb_url
```

## Test the Cluster

```bash
# Using the test script
./scripts/test_cluster.sh $(cd infra && uv run pulumi stack output alb_url)
```

## Teardown

```bash
cd infra
uv run pulumi destroy
uv run pulumi stack rm dev
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | ALB health check |
| GET | `/cluster/status` | Cluster members and state |
| POST | `/counter/{entity_id}/increment` | Increment counter (body: `{"amount": 1}`) |
| GET | `/counter/{entity_id}` | Get counter value |
| PUT | `/kv/{entity_id}/{key}` | Set KV entry (body: `{"value": "..."}`) |
| GET | `/kv/{entity_id}/{key}` | Get KV entry |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_INDEX` | `0` | Node index (0 = seed) |
| `SEED_IPS` | `127.0.0.1` | Comma-separated seed node IPs |
| `CASTY_PORT` | `25520` | Casty TCP port |
| `HTTP_PORT` | `8000` | FastAPI HTTP port |

Pulumi config:

| Key | Default | Description |
|-----|---------|-------------|
| `instance_type` | `t3.small` | EC2 instance type |
| `node_count` | `10` | Number of nodes |
| `key_name` | (none) | SSH key pair name |
