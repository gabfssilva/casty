# Distributed Rate Limiter Example

A complete example of a distributed rate limiter built with Casty's actor model and sharded entities.

## Features

- **Token Bucket Algorithm**: 10 req/sec refill rate, 20 token burst capacity
- **Sharded by User/API Key**: Each user gets a dedicated rate limiter entity routed via consistent hashing
- **Multi-node Cluster**: Demonstrates Casty's cluster coordination across 3+ nodes
- **Automatic Failover**: SWIM failure detection + hash ring recalculation
- **Split-Brain Reconciliation**: Three-way merge sums token consumption from both partitions
- **Docker Compose**: Easy local deployment

## Quick Start

### Single-Node Demo (Local)

Run the demo without Docker:

```bash
uv run python examples/06-rate-limiter/single_node.py
```

Output shows rate limiting in action:
```
alice:
✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✗✗✗✗✗✗✗✗✗✗
  Result: 20 allowed, 10 denied
```

The first 20 requests are allowed (burst capacity), then requests are denied until tokens refill.

### Multi-Node Cluster (Docker Compose)

Start a 10-node cluster with a client:

```bash
docker-compose up --build
```

This starts:
- **node-0 to node-9** (ports 9000-9009): Distributed cluster
  - node-0 is the seed node
  - Nodes 1-9 join via node-0
- **client**: Simulates 1000 users making 50 requests each (50,000 total requests)

The client connects to the cluster, spawns sharded rate limiters, and generates large-scale traffic. Each user is routed to a specific node via consistent hashing across the 10 nodes.

Expected output:
```
Simulating 1000 users making requests...
Rate: 10 tokens/sec | Burst: 20 tokens | Requests per user: 50

Total Requests: 50,000
  Allowed: 21,500 (43.0%)
  Denied:  28,500 (57.0%)

Sample User Statistics (every 100th user):
user-0     | Tokens:   0.53 | Requests: 50 (allowed: 21, denied: 29)
user-100   | Tokens:   0.40 | Requests: 50 (allowed: 22, denied: 28)
...

Throughput: 20,500.0 req/s
```

## Architecture

### Single-Node View

```
┌─────────────────────────────────────┐
│     ActorSystem (Local)             │
│  ┌──────────────────────────────┐  │
│  │  RateLimiter["user-0"]       │  │
│  │  RateLimiter["user-1"]       │  │
│  │  RateLimiter["user-2"]       │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Distributed View (10-Node Cluster)

```
Nodes 0-9 (ports 9000-9009) with ~100 users per node:

┌─────────────────────────────────────────────────────────────────────┐
│                     10-Node Distributed Cluster                     │
│                                                                      │
│  Node-0  Node-1  Node-2  Node-3  Node-4  Node-5  Node-6  Node-7   │
│  (9000)  (9001)  (9002)  (9003)  (9004)  (9005)  (9006)  (9007)   │
│                                                                      │
│  User-0  User-100 User-200 User-300 User-400 User-500 User-600... │
│  User-10 User-110 User-210 User-310 User-410 User-510 User-610... │
│  ...     ...     ...     ...     ...     ...     ...     ...       │
│  User-90 User-190 User-290 User-390 User-490 User-590 User-690... │
│                                                                      │
│  Node-8  Node-9                                                     │
│  (9008)  (9009)                                                     │
│                                                                      │
│  User-800 User-900                                                  │
│  User-810 User-910                                                  │
│  ...      ...                                                       │
│  User-890 User-990                                                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
         ▲                                               ▲
         │                                               │
         └───────────────────┬─────────────────────────┘
                    Cluster Network (gossip + SWIM)
```

Each node owns ~100 users (via consistent hashing).
Users are routed to their owner node automatically.

**How it works:**
1. User spawns sharded rate limiters with `name="rate-limiters"`
2. When accessing `limiters["user-0"]`, consistent hashing determines owner node
3. Request is routed to owner node via cluster networking
4. Owner node's RateLimiter actor processes the request
5. If node fails, SWIM detects it and hash ring recalculates owner

### Sharded Entity Routing

```python
# Client code
limiters = await system.spawn(RateLimiter, name="rate-limiters", sharded=True)

# This access:
await limiters["user-123"].ask(CheckToken())

# Routes to:
1. Compute hash("rate-limiters:user-123")
2. Find owner node via consistent hashing
3. Send request to that node
4. That node's RateLimiter["user-123"] processes it
```

## Token Bucket Algorithm

The rate limiter uses a sliding window token bucket:

- **Tokens**: Floating-point count (e.g., 15.5 tokens)
- **Refill**: `tokens += (elapsed_time * rate)` capped at `capacity`
- **Rate**: 10 tokens/second
- **Capacity**: 20 tokens (burst size)
- **Check**: `if tokens >= 1.0: tokens -= 1.0; allow() else: deny()`

**Example timeline:**
```
t=0s:   tokens=20.0 [grant]  → tokens=19.0
t=0s:   tokens=19.0 [grant]  → tokens=18.0
...
t=0s:   tokens=1.0  [grant]  → tokens=0.0
t=0.05s tokens=0.0  [refill] → tokens=0.5
t=0.05s tokens=0.5  [deny]            (need 1.0)
t=0.1s  tokens=1.0  [refill] → tokens=1.0
t=0.1s  tokens=1.0  [grant]  → tokens=0.0
```

## Three-Way Merge (Split-Brain)

If a network partition causes two nodes to own the same entity:

```python
def __casty_merge__(self, base: 'RateLimiter', other: 'RateLimiter'):
    # Both sides consumed tokens independently
    my_consumed = base.capacity - self.tokens        # 5 tokens consumed
    their_consumed = base.capacity - other.tokens    # 3 tokens consumed

    # Sum the consumption (prevents token duplication)
    total_consumed = my_consumed + their_consumed    # 8 tokens total
    self.tokens = max(0, base.capacity - total_consumed)  # 20 - 8 = 12
```

This ensures that in split-brain scenarios, the merged state respects the true quota consumption from both sides.

## Testing

### Local Functional Test
```bash
uv run python examples/06-rate-limiter/single_node.py
```

Verify:
- ✓ First 20 requests allowed (burst capacity)
- ✓ Next 10 requests denied (rate limited)
- ✓ Final tokens < 20 (partial refill)

### Docker Failover Test
```bash
# Start cluster
docker-compose up

# In another terminal, kill a node
docker-compose stop node-1

# Watch client logs - should continue without errors
docker-compose logs client -f
```

Verify:
- ✓ Client requests still succeed
- ✓ Entities reroute to remaining nodes
- ✓ No significant increase in denial rate

### Performance
```bash
docker-compose up
# Watch output for throughput metrics
```

Expected: 250+ requests/second with 3 nodes

## Files

- **`rate_limiter.py`**: Core RateLimiter actor implementation
- **`single_node.py`**: Local single-node demo
- **`cluster_node.py`**: Docker cluster node entrypoint
- **`client.py`**: Load generator that simulates traffic
- **`docker-compose.yml`**: 3-node cluster + client orchestration
- **`Dockerfile`**: Container image
- **`README.md`**: This file

## Implementation Details

### Actor Definition

```python
@dataclass
class CheckToken:
    """Request a token - reply with bool"""
    pass

@dataclass
class GetStatus:
    """Query state - reply with dict"""
    pass

class RateLimiter(Actor[CheckToken | GetStatus | Reset]):
    def __init__(self, entity_id: str, rate: float = 10.0, capacity: int = 20):
        self.entity_id = entity_id
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()
        ...

    async def receive(self, msg, ctx: Context):
        match msg:
            case CheckToken():
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    ctx.reply(True)
                else:
                    ctx.reply(False)
```

### Spawning Sharded

```python
# Each node spawns the same sharded actor
limiters = await system.spawn(
    RateLimiter,
    name="rate-limiters",
    sharded=True,
)

# Access routes through cluster
await limiters["user-123"].ask(CheckToken())
```

## Configuration

Edit the rate and capacity in the source files:

**`cluster_node.py`** (line ~56):
```python
limiters = await system.spawn(
    RateLimiter,
    name="rate-limiters",
    sharded=True,
    # Add kwargs here to customize rate/capacity:
    # rate=20.0, capacity=50,
)
```

**`client.py`** (line ~107):
```python
limiters = await system.spawn(
    RateLimiter,
    name="rate-limiters",
    sharded=True,
)
```

## Troubleshooting

### Client can't connect to cluster
```
Error: Failed to connect to cluster
```

- Ensure nodes are healthy: `docker-compose logs node-0`
- Wait longer for cluster formation (takes ~3-5s)
- Check network connectivity: `docker network ls`

### Requests timing out
```
⚠️  user-0 request timeout
```

- Nodes might be overloaded - reduce request rate in `client.py` (increase sleep)
- Check node logs: `docker-compose logs node-1`

### High denial rate (>50%)
- Expected if request rate exceeds refill rate (10 req/sec per user)
- Increase `capacity` to allow larger bursts
- Or reduce number of simultaneous users

## Next Steps

Extend this example:
1. Add persistence (WAL) to survive node restarts
2. Implement different quota strategies (per-IP, per-tier)
3. Add metrics/monitoring (request count, denial rate)
4. Implement sliding window or leaky bucket variants
5. Add distributed quota pooling (borrow tokens from other nodes)

## References

- [Casty Documentation](https://github.com/gabfssilva/casty)
- [Token Bucket Algorithm](https://en.wikipedia.org/wiki/Token_bucket)
- [Actor Model](https://en.wikipedia.org/wiki/Actor_model)
- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
