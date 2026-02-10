# Configuration

Every parameter shown in previous sections — mailbox capacity, supervision strategy, gossip interval, failure detector threshold — can be set programmatically. But when deploying the same codebase across environments (dev, staging, production), hardcoding these values becomes impractical. Casty supports TOML-based configuration via a `casty.toml` file.

Configuration is always optional. When absent, all parameters use the same defaults as the programmatic API. When present, the TOML file produces the existing dataclasses — `ClusterConfig`, `ReplicationConfig`, etc. — through a pure loader function. No global state, no singletons, no magic.

## File Structure

A complete `casty.toml` covering all configurable aspects:

```toml
[system]
name = "my-app"

# --- Global defaults (apply to all actors) ---
[defaults.mailbox]
capacity = 1000
strategy = "drop_new"         # drop_new | drop_oldest | backpressure

[defaults.supervision]
strategy = "restart"          # restart | stop | escalate
max_restarts = 3
within_seconds = 60.0

[defaults.sharding]
num_shards = 256

[defaults.replication]
replicas = 2
min_acks = 1
ack_timeout = 5.0

# --- Cluster ---
[cluster]
host = "0.0.0.0"
port = 25520
seed_nodes = ["node1:25520", "node2:25520"]
roles = ["worker"]

[cluster.gossip]
interval = 1.0

[cluster.heartbeat]
interval = 0.5
availability_check_interval = 2.0

[cluster.tls]
certfile = "certs/node.pem"
cafile = "certs/ca.pem"
# keyfile = "certs/node.key"  # optional, if key is separate from certfile

[cluster.failure_detector]
threshold = 8.0
max_sample_size = 200
min_std_deviation_ms = 100.0
acceptable_heartbeat_pause_ms = 0.0
first_heartbeat_estimate_ms = 1000.0

# --- Per-actor overrides ---
[actors.orders]
sharding = { num_shards = 512 }
replication = { replicas = 3, min_acks = 2 }

[actors.my-worker]
mailbox = { capacity = 5000, strategy = "backpressure" }
supervision = { strategy = "stop" }

[actors."child-\\d+"]
mailbox = { capacity = 100, strategy = "drop_oldest" }
```

Every key is optional. A minimal `casty.toml` can be just:

```toml
[system]
name = "my-app"
```

## Loading Configuration

`load_config()` accepts an explicit path or discovers `casty.toml` automatically by walking up from the current working directory (like `pyproject.toml`):

```python
from casty import load_config

# Auto-discovery — walks up from CWD looking for casty.toml
config = load_config()

# Explicit path
config = load_config(Path("infra/casty.toml"))
```

For a local `ActorSystem`, pass the config to the constructor:

```python
from casty import ActorSystem, load_config

config = load_config()

async with ActorSystem(config=config) as system:
    ref = system.spawn(my_behavior(), "my-actor")
    # mailbox and supervision come from casty.toml
```

For a `ClusteredActorSystem`, use `from_config` to derive host, port, seed nodes, and cluster tuning from the `[cluster]` section:

```python
from casty import load_config
from casty.sharding import ClusteredActorSystem

config = load_config()

async with ClusteredActorSystem.from_config(config) as system:
    # host, port, seed_nodes, roles, TLS, gossip interval,
    # heartbeat interval, failure detector — all from casty.toml
    ...
```

Programmatic overrides take precedence over the file — useful for dynamic ports in tests or container orchestration:

```python
async with ClusteredActorSystem.from_config(
    config,
    host="10.0.0.5",
    port=0,  # OS-assigned port
) as system:
    ...
```

## Per-Actor Overrides

Actor keys under `[actors.*]` are matched against the actor name at `spawn()` time using `re.fullmatch()`. Simple names like `orders` match exactly. Regex patterns use quoted TOML keys:

```toml
# Exact match — only the actor named "orders"
[actors.orders]
mailbox = { capacity = 5000 }

# Regex — matches "sensor-001", "sensor-042", etc.
[actors."sensor-\\d+"]
mailbox = { capacity = 100, strategy = "drop_oldest" }
```

Resolution order: **per-actor override > `[defaults.*]` > dataclass defaults**. First matching pattern wins (definition order in TOML).

```toml
[defaults.mailbox]
capacity = 1000
strategy = "drop_new"

[actors.orders]
mailbox = { capacity = 5000 }
# strategy inherits "drop_new" from [defaults.mailbox]
```

Internal actors (names starting with `_`) are never resolved against config — they always use framework defaults.
