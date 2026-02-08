# Example 13 — Cluster Broadcast

A 5-node cluster where the elected leader broadcasts an announcement to
every member and collects acknowledgements — all with a single `ask()` call.

## What it demonstrates

- **`Behaviors.broadcasted()`** — spawns the actor on every node automatically
- **`BroadcastRef[M]`** — `tell()` fans out to all nodes, `ask()` collects all responses
- **Typesafe broadcast ask** — `ask(BroadcastRef, ...)` returns `tuple[Ack, ...]`
- **Cluster formation** — 5 Docker containers discover each other via gossip
- **Barrier synchronization** — all nodes wait for each other before shutdown

## Architecture

```
ref.tell(m) → broadcast_proxy → node-1: /_bcast-listener.tell(m)
                               → node-2: /_bcast-listener.tell(m) (TCP)
                               → node-3: /_bcast-listener.tell(m) (TCP)
                               → node-4: /_bcast-listener.tell(m) (TCP)
                               → node-5: /_bcast-listener.tell(m) (TCP)
```

Every node spawns a **broadcasted listener**. The proxy tracks cluster membership
and fans out messages to all `up` members.

## Key code

### Spawn — creates BroadcastRef

```python
listener: BroadcastRef[ListenerMsg] = system.spawn(
    Behaviors.broadcasted(listener_actor()), "listener"
)
```

### Ask — collects responses from all nodes

```python
acks: tuple[Ack, ...] = await system.ask(
    listener,
    lambda r: Announcement(text="Hello!", reply_to=r),
    timeout=5.0,
)
```

### Tell — fire-and-forget to all nodes

```python
listener.tell(SomeMessage(...))
```

## Running

```bash
cd examples/13_broadcast
docker compose up --build
```

Scale to more nodes by editing `docker-compose.yml` and the `--nodes` flag.

### Local (without Docker)

Terminal 1 (seed node):
```bash
uv run python examples/13_broadcast/main.py --port 25520 --nodes 3
```

Terminal 2:
```bash
uv run python examples/13_broadcast/main.py --port 25521 --seed 127.0.0.1:25520 --nodes 3
```

Terminal 3:
```bash
uv run python examples/13_broadcast/main.py --port 25522 --seed 127.0.0.1:25520 --nodes 3
```

## Expected output

```
01:47:44  INFO    node-5  I am the leader — starting broadcast
01:47:44  INFO    node-5  Broadcasting round #1...
01:47:44  INFO    node-5    ack from node-3 for #1
01:47:44  INFO    node-5    ack from node-5 for #1
01:47:44  INFO    node-5    ack from node-2 for #1
01:47:44  INFO    node-5    ack from node-4 for #1
01:47:44  INFO    node-5    ack from node-1 for #1
01:47:44  INFO    node-5  Round #1: 5 acks received
...
01:47:48  INFO    node-5  Broadcast complete!
```
