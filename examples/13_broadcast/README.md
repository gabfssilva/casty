# Example 13 — Cluster Broadcast

A 5-node cluster where the elected leader broadcasts an announcement to
every member and collects acknowledgements.

## What it demonstrates

- **Cluster formation** — 5 Docker containers discover each other via gossip
- **Leader election** — deterministic leader picks itself as the broadcaster
- **Remote lookup** — `system.lookup(path, node=address)` resolves actors on
  remote nodes
- **Request-reply across nodes** — `ask` with a remote ref, ack flows back
  over TCP
- **Barrier synchronization** — all nodes wait for each other before shutdown

## Architecture

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  node-1  │    │  node-2  │    │  node-3  │    │  node-4  │    │  node-5  │
│ /listener│    │ /listener│    │ /listener│    │ /listener│    │ /listener│
└────▲─────┘    └────▲─────┘    └────▲─────┘    └────▲─────┘    └────▲─────┘
     │               │               │               │               │
     └───────────────┴───────────────┴───────┬───────┴───────────────┘
                                             │
                                        leader node
                                     (broadcast loop)
```

Every node spawns a **listener** actor at the well-known path `/listener`.
The leader queries cluster state, iterates all `up` members, and sends an
`Announcement` to each via remote lookup. Each listener replies with an `Ack`.

## Key code

### Messages

```python
@dataclass(frozen=True)
class Announcement:
    text: str
    seq: int
    from_node: str
    reply_to: ActorRef[Ack]

@dataclass(frozen=True)
class Ack:
    seq: int
    from_node: str
```

### Listener (runs on every node)

```python
def listener_actor() -> Behavior[ListenerMsg]:
    node = socket.gethostname()

    async def receive(_ctx, msg):
        msg.reply_to.tell(Ack(seq=msg.seq, from_node=node))
        return Behaviors.same()

    return Behaviors.receive(receive)
```

### Broadcast (runs on leader only)

```python
state = await system.get_cluster_state()
up_members = [m for m in state.members if m.status == MemberStatus.up]

for member in up_members:
    ref = system.lookup("/listener", node=member.address)
    ack = await system.ask_or_none(ref, lambda r: Announcement(..., reply_to=r), timeout=5.0)
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
01:47:44  INFO    node-5  Broadcasting round #1 to 5 nodes...
01:47:44  INFO    node-5    ack from node-3 for #1
01:47:44  INFO    node-5    ack from node-5 for #1
01:47:44  INFO    node-5    ack from node-2 for #1
01:47:44  INFO    node-5    ack from node-4 for #1
01:47:44  INFO    node-5    ack from node-1 for #1
01:47:44  INFO    node-5  Round #1: 5/5 acks received
...
01:47:48  INFO    node-5  Broadcast complete!
```
