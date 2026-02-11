# Cluster Broadcast

Sharding routes messages to a **single** entity via its ID. But some scenarios require sending a message to **all** nodes — configuration updates, cache invalidation, system-wide announcements. `Behaviors.broadcasted()` wraps any behavior so that `tell()` automatically fans out to every cluster member and `ask()` collects all responses:

```python
@dataclass(frozen=True)
class Announcement:
    text: str
    reply_to: ActorRef[Ack]

@dataclass(frozen=True)
class Ack:
    from_node: str

def listener() -> Behavior[Announcement]:
    node = socket.gethostname()

    async def receive(_ctx, msg: Announcement) -> Behavior[Announcement]:
        msg.reply_to.tell(Ack(from_node=node))
        return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ClusteredActorSystem(
        name="demo", host="127.0.0.1", port=25520,
        node_id="node-1",
    ) as system:
        # BroadcastRef — tell/ask fan out to ALL nodes
        ref: BroadcastRef[Announcement] = system.spawn(
            Behaviors.broadcasted(listener()), "listener"
        )

        # ask() returns tuple[Ack, ...] — one per node
        acks: tuple[Ack, ...] = await system.ask(
            ref,
            lambda r: Announcement(text="Hello cluster!", reply_to=r),
            timeout=5.0,
        )
        for ack in acks:
            print(f"Ack from {ack.from_node}")

asyncio.run(main())
```

Under the hood, each node spawns a local copy of the actor at `/_bcast-{name}` and a proxy at `/{name}`. The proxy tracks cluster membership via gossip and fans out messages to all `up` members — locally or over TCP. The `BroadcastRef[M]` subclass of `ActorRef[M]` enables typesafe overloads: `ask(BroadcastRef, ...)` returns `tuple[R, ...]` instead of `R`.

---

**Next:** [Cluster Singleton](cluster-singleton.md)
