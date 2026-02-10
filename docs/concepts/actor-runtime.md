# Actor Runtime

The following concepts control the operational behavior of actors. They are grouped here because each is important but self-contained — none changes the fundamental mental model established in previous sections.

## Lifecycle Hooks

Actors transition through a defined lifecycle: start, stop, and (if supervised) restart. Lifecycle hooks allow executing side effects at each boundary — acquiring resources on start, releasing them on stop, logging on restart:

```python
def my_actor() -> Behavior[str]:
    async def pre_start(ctx: ActorContext[str]) -> None:
        ctx.log.info("Actor starting")

    async def post_stop(ctx: ActorContext[str]) -> None:
        ctx.log.info("Actor stopped")

    async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
        return Behaviors.same()

    return Behaviors.with_lifecycle(
        Behaviors.receive(receive),
        pre_start=pre_start,
        post_stop=post_stop,
    )
```

Available hooks: `pre_start` (before first message), `post_stop` (after final message), `pre_restart` (before restart), `post_restart` (after restart, before first message of new incarnation).

## Event Stream

The `EventStream` is a system-wide publish/subscribe bus for observability. Every significant actor lifecycle event is published automatically:

```python
system.event_stream.subscribe(ActorStarted, lambda e: print(f"Started: {e.ref}"))
system.event_stream.subscribe(ActorStopped, lambda e: print(f"Stopped: {e.ref}"))
system.event_stream.subscribe(DeadLetter, lambda e: print(f"Dead letter: {e.message}"))
```

A `DeadLetter` is published when a message is sent to an actor that has already stopped. This is valuable for debugging message routing issues.

Available events: `ActorStarted`, `ActorStopped`, `ActorRestarted`, `DeadLetter`, `UnhandledMessage`. In clustered mode, additional events are published: `MemberUp`, `MemberLeft`, `UnreachableMember`, `ReachableMember`.

## Mailbox Configuration

Each actor has a mailbox — a bounded queue that buffers incoming messages. When messages arrive faster than the actor can process them, the overflow strategy determines what happens:

```python
ref = system.spawn(
    my_behavior(),
    "bounded-actor",
    mailbox=Mailbox(capacity=100, overflow=MailboxOverflowStrategy.drop_oldest),
)
```

| Strategy | Behavior |
|----------|----------|
| `drop_new` (default) | Discard the incoming message when the mailbox is full |
| `drop_oldest` | Discard the oldest message in the mailbox to make room for the new one |
| `backpressure` | Raise `asyncio.QueueFull`, propagating pressure to the sender |

## Scheduling

The scheduler is an actor (spawned lazily by the system) that manages timed message delivery. Two patterns are supported: periodic ticks and one-shot delays.

```python
# Send a BalanceReport to the account actor every 30 seconds
system.tick("report", account_ref, BalanceReport(), interval=30.0)

# Send a Timeout message after 5 seconds
system.schedule("timeout", account_ref, Timeout(), delay=5.0)

# Cancel a scheduled task
system.cancel_schedule("report")
```

Scheduled tasks are identified by a string key. Scheduling a new task with the same key cancels the previous one.

---

**Next:** [Spy](spy.md)
