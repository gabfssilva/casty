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

The event stream is a system-wide publish/subscribe bus for observability, implemented as an actor. `system.event_stream` returns an `ActorRef[EventStreamMsg]` — you subscribe by spawning a handler actor and sending `EventStreamSubscribe`:

```python
from casty import (
    ActorStarted, ActorStopped, DeadLetter,
    EventStreamSubscribe, Behaviors,
)

def lifecycle_logger() -> Behavior[ActorStarted | ActorStopped | DeadLetter]:
    async def receive(ctx, msg):
        match msg:
            case ActorStarted(ref=ref):
                print(f"Started: {ref}")
            case ActorStopped(ref=ref):
                print(f"Stopped: {ref}")
            case DeadLetter(message=message):
                print(f"Dead letter: {message}")
        return Behaviors.same()
    return Behaviors.receive(receive)

logger = system.spawn(lifecycle_logger(), "lifecycle-logger")
system.event_stream.tell(EventStreamSubscribe(event_type=ActorStarted, handler=logger))
system.event_stream.tell(EventStreamSubscribe(event_type=ActorStopped, handler=logger))
system.event_stream.tell(EventStreamSubscribe(event_type=DeadLetter, handler=logger))
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

## Task Runner

The task runner is a system-level actor (`_task_runner`) that centralizes fire-and-forget coroutine execution. Instead of scattering bare `asyncio.create_task` calls — which produce orphan tasks that generate warnings on shutdown — all fire-and-forget work is routed through the task runner. It tracks every running task and cancels them all when it stops.

The system spawns the task runner automatically on the first `spawn()` call. You can also use it directly for your own fire-and-forget work:

```python
from casty import RunTask, TaskCompleted, TaskResult

# Fire and forget — no notification
system.lookup("/_task_runner").tell(RunTask(some_coroutine()))

# With completion notification
@dataclass(frozen=True)
class MyMsg:
    result: TaskResult

task_runner_ref.tell(RunTask(
    coro=some_coroutine(),
    reply_to=my_actor_ref,
    key="my-job",
))
# my_actor_ref will receive TaskCompleted("my-job"),
# TaskFailed("my-job", exc), or TaskCancelled("my-job")
```

The task runner is stopped last during shutdown, ensuring that fire-and-forget tasks from other actors are properly cancelled rather than orphaned.

---

**Next:** [Spy](spy.md)
