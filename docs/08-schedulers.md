# Schedules

A schedule sends a message to its actor at a wall-clock time, once or periodically. It is saved with the actor's state, so the current owner can resume it after a restart or ownership change.

## Scheduling a message

`await ctx.schedule(name, delay, interval, message)` creates or replaces a schedule. `delay` sets its first due time; `interval=None` makes it a one-shot schedule. A `timedelta` interval repeats it.

This program schedules a one-shot message and forwards the original reply target to it:

```python
import asyncio
from dataclasses import dataclass
from datetime import timedelta

from casty import ActorSystem, Askable, Context, Ref, actor


@dataclass(frozen=True)
class Start(Askable[int]):
    pass


@dataclass(frozen=True)
class Tick:
    reply_to: Ref[int]


@actor(initial=0)
async def ticker(ctx: Context[int, Start | Tick]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Start(reply_to=reply_to):
                await ctx.schedule("tick", timedelta(milliseconds=10), None, Tick(reply_to))
            case Tick(reply_to):
                total = await ctx.state.update(lambda value: value + 1)
                reply_to.tell(total)


async def main() -> None:
    async with ActorSystem() as system:
        ticks = system.ref(ticker, "ticks")
        print(await ticks.ask(Start()))


asyncio.run(main())
```

It prints `1` after the scheduled `Tick` is processed. The actor continues reading its mailbox after handling `Start`; it answers that request later through the ref carried by `Tick`.

A name identifies one schedule within one actor. Scheduling under an existing name replaces it. `ctx.schedules` maps names to handles; `await handle.cancel()` removes the corresponding schedule.

## Stored representation

The reserved `@schedules` page contains each schedule's name, serialized message, next due time and optional period. Due times and periods are encoded in microseconds; due times are measured from the Unix epoch.

Creating, replacing or cancelling a schedule writes this page through the same [state protocol](05-replication.md) as application state. A behavior change preserves it because the replacement accepts the same messages.

A new owner decodes the page and installs timers for the schedules it recovers. Only the active owner sends their messages. Replication preserves the schedules while replicas survive; persistence across the loss of every replica requires a [durable actor](03-state.md#memory-and-durable-storage).

## Activation and time

An actor with schedules does not idle out while waiting for its next occurrence. A stored schedule is nevertheless not an independent activation service: an inactive key starts through a ref, a message or recovery of its active marker. The clock alone does not start an inactive key.

Intervals follow the stored due time. If execution resumes late, the runtime advances to the first interval after the current time rather than emitting every missed occurrence in a burst. Different nodes must have synchronized wall clocks for these due times to have consistent meaning.

A timer inside a body's `TaskGroup` instead belongs to that run. It disappears when the body ends and has no stored schedule to recover. `ctx.merge` with a ticker can serve that per-run use case; `ctx.schedule` records work that should follow saved actor state.

## Delivery and external work

An ownership change can miss or repeat an occurrence. Scheduled delivery does not provide exactly-once execution of an external action.

For a daily report, the actor can record the last completed or reserved day in state. Saving the day before building the report prevents a retry from rebuilding that day's report, but a crash between the save and the build can omit it. Building first and saving afterward permits a repeated build after a crash. Choose the checkpoint according to which outcome the application can handle, and use an idempotency key when the external service supports one.

Schedule storage and external effects are separate operations. A quorum-confirmed schedule or checkpoint does not transactionally commit a database update, email or HTTP request made by the body.

Implementation: [schedule encoding and interval calculation](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/schedule.rs), [schedule handles](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/context.rs) and [timers and delivery](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/activation.rs).
