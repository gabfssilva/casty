# Pipe to Self

Actors process one message at a time. When an actor needs to call an async function — an HTTP request, a database query, file I/O — awaiting it inside the handler blocks the mailbox. No other messages can be processed until the coroutine completes.

[Request-reply](request-reply.md) solves actor-to-actor communication, but what about plain async functions that aren't actors?

`ctx.pipe_to_self()` dispatches a coroutine as a background `asyncio.Task` and sends the mapped result back to the actor's mailbox, preserving the sequential message-processing guarantee without blocking.

```python
import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class FetchUser:
    user_id: str

@dataclass(frozen=True)
class UserFound:
    name: str

@dataclass(frozen=True)
class UserFetchFailed:
    error: str

@dataclass(frozen=True)
class GetResult:
    reply_to: ActorRef[str]

type FetcherMsg = FetchUser | UserFound | UserFetchFailed | GetResult


async def fetch_user_from_api(user_id: str) -> str:
    """Simulate an async API call."""
    await asyncio.sleep(0.1)
    return f"User-{user_id}"


def fetcher(result: str = "") -> Behavior[FetcherMsg]:
    async def receive(ctx: ActorContext[FetcherMsg], msg: FetcherMsg) -> Behavior[FetcherMsg]:
        match msg:
            case FetchUser(user_id=uid):
                ctx.pipe_to_self(
                    fetch_user_from_api(uid),
                    lambda name: UserFound(name=name),
                    on_failure=lambda exc: UserFetchFailed(error=str(exc)),
                )
                return Behaviors.same()
            case UserFound(name=name):
                return fetcher(result=name)
            case UserFetchFailed(error=error):
                return fetcher(result=f"error: {error}")
            case GetResult(reply_to=reply_to):
                reply_to.tell(result)
                return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem("pipe-demo") as system:
        ref = system.spawn(fetcher(), "fetcher")

        ref.tell(FetchUser(user_id="42"))
        await asyncio.sleep(0.2)

        result = await system.ask(ref, lambda r: GetResult(reply_to=r), timeout=2.0)
        print(f"Result: {result}")  # Result: User-42


asyncio.run(main())
```

The actor receives `FetchUser`, kicks off the async operation in the background, and immediately returns `Behaviors.same()` — the mailbox stays open. When the coroutine completes, the `mapper` wraps the result into a `UserFound` message that arrives through the normal mailbox, processed in order with everything else.

## Error handling

The optional `on_failure` parameter maps exceptions to messages:

```python
ctx.pipe_to_self(
    fetch_user_from_api(user_id),
    lambda name: UserFound(name=name),
    on_failure=lambda exc: UserFetchFailed(error=str(exc)),
)
```

If `on_failure` is omitted and the coroutine raises, a warning is logged and the exception is discarded. The actor continues processing other messages normally.

## Signature

```python
def pipe_to_self[T](
    self,
    coro: Awaitable[T],
    mapper: Callable[[T], M],
    on_failure: Callable[[Exception], M] | None = None,
) -> None
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `coro` | `Awaitable[T]` | The async operation to run in background |
| `mapper` | `Callable[[T], M]` | Maps the successful result to a message for this actor |
| `on_failure` | `Callable[[Exception], M] \| None` | Maps a failure to a message. If `None`, failures are logged and discarded |

!!! warning
    The coroutine runs outside the actor's sequential message processing. Do not capture mutable state from the closure — by the time the coroutine completes, the actor may have moved to a different behavior with different state. The result should arrive as a message and be handled like any other.

---

**Next:** [Spy](spy.md)
