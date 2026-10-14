# Messaging

Messages are immutable values described by the actor's `Context[S, M]` annotation. A ref accepts that message type, and an `Askable[R]` declares the reply type `R`. The typed contract lets static checkers validate both the message and its answer.

## Tell and ask

`tell` sends a message without waiting for an answer. `ask` supplies a reply target and waits for the response sent to it. The actor replies through `msg.reply_to.tell(value)`.

This program uses the same actor for both operations:

```python
import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Askable, Context, actor


@dataclass(frozen=True)
class Add(Askable[int]):
    amount: int


@dataclass(frozen=True)
class Count(Askable[int]):
    pass


@actor(initial=0)
async def counter(ctx: Context[int, Add | Count]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Add(amount):
                total = await ctx.state.update(lambda value: value + amount)
                msg.reply_to.tell(total)
            case Count():
                msg.reply_to.tell(ctx.state.value)


async def main() -> None:
    async with ActorSystem() as system:
        views = system.ref(counter, "page-views")
        views.tell(Add(2))
        print(await views.ask(Count()))
        print(await views.ask(Add(3)))


asyncio.run(main())
```

The program prints `2`, then `5`. In this local execution, messages from one system to one key arrive in order. The first `tell` does not wait, but the subsequent `Count` is processed after it. In a cluster, that ordering applies while the owner remains the same.

`reply_to` is keyword-only and does not participate in positional dataclass matching. `case Add(amount)` therefore matches the application field. `ask` encodes its own reply target without changing the message value passed by the caller. An `Askable` sent with `tell` keeps the reply target it was constructed with; its default target discards the answer.

## References and reply targets

An entity target contains an actor type name and key. A reply target contains a node identity and request identifier. The caller registers a future under that identifier, and a response resolves it after decoding with the reply schema.

Reply targets do not require a separately activated actor. This allows a `Client` to receive answers despite hosting no actor keys. A node's process incarnation distinguishes current requests from a previous process at the same address.

A ref can be stored or forwarded in a message. A received entity ref does not itself create a missing key; for a type without a default initial state, using such a ref can raise `NotStarted`. Obtaining a ref through a system starts the key with its supplied or default initial value.

## Deadlines and cancellation

`ask` uses the actor type's or system's `ask_timeout`, ten seconds by default. An enclosing `asyncio.timeout` can impose a shorter application deadline. A timeout raises `TimeoutError`; caller cancellation raises `CancelledError`.

The runtime informs the destination when the caller stops waiting. A request still queued is dropped. If its body is already handling the request, that run is cancelled at its current await and restarted for subsequent work. A reply already sent is not taken back, and completed writes and external effects are not undone.

A deadline therefore limits waiting, not whether the operation had an effect. [Failures](10-failures.md) describes how to treat an uncertain outcome.

## Asks between actors

Awaiting `ref.ask` inside a body holds up that activation's next message. If A waits for B and B asks A before answering, neither body could read the message needed to proceed. The runtime carries a chain of waiting runs, detects that cycle and raises `ReentrancyError` immediately.

`ctx.ask(target, message, mapper)` avoids holding up the mailbox. It starts the ask and returns; when the reply arrives, `mapper` turns it into a message for this actor. The body handles that result in the normal inbox order. `failed=` can similarly map an exception into a failure message.

The mapper runs after the body has moved on. Bind the originating message or identifier when constructing it, for example `lambda answer, request=msg: Completed(request, answer)`. Otherwise a closure over the loop variable may use the message from a later iteration.

See the [actors talking example](https://github.com/gabfssilva/casty/tree/main/examples/02-actors-talking) for the complete message types and result flow.

## Asynchronous work

Use `ctx.to_self(work, mapper, failed=...)` to run an awaitable and return its result as a message. This preserves the inbox as the point where the actor applies results to state. Work piped this way can complete after the actor became idle and reactivate the key when its result arrives.

Await state writes, behavior changes and schedule changes directly: the next message should observe those transitions. Pipe independent service calls or groups of asks when the actor should keep receiving messages while they run.

A blocking call runs on the event loop if invoked directly. Put it in `asyncio.to_thread` and pipe that awaitable back into the inbox. For substantial Python CPU work on a GIL build, a process pool avoids tying computation to the event loop or the interpreter lock. Free-threaded builds can also execute Python computation on threads.

Tasks created inside a body's `TaskGroup` belong to that run and end with it. Work that must be resumed after failure needs a checkpoint in state. Only the inbox reader should apply state transitions from concurrent work.

`ctx.merge(source)` combines mailbox messages with values from an asynchronous iterable. See the [stream example](https://github.com/gabfssilva/casty/tree/main/examples/03-streams) and [consumer example](https://github.com/gabfssilva/casty/tree/main/examples/08-consumers).

## Mailbox bounds

`@actor(mailbox=n)` bounds queued messages. With `on_full="refuse"`, an ask to a full mailbox raises `MailboxFull`. With `on_full="wait"`, it waits within its deadline for space. A tell to a full mailbox is dropped in either mode and reported to the observer.

Message delivery is at most once. Neither a successful send nor an expired deadline establishes that application work completed. The [failure and observation chapter](10-failures.md) covers those outcomes.

Implementation: [reply serialization](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/schema/dump.rs), [reply futures](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/replies.rs), [routing](https://github.com/gabfssilva/casty/blob/main/crates/casty-node/src/routing/service.rs), [mailboxes](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/mailbox.rs) and [ask chains](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/chain.rs).
