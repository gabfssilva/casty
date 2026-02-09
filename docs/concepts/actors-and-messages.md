# Actors and Messages

In the actor model, an **actor** is the fundamental unit of computation. When an actor receives a message, it can do exactly three things:

1. **Send messages** to other actors it knows about.
2. **Create new actors** (its children).
3. **Designate the behavior** for the next message it receives.

That's the entire model. There is no shared memory between actors, no synchronization primitives, no mutex. Concurrency emerges naturally: each actor processes one message at a time from its mailbox, and multiple actors run concurrently on the asyncio event loop.

A **message** is any value sent to an actor. In Casty, messages are frozen dataclasses — immutable by construction. Related messages are grouped using PEP 695 type aliases, which enables exhaustive pattern matching via `match` statements:

```python
from dataclasses import dataclass
from casty import ActorRef

@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | Withdraw | GetBalance
```

The type alias `AccountMsg` is a union of all messages the actor accepts. When handling messages with `match`, the type checker verifies that all variants are covered. If a new message type is added to the union but not handled, Pyright reports an error at development time.

---

**Next:** [Behaviors as Values](behaviors.md)
