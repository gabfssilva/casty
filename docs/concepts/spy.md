# Spy

Actors are opaque by design — you send messages in and observe effects out, but you can't see what an actor received, in what order, or when. This makes it difficult to trace message flows, verify that routing is correct, or understand why an actor reached a particular state.

`Behaviors.spy()` wraps any behavior with a transparent observer. The spy is the **parent** of the target actor: it spawns the real actor as a child, forwards every message, and emits a `SpyEvent` to an observer for each message received — including a `Terminated` signal when the target stops.

```python
import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, SpyEvent, Terminated


@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | GetBalance


def account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return account(balance + amount)
            case GetBalance(reply_to=reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)


def log_observer() -> Behavior[SpyEvent[AccountMsg]]:
    async def receive(
        ctx: ActorContext[SpyEvent[AccountMsg]], event: SpyEvent[AccountMsg]
    ) -> Behavior[SpyEvent[AccountMsg]]:
        match event.event:
            case Terminated():
                print(f"[spy] {event.actor_path} terminated at t={event.timestamp:.4f}")
            case msg:
                print(f"[spy] {event.actor_path} received {msg} at t={event.timestamp:.4f}")
        return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem("bank") as system:
        observer = system.spawn(log_observer(), "observer")

        ref = system.spawn(
            Behaviors.spy(account(), observer),
            "account",
        )

        ref.tell(Deposit(100))
        ref.tell(Deposit(50))
        balance = await system.ask(ref, lambda r: GetBalance(reply_to=r), timeout=2.0)
        print(f"Balance: {balance}")

        await asyncio.sleep(0.1)


asyncio.run(main())
```

Output:

```
[spy] /account received Deposit(amount=100) at t=12345.0001
[spy] /account received Deposit(amount=50) at t=12345.0002
[spy] /account received GetBalance(reply_to=...) at t=12345.0003
Balance: 150
```

The spy is completely transparent: the target actor processes messages normally, replies reach their destination, and supervision works as expected. Because the spy is a pure userland wrapper — it spawns the target as a child and forwards messages — no internal runtime changes are needed.

`SpyEvent` contains three fields:

| Field | Type | Description |
|-------|------|-------------|
| `actor_path` | `str` | Path of the spied actor (e.g. `"/account"`) |
| `event` | `M \| Terminated` | The message received, or `Terminated` when the actor stops |
| `timestamp` | `float` | Monotonic timestamp via `time.monotonic()` |

The spy composes freely with other behavior wrappers. Spy a supervised actor to observe crash-restart cycles. Spy a lifecycle-wrapped actor to see the messages that arrive between start and stop:

```python
strategy = OneForOneStrategy(max_restarts=3, within=60.0)

ref = system.spawn(
    Behaviors.spy(
        Behaviors.supervise(my_behavior(), strategy),
        observer,
    ),
    "resilient",
)
```

The observer itself is an ordinary actor — you can write it to log, collect metrics, filter events, or forward to an external system.

---

**Next:** [Event Sourcing](../persistence/event-sourcing.md)
