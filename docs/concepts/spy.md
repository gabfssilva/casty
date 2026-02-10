# Spy

Actors are opaque by design — you send messages in and observe effects out, but you can't see what an actor received, in what order, or when. This makes it difficult to trace message flows, verify that routing is correct, or understand why an actor reached a particular state.

`Behaviors.spy()` wraps any behavior with a transparent observer. The spy is a **cell-level wrapper** — like `SupervisedBehavior` or `LifecycleBehavior` — so the cell itself emits a `SpyEvent` after processing each message. This means **all** messages are captured, including self-tells (`ctx.self.tell(...)`).

```python
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
        print(f"[spy] {event.actor_path} received {event.event} at t={event.timestamp:.4f}")
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

The spy is completely transparent: the target actor processes messages normally, replies reach their destination, and supervision works as expected. Because the spy is a cell-level behavior wrapper, it captures every message the actor actually processes — including self-tells that a forwarding proxy would miss.

`SpyEvent` contains three fields:

| Field | Type | Description |
|-------|------|-------------|
| `actor_path` | `str` | Path of the spied actor (e.g. `"/account"`) |
| `event` | `M \| Terminated` | The message received by the actor |
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

## Spying on children

By default, only the wrapped actor is observed. Pass `spy_children=True` to automatically spy on every child the actor spawns — recursively. All children (and their children) report to the same observer:

```python
ref = system.spawn(
    Behaviors.spy(parent_behavior(), observer, spy_children=True),
    "parent",
)
```

When the parent spawns a child via `ctx.spawn(...)`, the child is transparently wrapped with the same spy observer and `spy_children=True`, so the propagation continues down the entire subtree. The `actor_path` field in each `SpyEvent` distinguishes which actor produced the event (e.g. `"/parent"` vs `"/parent/worker-1"`).

---

**Next:** [Event Sourcing](../persistence/event-sourcing.md)
