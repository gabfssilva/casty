# Getting Started

## Installation

```bash
pip install casty
```

Casty requires **Python 3.12+** and has zero external dependencies — stdlib only.

## Your First Actor

```python
@dataclass(frozen=True)
class Greet:
    name: str

def greeter() -> Behavior[Greet]:
    async def receive(ctx: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
        print(f"Hello, {msg.name}!")
        return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(greeter(), "greeter")
        ref.tell(Greet("Alice"))
        ref.tell(Greet("Bob"))
        await asyncio.sleep(0.1)

asyncio.run(main())
# Hello, Alice!
# Hello, Bob!
```

Four things happened here:

1. **`Greet` is a frozen dataclass.** Messages are immutable values — safe to send between actors without defensive copying.
2. **`greeter()` returns a `Behavior[Greet]`.** The behavior is a value produced by `Behaviors.receive()`, not a class instance. The actor system interprets this value to wire up the message handler.
3. **`Behaviors.same()` means "keep the current behavior."** Returning a different behavior would transition the actor to a new state. This is the fundamental mechanism for state management.
4. **`tell()` is fire-and-forget.** Messages are enqueued in the actor's mailbox and processed asynchronously, one at a time.

## What's Next?

The sections that follow build on these foundations progressively — from functional state management through fault tolerance, event sourcing, and distributed clustering. Each section introduces one concept, explains why it exists in the actor model, and shows how Casty implements it.

Start with **[Actors and Messages](../concepts/actors-and-messages.md)** to understand the core building blocks.
