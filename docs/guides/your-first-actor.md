# Your First Actor

In this guide you'll build a **traffic light** — a state machine that cycles through green, yellow, and red. Along the way you'll learn the three core ideas in Casty: messages, behaviors, and state transitions.

## Messages

Every actor communicates through messages. In Casty, messages are frozen dataclasses — immutable values that are safe to send between actors.

Our traffic light handles two messages:

```python
--8<-- "examples/guides/01_your_first_actor.py:9:19"
```

`Tick` advances the light to the next color. `GetColor` is a request-reply message: the sender includes its own `ActorRef` so the light can respond.

## Behaviors as State

Instead of a single handler with an `if color == "green"` check, each color is its own behavior function. The traffic light *is* whichever behavior is currently active:

```python
--8<-- "examples/guides/01_your_first_actor.py:22:64"
```

Notice: there are no mutable variables. No `self.color = "yellow"`. When the light receives a `Tick`, it **returns a different behavior** — and that *is* the state transition. This is called **behavior recursion**.

## Running It

The `main()` function spawns the actor and sends messages:

```python
--8<-- "examples/guides/01_your_first_actor.py:67:83"
```

Output:

```
🟢 → 🟡
🟡 → 🔴
🔴 → 🟢
Current color: green
```

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/01_your_first_actor.py
```

---

**What you learned:**

- **Messages** are frozen dataclasses — immutable and type-safe.
- **Behaviors** are values, not classes. Each state is a function that returns a `Behavior`.
- **State transitions** happen by returning a different behavior — no mutation needed.
- **Request-reply** works by including an `ActorRef` in the message.
