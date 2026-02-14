# Shopping Cart

In this guide you'll build a **shopping cart** that expires if abandoned. Along the way you'll learn how behavior recursion models state, how terminal behaviors protect invariants, and how the scheduler handles timeouts.

## Messages

The cart uses frozen dataclasses for all messages, plus a value object for items and a result type for receipts:

```python
--8<-- "examples/guides/02_shopping_cart.py:33:70"
```

`Item` is a value object. `Receipt` is the checkout result returned to the caller. The rest are commands the cart understands. Notice `CartExpired` carries no data — it's a signal sent by the scheduler when the timeout fires. The cart never creates it directly.

## The Empty Cart

An empty cart only accepts `AddItem`. Everything else is ignored:

```python
--8<-- "examples/guides/02_shopping_cart.py:75:96"
```

When the first item arrives, two things happen: we schedule a 5-second timeout via `ScheduleOnce`, and we transition to `active_cart` with the item in our dict. The `SchedulerRef` type alias keeps the signatures readable.

## The Active Cart

This is where the interesting logic lives. Each match arm handles one command:

```python
--8<-- "examples/guides/02_shopping_cart.py:99:136"
```

Walking through each arm:

- **AddItem** — behavior recursion with a new dict (`{**items, item.name: item}`). This *is* the state transition. No mutation, no `self.items[name] = item`.
- **RemoveItem** — filters out the item. If the cart becomes empty, cancels the timer and transitions back to `empty_cart`.
- **GetTotal** — request-reply: computes the sum, sends it to `reply_to`, stays in the same behavior.
- **Checkout** — cancels the timer, builds a `Receipt`, replies, and transitions to `checked_out`. The cart is done.
- **CartExpired** — the timer fired. Transitions to `expired`. No cleanup needed — the behavior change *is* the cleanup.

## Terminal States

Once a cart is checked out or expired, it ignores all messages:

```python
--8<-- "examples/guides/02_shopping_cart.py:139:154"
```

Both behaviors do the same thing: return `Behaviors.same()` for every message. There is no flag to accidentally forget to check. A checked-out cart can't accept items because the behavior simply doesn't handle them. The behavior *is* the state.

## Wiring It Up

The `shopping_cart` function ties everything together with `Behaviors.setup()`:

```python
--8<-- "examples/guides/02_shopping_cart.py:160:165"
```

`Behaviors.setup()` gives us an `ActorContext` before the first message arrives. We use it to spawn a scheduler as a **child actor** and pass its ref to `empty_cart`. Because the scheduler is a child, it shares the cart's lifecycle — when the cart stops, the scheduler stops with it.

## Running It

The `main()` function spawns two carts: one that checks out successfully, and one that gets abandoned:

```python
--8<-- "examples/guides/02_shopping_cart.py:171:196"
```

Output:

```
Total: $100.00
Receipt: 2 items, $100.00
Waiting for cart-2 to expire...
Cart expired — items discarded
```

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/02_shopping_cart.py
```

---

**What you learned:**

- **Behavior recursion** passes new state as function arguments — no mutation needed.
- **Terminal behaviors** (`checked_out`, `expired`) protect invariants by construction — a checked-out cart can't accept items because the behavior simply doesn't handle them.
- **The scheduler** sends timed messages without blocking the actor's mailbox.
- **Child actors** (the scheduler) share their parent's lifecycle — when the cart stops, the scheduler stops.
