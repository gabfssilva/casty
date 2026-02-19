# Stateful Companion

In this guide you'll build a **circuit breaker** — an immutable companion object that an actor uses to protect against cascading failures. Along the way you'll learn how to extract business rules into plain frozen dataclasses whose methods work as a constant fold: every call returns the result plus the updated state.

## The Problem

Sometimes an actor's message handler grows complex — rate limiting, failure detection, metrics collection. You *can* keep everything inline, but if you'd rather extract that logic into a reusable, testable object, that object needs state. In a mutable world you'd reach for a class with internal dicts. In Casty, mutation is the enemy.

The **stateful companion** is one way to handle this: a frozen dataclass whose every method returns `tuple[result, new_self]`. The actor carries it as a behavior parameter and does behavior recursion as usual — the companion is just another piece of immutable state.

## The Companion

A circuit breaker has three states: **closed** (normal operation), **open** (all calls rejected), and **half-open** (one trial call allowed). The `CircuitBreaker` companion tracks failure counts and timestamps, all as immutable fields:

```python
--8<-- "examples/guides/09_stateful_companion.py:31:43"
```

The `allow` method decides whether a call can proceed. When the circuit is open but the reset timeout has elapsed, it transitions to half-open:

```python
--8<-- "examples/guides/09_stateful_companion.py:45:64"
```

After the call completes, the actor reports the outcome. `record_success` resets the breaker to closed. `record_failure` increments the counter and trips the circuit when it hits the threshold:

```python
--8<-- "examples/guides/09_stateful_companion.py:66:86"
```

Notice: every method returns `tuple[result, CircuitBreaker]`. Even `status`, which doesn't change state, follows the same signature — it returns `self` unchanged:

```python
--8<-- "examples/guides/09_stateful_companion.py:88:89"
```

This uniformity means the actor never has to wonder whether a method changed the companion. It always unpacks `result, new_companion` and moves on.

## Messages

The caller handles two messages: service calls and status queries:

```python
--8<-- "examples/guides/09_stateful_companion.py:92:109"
```

## Using It in an Actor

The `service_caller` actor is thin — it checks the breaker, makes the call, records the outcome, and transitions with the updated breaker:

```python
--8<-- "examples/guides/09_stateful_companion.py:112:139"
```

The circuit breaker state machine (closed/open/half-open, failure counting, timeout tracking) lives entirely in the companion. The actor just asks questions and reports results.

## Running It

The `main()` function trips the breaker with failures, observes it blocking calls, waits for the reset timeout, and watches it recover:

```python
--8<-- "examples/guides/09_stateful_companion.py:142:184"
```

Output:

```
── Sending 3 failures to trip the breaker ──
  Call 1: service error
  Call 2: service error
  Call 3: service error
  Circuit state: open

── Calling while circuit is open ──
  Call: circuit open

── Waiting for reset timeout (1s) ──

── Calling after timeout (half-open) ──
  Call: ok
  Circuit state: closed
```

Three failures trip the breaker open. The next call is rejected immediately — the companion said no. After the 1-second timeout the breaker moves to half-open, the trial call succeeds, and the circuit closes again. The actor never mutated anything.

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/09_stateful_companion.py
```

---

**What you learned:**

- **Stateful companions** are frozen dataclasses whose methods return `tuple[result, new_self]` — a constant fold over immutable state.
- **Companions can hold business rules** you choose to extract — keeping the actor focused on message routing and state transitions.
- **The fold signature is uniform** — every method returns the pair, even read-only ones. No guessing about side effects.
- **Companions compose with behavior recursion** naturally — they're just another parameter in the closure.
