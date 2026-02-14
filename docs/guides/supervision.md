# Supervision

In this guide you'll build **supervised workers** that recover from failures automatically. Along the way you'll learn how supervision strategies decide what happens on crash, how lifecycle hooks observe the recovery process, and how supervision trees protect entire hierarchies of actors.

## Messages

A worker processes tasks. Some tasks are "bad" and cause it to crash:

```python
--8<-- "examples/guides/03_supervision.py:20:35"
```

`Process` carries the task payload. `CrashOnPurpose` forces a `RuntimeError` — we'll use it to test how different strategies respond to different exception types.

## The Worker

The worker raises on bad input and accumulates results via behavior recursion:

```python
--8<-- "examples/guides/03_supervision.py:41:57"
```

No try/except anywhere — the worker doesn't know it's supervised. Supervision is a concern of the *parent*, not the actor itself.

## Supervision Strategies

A `OneForOneStrategy` decides what to do when a child fails. The simplest form restarts on every error:

```python
--8<-- "examples/guides/03_supervision.py:63:68"
```

`max_restarts=3` within `60.0` seconds means: after 3 crashes in a sliding window, stop trying and let the actor die. This prevents infinite restart loops.

For more control, pass a `decider` that inspects the exception:

```python
--8<-- "examples/guides/03_supervision.py:71:83"
```

`ValueError` → restart (transient). `RuntimeError` → stop (fatal). Anything else → escalate to the parent.

## Lifecycle Hooks

`Behaviors.with_lifecycle()` wraps a behavior with hooks that fire at key moments:

```python
--8<-- "examples/guides/03_supervision.py:89:104"
```

Notice the composition: `worker()` → `Behaviors.supervise()` → `Behaviors.with_lifecycle()`. Each layer adds a concern without modifying the inner behavior. `pre_restart` receives the exception that caused the failure — useful for logging or cleanup.

## Supervision Trees

A supervisor spawns children and wraps each with a strategy. If a child crashes, only that child is affected:

```python
--8<-- "examples/guides/03_supervision.py:110:153"
```

`team_supervisor` uses `Behaviors.setup()` to spawn workers as children in `pre_start`. Each child is independently supervised with `always_restart()`. The supervisor delegates work by index — when `worker-2` crashes on a bad task, it restarts silently and handles the next task.

## Running It

```python
--8<-- "examples/guides/03_supervision.py:159:201"
```

Output:

```
── Basic supervision ──
  [/basic-worker] Processed: hello
  [/basic-worker] Processed: world

── Selective strategy + lifecycle hooks ──
  [/selective-worker] Started
  [/selective-worker] Processed: good
  ValueError — restarting: Cannot process: bad
  [/selective-worker] Restarting after: Cannot process: bad
  [/selective-worker] Started
  RuntimeError — stopping: Intentional crash!
  [/selective-worker] Stopped

── Supervision tree ──
  [/team/worker-0] Processed: task-a
  [/team/worker-1] Processed: task-b
  [/team/worker-2] Processed: task-c
  Team has 3 workers
```

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/03_supervision.py
```

---

**What you learned:**

- **`OneForOneStrategy`** decides per-child recovery: restart, stop, or escalate.
- **Custom deciders** inspect the exception to choose different directives for different failure modes.
- **Lifecycle hooks** (`pre_start`, `post_stop`, `pre_restart`) observe the actor lifecycle without modifying behavior.
- **Supervision trees** protect hierarchies — a crashing child is restarted without affecting siblings or the parent.
