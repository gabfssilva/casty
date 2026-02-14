# Pipe to Self

In this guide you'll build a **weather station** that fetches forecasts from an external API without blocking its mailbox. Along the way you'll learn how `pipe_to_self` turns async operations into messages, how the actor stays responsive during slow I/O, and how to handle both success and failure.

## The Problem

Actors process one message at a time. If your handler awaits a slow network call directly, the mailbox blocks — no other messages get through until the I/O completes. `pipe_to_self` solves this by running the coroutine in the background and delivering the result as a regular message.

## External API

A simulated weather service with a 500ms delay:

```python
--8<-- "examples/guides/04_pipe_to_self.py:12:22"
```

## Messages

The actor needs messages for requests, results, errors, and a ping to prove responsiveness:

```python
--8<-- "examples/guides/04_pipe_to_self.py:28:56"
```

Notice `WeatherResult` and `WeatherFailed` — these are the messages the actor sends *to itself* when the background fetch completes or fails. The caller never sees them.

## The Behavior

```python
--8<-- "examples/guides/04_pipe_to_self.py:62:99"
```

Walking through each arm:

- **FetchWeather** — calls `ctx.pipe_to_self()` with three arguments: the coroutine, a mapper that converts the raw API response into a `WeatherResult` message, and an `on_failure` handler that wraps exceptions into `WeatherFailed`. Returns `Behaviors.same()` immediately — the mailbox is free.
- **WeatherResult** — the piped result arrives as a normal message. The actor appends it to state via behavior recursion.
- **WeatherFailed** — the error handler fired. Log it and move on.
- **Ping** — proves the mailbox is responsive even while fetches are in-flight.

## Running It

```python
--8<-- "examples/guides/04_pipe_to_self.py:105:135"
```

Output:

```
── Requesting forecasts (non-blocking) ──
  Ping reply: pong (mailbox is responsive!)
  Weather in London: 12.5C, cloudy
  Weather in Tokyo: 28.0C, sunny
  Failed to fetch narnia: Unknown city: narnia

── Collected 2 forecasts ──
  London: 12.5C, cloudy
  Tokyo: 28.0C, sunny
```

Notice: the `Ping` reply arrives *before* any weather results. The three fetches are running concurrently in the background, but the mailbox keeps processing other messages.

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/04_pipe_to_self.py
```

---

**What you learned:**

- **`pipe_to_self`** runs a coroutine in the background and delivers the result as a message — no mailbox blocking.
- **Mappers** convert raw results into typed messages: `lambda data: WeatherResult(...)`.
- **`on_failure`** converts exceptions into messages so the actor can handle errors as part of its normal message loop.
- **Multiple `pipe_to_self` calls** run concurrently — the actor stays responsive to other messages.
