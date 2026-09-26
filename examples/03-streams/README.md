# Streams

Demonstrates `ctx.merge` combining messages from the mailbox with events from an async generator in the same loop of the actor. A meter receives readings, closes a window every half second and saves its average; at the end, the program prints the averages of the windows that received data.

```sh
cd examples/03-streams
uv run main.py
```
