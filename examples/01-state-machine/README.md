# State machine

Models an order in the pending, paid and shipped states, each with its own actor and state type. `ctx.become` changes the behavior that receives the next messages without changing the reference to the order; the program prints the accepted transitions and the refused operations in each state.

```sh
cd examples/01-state-machine
uv run main.py
```
