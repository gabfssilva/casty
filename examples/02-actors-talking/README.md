# Actors talking to each other

Demonstrates an actor that coordinates transfers by asking two account actors through `ctx.system.ref` and `ask`. The program deposits an initial balance, attempts two transfers and prints the final balances; the withdrawal and the deposit are separate operations, with no atomic transaction between the accounts.

```sh
cd examples/02-actors-talking
uv run main.py
```
