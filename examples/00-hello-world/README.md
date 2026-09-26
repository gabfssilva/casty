# Hello world

Creates an actor with an initial state, asks it with `ask`, and saves its state with `ctx.state.set`. Each key keeps its own counter: two messages to `ana` increment the same counter, while `bia` starts at one.

```sh
cd examples/00-hello-world
uv run main.py
```
