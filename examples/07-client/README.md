# External client

Demonstrates a `Client` that reaches the actors of the cluster without hosting actors or storing their state. Two processes run the nodes, while a third sends votes for four options and asks for the count and the node responsible for each; the shared types are in `app.py`, and repeated votes from the same voter for the same option count only once.

Needs local TCP ports `7421` and `7422` to be free. Run each block in a separate terminal.

Terminal 1:

```sh
cd examples/07-client
uv run node.py 7421
```

Terminal 2:

```sh
cd examples/07-client
uv run node.py 7422
```

Once the nodes print `is up`, run in terminal 3:

```sh
cd examples/07-client
uv run client.py
```

Stop the nodes with `Ctrl+C` in their terminals.
