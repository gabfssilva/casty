# Replication

Starts three nodes in the same process and keeps a journal with three replicas and majority writes. After saving three entries, the program abruptly stops the node running the actor, reads the entries on a survivor and appends another, showing that the confirmed state is recovered after the loss of a node.

Needs local TCP ports `7401` to `7403` to be free.

```sh
cd examples/05-replication
uv run main.py
```
