# One agent per node

Keeps an agent on each node of a three-node cluster. The type is declared with `pinned=True`, so each reference names the node with `at=` instead of letting the hash ring choose: each node reaches its own agent by its `NodeId`, the first node reaches all of them by the `Member`s of its table, and also by the `host:port` address each node announces. The program adds a fourth node and shows that no agent moves; gracefully stops the second node and shows that its agent becomes unavailable, with no other node taking it over; and starts a new process at the same address, where the same reference answers again and the agent starts over from the `initial` state.

Needs local TCP ports `7441` to `7444` to be free.

```sh
cd examples/10-agent-per-node
uv run main.py
```
