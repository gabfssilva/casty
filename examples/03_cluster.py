"""A real cluster: three nodes, keys spread over the ring, one-hop routing.

Each actor key is owned by exactly one node (token ring with vnodes); calls
from any member route straight to the owner. A lite member (`casty.connect`)
knows the ring and routes the same way, but hosts nothing.

All three nodes run in this one process to keep the example self-contained —
in production each `casty.start` is its own process/machine.

Run: uv run python examples/03_cluster.py
"""

import asyncio
from collections import Counter as Tally

import casty


@casty.actor
class PageCounter:
    hits: int = 0

    async def hit(self) -> int:
        self.hits += 1
        return self.hits


async def main() -> None:
    node_a = await casty.start("127.0.0.1:7111")
    node_b = await casty.start("127.0.0.1:7112", seeds=["127.0.0.1:7111"])
    node_c = await casty.start("127.0.0.1:7113", seeds=["127.0.0.1:7111"])
    nodes = [node_a, node_b, node_c]
    while any(len(n.membership.alive_members()) < 3 for n in nodes):
        await asyncio.sleep(0.05)
    print(f"cluster up: {len(node_a.membership.alive_members())} members")

    # 30 pages, called through whichever node is handy — placement is the same
    # from everywhere, so each page's count lives in exactly one activation
    for i in range(30):
        await nodes[i % 3].actor(PageCounter, f"/page/{i}").hit()
    for i in range(30):
        await nodes[(i + 1) % 3].actor(PageCounter, f"/page/{i}").hit()

    ring = node_a.ring
    assert ring is not None
    placement = Tally(ring.owner(f"{PageCounter.__module__}.PageCounter/{key}")
                      for key in (f"/page/{i}" for i in range(30)))
    spread = ", ".join(f"{count} pages" for count in placement.values())
    print(f"placement over 3 nodes: {spread}")

    # every page was hit twice, no matter which node dispatched the call
    counts = [await node_b.actor(PageCounter, f"/page/{i}").hit() for i in range(30)]
    assert all(count == 3 for count in counts)
    print("all 30 pages agree: 3 hits each")

    # a lite member routes in one hop but never hosts actors
    client = await casty.connect(["127.0.0.1:7111"])
    print(f"via lite member: /page/0 -> {await client.actor(PageCounter, '/page/0').hit()} hits")
    await client.close()

    for node in nodes:
        await node.close()


if __name__ == "__main__":
    asyncio.run(main())
