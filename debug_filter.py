"""Test via DevelopmentCluster exactly like example 05."""
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.cluster import DevelopmentCluster


@dataclass
class Inc:
    pass


@dataclass
class Get:
    pass


CounterMsg = Inc | Get


@actor(clustered=True)
async def counter(*, state: int = 0, mailbox: Mailbox[CounterMsg]):
    print("[counter] starting mailbox loop")
    async for msg, ctx in mailbox:
        print(f"[counter] got {msg}")
        match msg:
            case Inc():
                state += 1
            case Get():
                await ctx.reply(state)


async def main():
    print("Starting DevelopmentCluster...")
    async with asyncio.timeout(10):
        async with DevelopmentCluster(nodes=2) as cluster:
            print(f"Cluster ready: {len(cluster.nodes)} nodes")
            node = cluster.nodes[0]
            print(f"Node: {node.node_id}")

            print("Creating actor with clustered=True...")
            ref = await node.actor(counter(), name="test")
            print(f"Actor created: {ref}")

            print("Sending Inc...")
            await ref.send(Inc())
            print("Asking Get...")
            result = await ref.ask(Get())
            print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
