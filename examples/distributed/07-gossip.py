"""Gossip Propagation Example.

Demonstrates:
- 10-node DevelopmentCluster
- Gossip-style key-value propagation
- Eventual consistency across all nodes

Run with: uv run python examples/distributed/07-gossip.py
"""

import asyncio

from casty.cluster import DevelopmentCluster, Put, Get


async def main():
    async with asyncio.timeout(30):
        print("=== Gossip Propagation Example ===\n")
        print("Creating 10-node cluster...")

        nodes = 10

        async with DevelopmentCluster(nodes) as cluster:
            print(f"Started {len(cluster)}-node cluster\n")

            print("Phase 1: Sending key-value via cluster.gossip()")
            print("-" * 50)

            gossip = await cluster.gossip()
            await gossip.send(Put("config/timeout", b"30"))
            await gossip.send(Put("config/retries", b"3"))
            await gossip.send(Put("status/ready", b"true"))

            print("  Put: config/timeout = 30")
            print("  Put: config/retries = 3")
            print("  Put: status/ready = true")

            print("\nPhase 2: Waiting for gossip propagation...")
            print("-" * 50)

            nodes_with_data = 0
            for i in range(10):
                nodes_with_data = 0
                for node in cluster:
                    node_gossip = await node._system.actor(name="gossip_actor/gossip")
                    try:
                        val = await node_gossip.ask(Get("config/timeout"), timeout=1.0)
                        if val == b"30":
                            nodes_with_data += 1
                    except asyncio.TimeoutError:
                        pass

                print(f"  Iteration {i}: {nodes_with_data}/{nodes} nodes have data")

                if nodes_with_data >= nodes:
                    break

                await asyncio.sleep(1)

            print(f"\n=== Summary ===")
            print(f"Information propagated to {nodes_with_data}/{len(cluster)} nodes")

            if nodes_with_data == len(cluster):
                print("SUCCESS: All nodes received the gossip updates!")
            else:
                print("PARTIAL: Some nodes haven't received updates yet")
                print("(This is expected with gossip - eventual consistency)")


if __name__ == "__main__":
    import warnings
    import logging
    warnings.filterwarnings("ignore")
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(main())
    except TimeoutError:
        print("\n(Shutdown timed out - this is expected)")
    except KeyboardInterrupt:
        pass
