"""Clustered — multi-node setup with sharded entities and quorum.

Starts two nodes in the same process to demonstrate cluster formation,
sharding, and cross-node routing. In production, each node runs in its
own process or container.
"""

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import (
    ActorRef,
    Behavior,
    Behaviors,
    ShardEnvelope,
    ClusteredActorSystem,
)


# ── Messages ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetValue


# ── Entity factory (one instance per entity_id) ─────────────────────


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    def active(value: int = 0) -> Behavior[CounterMsg]:
        async def receive(_ctx: Any, msg: CounterMsg) -> Behavior[CounterMsg]:
            match msg:
                case Increment(amount):
                    print(f"  [{entity_id}] {value} + {amount} = {value + amount}")
                    return active(value + amount)
                case GetValue(reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active()


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    # Start two cluster nodes
    node1 = ClusteredActorSystem(
        name="my-cluster",
        host="127.0.0.1",
        port=25520,
        node_id="node-1",
    )
    node2 = ClusteredActorSystem(
        name="my-cluster",
        host="127.0.0.1",
        port=25521,
        node_id="node-2",
        seed_nodes=[("127.0.0.1", 25520)],
    )

    async with node1, node2:
        # Wait for both nodes to be UP
        await node1.wait_for(2)
        await node2.wait_for(2)
        print("── Cluster formed (2 nodes) ──\n")

        # Spawn sharded entities on both nodes
        proxy1: ActorRef[ShardEnvelope[CounterMsg]] = node1.spawn(
            Behaviors.sharded(counter_entity, num_shards=10), "counters"
        )
        proxy2: ActorRef[ShardEnvelope[CounterMsg]] = node2.spawn(
            Behaviors.sharded(counter_entity, num_shards=10), "counters"
        )
        await asyncio.sleep(1.0)

        # Route messages by entity_id — shards are distributed across nodes
        print("── Sending increments ──")
        proxy1.tell(ShardEnvelope("alice", Increment(10)))
        proxy1.tell(ShardEnvelope("alice", Increment(5)))
        proxy1.tell(ShardEnvelope("bob", Increment(100)))
        proxy2.tell(ShardEnvelope("carol", Increment(42)))
        await asyncio.sleep(0.5)

        # Query from either node — routing is transparent
        print("\n── Querying balances ──")
        for name in ("alice", "bob", "carol"):
            value: int = await node1.ask(
                proxy1,
                lambda r, eid=name: ShardEnvelope(eid, GetValue(reply_to=r)),
                timeout=5.0,
            )
            print(f"  {name}: {value}")


asyncio.run(main())
