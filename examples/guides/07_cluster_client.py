"""Client + Cluster — ClusterClient connecting from outside the cluster.

The ClusterClient connects to a running cluster via TCP, receives topology
updates, and routes messages directly to the node owning each shard — no
cluster membership required.
"""

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import (
    ActorRef,
    Behavior,
    Behaviors,
    ClusterClient,
    ClusteredActorSystem,
    ShardEnvelope,
)


# ── Messages ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AddPoints:
    amount: int


@dataclass(frozen=True)
class GetPoints:
    reply_to: ActorRef[int]


type LoyaltyMsg = AddPoints | GetPoints


# ── Entity factory ───────────────────────────────────────────────────


def loyalty_entity(entity_id: str) -> Behavior[LoyaltyMsg]:
    def active(points: int = 0) -> Behavior[LoyaltyMsg]:
        async def receive(_ctx: Any, msg: LoyaltyMsg) -> Behavior[LoyaltyMsg]:
            match msg:
                case AddPoints(amount):
                    print(f"  [{entity_id}] +{amount} points (total: {points + amount})")
                    return active(points + amount)
                case GetPoints(reply_to):
                    reply_to.tell(points)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active()


# ── Main ─────────────────────────────────────────────────────────────


NUM_SHARDS = 10


async def main() -> None:
    # 1. Start a single-node cluster
    cluster = ClusteredActorSystem(
        name="loyalty-cluster",
        host="127.0.0.1",
        port=25530,
        node_id="node-1",
    )

    async with cluster:
        cluster.spawn(
            Behaviors.sharded(loyalty_entity, num_shards=NUM_SHARDS),
            "loyalty",
        )
        await cluster.wait_for(1)
        print("── Cluster running ──\n")

        # 2. Connect from outside with ClusterClient
        async with ClusterClient(
            contact_points=[("127.0.0.1", 25530)],
            system_name="loyalty-cluster",
        ) as client:
            await asyncio.sleep(1.0)

            # Get a ref that routes to the cluster's "loyalty" shard type
            loyalty = client.entity_ref("loyalty", num_shards=NUM_SHARDS)

            # tell() — fire and forget
            print("── Sending points ──")
            loyalty.tell(ShardEnvelope("user-1", AddPoints(100)))
            loyalty.tell(ShardEnvelope("user-1", AddPoints(50)))
            loyalty.tell(ShardEnvelope("user-2", AddPoints(200)))
            await asyncio.sleep(0.5)

            # ask() — request-reply from outside the cluster
            print("\n── Querying points ──")
            for user in ("user-1", "user-2"):
                points: int = await client.ask(
                    loyalty,
                    lambda r, uid=user: ShardEnvelope(
                        uid, GetPoints(reply_to=r)
                    ),
                    timeout=5.0,
                )
                print(f"  {user}: {points} points")


asyncio.run(main())
