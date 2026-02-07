# tests/test_sharding_integration.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef, Behavior
from casty.sharding import ShardEnvelope
from casty.shard_coordinator_actor import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty.shard_region_actor import shard_region_actor
from casty.cluster_state import NodeAddress


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountMsg = Deposit | GetBalance


def account_entity(entity_id: str) -> Behavior[AccountMsg]:
    def active(balance: int) -> Behavior[AccountMsg]:
        async def receive(ctx: Any, msg: AccountMsg) -> Any:
            match msg:
                case Deposit(amount):
                    return active(balance + amount)
                case GetBalance(reply_to):
                    reply_to.tell(balance)
                    return Behaviors.same()
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)


async def test_sharding_full_workflow() -> None:
    """Full local sharding: create cluster, init sharding, route messages."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="bank") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=account_entity,
                num_shards=50,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        # Deposit to multiple accounts
        for i in range(5):
            region_ref.tell(ShardEnvelope(f"account-{i}", Deposit(100 * (i + 1))))

        # Deposit more to account-0
        region_ref.tell(ShardEnvelope("account-0", Deposit(50)))
        await asyncio.sleep(0.3)

        # Check balances
        balance_0 = await system.ask(
            region_ref,
            lambda r: ShardEnvelope("account-0", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance_0 == 150  # 100 + 50

        balance_4 = await system.ask(
            region_ref,
            lambda r: ShardEnvelope("account-4", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance_4 == 500
