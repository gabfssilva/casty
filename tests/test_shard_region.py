# tests/test_shard_region.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef, Behavior
from casty.cluster import ShardEnvelope
from casty.cluster.region import shard_region_actor
from casty.cluster.coordinator import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty.cluster.state import NodeAddress


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[str]


type EntityMsg = str | GetValue


def echo_entity(entity_id: str) -> Behavior[EntityMsg]:
    received: list[str] = []

    async def receive(ctx: Any, msg: EntityMsg) -> Any:
        match msg:
            case GetValue(reply_to):
                reply_to.tell(f"{entity_id}:{len(received)}")
                return Behaviors.same()
            case str() as text:
                received.append(text)
                return Behaviors.same()
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


async def test_shard_region_routes_to_entity() -> None:
    """ShardRegion creates entity and delivers messages."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
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
                entity_factory=echo_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        region_ref.tell(ShardEnvelope(entity_id="user-1", message="hello"))
        region_ref.tell(ShardEnvelope(entity_id="user-1", message="world"))
        await asyncio.sleep(0.3)

        result = await system.ask(
            region_ref,
            lambda r: ShardEnvelope(entity_id="user-1", message=GetValue(reply_to=r)),
            timeout=5.0,
        )
        assert result == "user-1:2"


async def test_shard_region_multiple_entities() -> None:
    """Different entity IDs create separate actors."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
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
                entity_factory=echo_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        region_ref.tell(ShardEnvelope(entity_id="a", message="msg1"))
        region_ref.tell(ShardEnvelope(entity_id="b", message="msg2"))
        region_ref.tell(ShardEnvelope(entity_id="b", message="msg3"))
        await asyncio.sleep(0.3)

        result_a = await system.ask(
            region_ref,
            lambda r: ShardEnvelope(entity_id="a", message=GetValue(reply_to=r)),
            timeout=5.0,
        )
        result_b = await system.ask(
            region_ref,
            lambda r: ShardEnvelope(entity_id="b", message=GetValue(reply_to=r)),
            timeout=5.0,
        )
        assert result_a == "a:1"
        assert result_b == "b:2"
