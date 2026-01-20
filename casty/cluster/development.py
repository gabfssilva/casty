from __future__ import annotations

import asyncio
import random
from enum import Enum
from typing import Any

from casty.actor import Behavior
from casty.ref import ActorRef
from casty.mailbox import Filter, MessageStream
from casty.state import State
from .replication import Routing
from .clustered_system import ClusteredActorSystem


def debug_filter[M](node_id: str) -> Filter[M]:
    def filter_fn(state: State[Any] | None, stream: MessageStream[M]) -> MessageStream[M]:
        async def filtered() -> MessageStream[M]:
            async for msg, ctx in stream:
                print(f"[{node_id}] {ctx.self_id} <- {type(msg).__name__}: {msg}")
                yield msg, ctx
        return filtered()
    return filter_fn


class DistributionStrategy(Enum):
    RANDOM = "random"
    ROUND_ROBIN = "round-robin"
    CONSISTENT = "consistent"


class DevelopmentCluster:
    def __init__(
        self,
        nodes: int = 3,
        *,
        strategy: DistributionStrategy = DistributionStrategy.RANDOM,
        debug: bool = False,
    ) -> None:
        self._node_count = nodes
        self._strategy = strategy
        self._systems: list[ClusteredActorSystem] = []
        self._round_robin_index = 0
        self._debug = debug

    def __getitem__(self, index: int) -> ClusteredActorSystem:
        return self._systems[index]

    def __len__(self) -> int:
        return len(self._systems)

    def __iter__(self):
        return iter(self._systems)

    @property
    def nodes(self) -> list[ClusteredActorSystem]:
        return list(self._systems)

    def _next_node(self, name: str | None = None) -> ClusteredActorSystem:
        match self._strategy:
            case DistributionStrategy.RANDOM:
                return random.choice(self._systems)
            case DistributionStrategy.ROUND_ROBIN:
                node = self._systems[self._round_robin_index]
                self._round_robin_index = (self._round_robin_index + 1) % len(self._systems)
                return node
            case DistributionStrategy.CONSISTENT:
                index = hash(name) % len(self._systems)
                return self._systems[index]

    async def actor[M](
        self,
        behavior: Behavior,
        *,
        name: str,
        replicas: int | None = None,
        write_quorum: int | None = None,
        routing: Routing | None = None,
    ) -> ActorRef[M]:
        node = self._next_node(name)
        return await node.actor(
            behavior,
            name=name,
            replicas=replicas,
            write_quorum=write_quorum,
            routing=routing,
        )

    async def start(self) -> None:
        first_system = ClusteredActorSystem(
            node_id="node-0",
            host="127.0.0.1",
            port=0,
            debug_filter=debug_filter("node-0") if self._debug else None,
        )
        await first_system.start()
        self._systems.append(first_system)

        for i in range(1, self._node_count):
            node_id = f"node-{i}"
            system = ClusteredActorSystem(
                node_id=node_id,
                host="127.0.0.1",
                port=0,
                seeds=[first_system.address],
                debug_filter=debug_filter(node_id) if self._debug else None,
            )
            await system.start()
            self._systems.append(system)

        await asyncio.sleep(0.3)

        from .messages import GetAliveMembers, Join
        seed_members = await first_system._membership_ref.ask(GetAliveMembers())
        for system in self._systems[1:]:
            for member_id, info in seed_members.items():
                if member_id != system.node_id:
                    await system._membership_ref.send(Join(node_id=member_id, address=info.address))

        await asyncio.sleep(0.2)

    async def shutdown(self) -> None:
        for system in reversed(self._systems):
            await system.shutdown()
        self._systems.clear()

    async def __aenter__(self) -> "DevelopmentCluster":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
