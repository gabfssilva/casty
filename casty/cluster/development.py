from __future__ import annotations

import asyncio
import random
from enum import Enum
from typing import Any

from casty.actor import Behavior
from casty.ref import ActorRef
from .clustered_system import ClusteredActorSystem


class DistributionStrategy(Enum):
    RANDOM = "random"
    ROUND_ROBIN = "round-robin"
    CONSISTENT = "consistent"


class DevelopmentCluster:
    def __init__(
        self,
        nodes: int = 3,
        *,
        strategy: DistributionStrategy = DistributionStrategy.ROUND_ROBIN,
    ) -> None:
        self._node_count = nodes
        self._strategy = strategy
        self._systems: list[ClusteredActorSystem] = []
        self._round_robin_index = 0

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
    ) -> ActorRef[M]:
        node = self._next_node(name)
        return await node.actor(behavior, name=name)

    async def start(self) -> None:
        for i in range(self._node_count):
            node_id = f"node-{i}"
            system = ClusteredActorSystem(
                node_id=node_id,
                host="127.0.0.1",
                port=0,
            )
            await system.start()
            self._systems.append(system)

        for i, system in enumerate(self._systems):
            for j, other in enumerate(self._systems):
                if i != j:
                    await system.connect_to(other.address)

        await asyncio.sleep(0.1)

    async def shutdown(self) -> None:
        for system in reversed(self._systems):
            await system.shutdown()
        self._systems.clear()

    async def __aenter__(self) -> "DevelopmentCluster":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
