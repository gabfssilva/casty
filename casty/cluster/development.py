from __future__ import annotations

import asyncio
import random
from enum import Enum
from typing import Any, Awaitable

from casty.actor import Behavior
from casty.ref import ActorRef
from casty.mailbox import Filter, MessageStream
from casty.state import State
from .clustered_system import ClusteredActorSystem


def debug_filter[M](node_id: str) -> Filter[M]:
    def filter_fn(state: State[Any] | None, stream: MessageStream[M]) -> MessageStream[M]:
        async def filtered() -> MessageStream[M]:
            async for msg, ctx in stream:
                print(f"[{node_id}] {ctx.self_id} <- {type(msg).__name__}: {msg}", flush=True)
                yield msg, ctx
        return filtered()
    return filter_fn


class DistributionStrategy(Enum):
    RANDOM = "random"
    ROUND_ROBIN = "round-robin"
    CONSISTENT = "consistent"

class ClusteredDevelopmentActorRef[M](ActorRef[M]):
    def __init__(
        self,
        cluster: DevelopmentCluster,
        behavior: Behavior,
        *,
        name: str,
    ) -> None:
        self._cluster = cluster
        self.actor_id = name
        self._behavior = behavior
        self._cached_ref: ActorRef[M] | None = None

    async def _get_ref(self) -> ActorRef[M]:
        if self._cached_ref is None:
            node = self._cluster._next_node(name=self.actor_id)
            self._cached_ref = await node.actor(behavior=self._behavior, name=self.actor_id)
            if self._cached_ref is None:
                raise RuntimeError(f"Failed to get actor ref for {self.actor_id}")
        return self._cached_ref

    async def send(self, msg: M, *, sender: ActorRef[Any] | None = None) -> None:
        ref = await self._get_ref()
        await ref.send(msg=msg, sender=sender)

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        ref = await self._get_ref()
        await ref.send_envelope(envelope)

    async def ask(self, msg: M, timeout: float | None = None) -> Any:
        ref = await self._get_ref()
        return await ref.ask(msg=msg, timeout=timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)

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
    ) -> ActorRef[M] | None:
        return ClusteredDevelopmentActorRef(cluster=self,behavior=behavior,name=name)

    async def start(self) -> None:
        print("Starting development cluster...", flush=True)
        first_system = ClusteredActorSystem(
            node_id="node-0",
            host="127.0.0.1",
            port=0,
            debug_filter=debug_filter("node-0") if self._debug else None,
        )
        print("Initializing node #1...", flush=True)
        await first_system.start()
        print("Node #1 is ready!", flush=True)
        self._systems.append(first_system)

        first_address = await first_system.address()

        for i in range(1, self._node_count):
            print(f"Initializing node #{i}...", flush=True)

            node_id = f"node-{i}"
            system = ClusteredActorSystem(
                node_id=node_id,
                host="127.0.0.1",
                port=0,
                seeds=[("node-0", first_address)],
                debug_filter=debug_filter(node_id) if self._debug else None,
            )
            await system.start()

            print(f"Node #{i} is ready!", flush=True)

            self._systems.append(system)

        print("Waiting for node discovery...", flush=True)
        await self.wait_for(self._node_count)

    async def shutdown(self) -> None:
        for system in reversed(self._systems):
            await system.shutdown()
        self._systems.clear()

    async def __aenter__(self) -> "DevelopmentCluster":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()

    async def wait_for(self, nodes: int):
        from casty.cluster import WaitFor
        cluster = await self._next_node().actor(name="cluster")
        await cluster.ask(WaitFor(nodes=nodes))

    async def gossip(self) -> ActorRef:
        node = self._next_node()
        return await node._system.actor(name="gossip")