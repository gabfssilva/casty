from __future__ import annotations

import random
from enum import Enum
from typing import Any, TYPE_CHECKING

from .clustered_system import ClusteredSystem
from .config import ClusterConfig
from .scope import Scope

if TYPE_CHECKING:
    from casty import Actor
    from casty.protocols import ActorRef
    from casty.supervision import SupervisorConfig


def _next_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class DistributionStrategy(Enum):
    RANDOM = "random"
    ROUND_ROBIN = "round-robin"


class DevelopmentCluster:
    """Multi-node cluster for testing that implements System protocol.

    All System operations are delegated to nodes based on the distribution strategy.

    Examples:
        # Basic usage - operations on random nodes
        async with DevelopmentCluster(3) as cluster:
            ref = await cluster.actor(Counter, name="counter", scope="cluster")
            await ref.send(Increment(10))

        # Specific node operations
        async with DevelopmentCluster(3) as cluster:
            ref = await cluster.node(0).actor(Counter, name="counter", scope="cluster")
            await cluster.node(1).actor(Counter, name="counter", scope="cluster")
    """

    def __init__(
        self,
        nodes: int = 3,
        *,
        node_id_prefix: str = "node",
        strategy: DistributionStrategy = DistributionStrategy.RANDOM,
    ):
        self._node_count = nodes
        self._prefix = node_id_prefix
        self._strategy = strategy
        self._nodes: list[ClusteredSystem] = []
        self._round_robin_index = 0
        self._head_port: int | None = None

    def _next_node(self) -> ClusteredSystem:
        match self._strategy:
            case DistributionStrategy.RANDOM:
                return random.choice(self._nodes)
            case DistributionStrategy.ROUND_ROBIN:
                node = self._nodes[self._round_robin_index]
                self._round_robin_index = (self._round_robin_index + 1) % len(self._nodes)
                return node

    async def actor[M](
        self,
        actor_cls: type["Actor[M]"],
        *,
        name: str,
        scope: Scope = 'local',
        supervision: "SupervisorConfig | None" = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> "ActorRef[M]":
        return await self._next_node().actor(
            actor_cls,
            name=name,
            scope=scope,
            supervision=supervision,
            durable=durable,
            **kwargs,
        )

    async def stop(self, ref: "ActorRef[Any]") -> bool:
        return await self._next_node().stop(ref)

    async def schedule[R](
        self,
        timeout: float,
        listener: "ActorRef[R]",
        message: R,
    ) -> str | None:
        return await self._next_node().schedule(timeout, listener, message)

    async def cancel_schedule(self, task_id: str) -> None:
        await self._next_node().cancel_schedule(task_id)

    async def tick[R](
        self,
        message: R,
        interval: float,
        listener: "ActorRef[R]",
    ) -> str | None:
        return await self._next_node().tick(message, interval, listener)

    async def cancel_tick(self, subscription_id: str) -> None:
        await self._next_node().cancel_tick(subscription_id)

    async def shutdown(self) -> None:
        for node in reversed(self._nodes):
            await node.shutdown()
        self._nodes.clear()
        self._round_robin_index = 0

    async def start(self) -> None:
        self._head_port = _next_free_port()
        base_config = ClusterConfig.development()

        head = ClusteredSystem(
            ClusterConfig(
                bind_host="127.0.0.1",
                bind_port=self._head_port,
                node_id=f"{self._prefix}-0",
                protocol_period=base_config.protocol_period,
                ping_timeout=base_config.ping_timeout,
                ping_req_timeout=base_config.ping_req_timeout,
                suspicion_mult=base_config.suspicion_mult,
            )
        )
        await head.start()
        self._nodes.append(head)

        for i in range(1, self._node_count):
            node = ClusteredSystem(
                ClusterConfig(
                    bind_host="127.0.0.1",
                    bind_port=_next_free_port(),
                    node_id=f"{self._prefix}-{i}",
                    seeds=[f"127.0.0.1:{self._head_port}"],
                    protocol_period=base_config.protocol_period,
                    ping_timeout=base_config.ping_timeout,
                    ping_req_timeout=base_config.ping_req_timeout,
                    suspicion_mult=base_config.suspicion_mult,
                )
            )
            await node.start()
            self._nodes.append(node)

    def node(self, identifier: int | str) -> ClusteredSystem:
        """Access a specific node by index or node_id.

        Args:
            identifier: Node index (int) or node_id string

        Returns:
            The ClusteredSystem for that node

        Examples:
            cluster.node(0)           # By index
            cluster.node("node-0")    # By node_id
        """
        if isinstance(identifier, int):
            if identifier < 0 or identifier >= len(self._nodes):
                raise IndexError(f"Node index {identifier} out of range")
            return self._nodes[identifier]

        for n in self._nodes:
            if n.node_id == identifier:
                return n
        raise KeyError(f"Node '{identifier}' not found")

    def __getitem__(self, index: int) -> ClusteredSystem:
        return self._nodes[index]

    def __len__(self) -> int:
        return len(self._nodes)

    def __iter__(self):
        return iter(self._nodes)

    @property
    def nodes(self) -> list[ClusteredSystem]:
        return list(self._nodes)

    async def __aenter__(self) -> "DevelopmentCluster":
        await self.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: object,
    ) -> None:
        await self.shutdown()
