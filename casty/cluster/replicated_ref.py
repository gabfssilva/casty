from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, TYPE_CHECKING

from casty.ref import ActorRef
from casty.actor_config import Routing
from casty.envelope import Envelope

if TYPE_CHECKING:
    from .replica_manager import ReplicaManager


@dataclass
class ReplicatedActorRef[M](ActorRef[M]):
    actor_id: str
    manager: "ReplicaManager"
    node_refs: dict[str, ActorRef[M]]
    routing: dict[type, Routing]
    local_node: str
    _round_robin_idx: int = field(default=0, repr=False)

    def _get_routing(self, msg: M) -> Routing:
        return self.routing.get(type(msg), Routing.LEADER)

    def _select_node(self, routing: Routing) -> str | None:
        replicas = self.manager.get_replicas(self.actor_id)
        if not replicas:
            return None

        match routing:
            case Routing.LEADER:
                return self.manager.get_leader(self.actor_id)

            case Routing.ANY:
                self._round_robin_idx = (self._round_robin_idx + 1) % len(replicas)
                return replicas[self._round_robin_idx]

            case Routing.LOCAL_FIRST:
                if self.local_node in replicas:
                    return self.local_node
                return replicas[0]

        return None

    async def send(self, msg: M, *, sender: ActorRef[Any] | None = None) -> None:
        routing = self._get_routing(msg)
        node = self._select_node(routing)

        if node and node in self.node_refs:
            await self.node_refs[node].send(msg, sender=sender)

    async def send_envelope(self, envelope: Envelope[M]) -> None:
        routing = self._get_routing(envelope.payload)
        node = self._select_node(routing)

        if node and node in self.node_refs:
            await self.node_refs[node].send_envelope(envelope)

    async def ask[R](self, msg: M, timeout: float = 30.0) -> R:
        routing = self._get_routing(msg)
        node = self._select_node(routing)

        if node and node in self.node_refs:
            return await self.node_refs[node].ask(msg, timeout=timeout)

        raise RuntimeError(f"No available node for {self.actor_id}")

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
