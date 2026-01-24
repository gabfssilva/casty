from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, TYPE_CHECKING

from ..ref import ActorRef

if TYPE_CHECKING:
    from casty.envelope import Envelope
    from .shard import ShardCoordinator
    from .membership import MemberInfo


class ShardResolver(Protocol):
    def resolve_address(self, node_id: str) -> str: ...
    def get_leader_id(self, actor_id: str) -> str: ...


@dataclass
class ClusterShardResolver:
    shard_coordinator: "ShardCoordinator"
    members: dict[str, "MemberInfo"]
    replicas: int

    def resolve_address(self, node_id: str) -> str:
        if node_id in self.members:
            return self.members[node_id].address
        raise RuntimeError(f"Unknown node: {node_id}")

    def get_leader_id(self, actor_id: str) -> str:
        return self.shard_coordinator.get_leader_id(actor_id, self.replicas)


@dataclass
class ShardedActorRef[M](ActorRef[M]):
    actor_id: str
    resolver: ShardResolver
    send_fn: Callable[[str, str, M], Awaitable[None]]
    ask_fn: Callable[[str, str, M, float], Awaitable[Any]]
    known_leader_id: str | None = None

    def _get_target_address(self) -> str:
        if self.known_leader_id is None:
            self.known_leader_id = self.resolver.get_leader_id(self.actor_id)
        return self.resolver.resolve_address(self.known_leader_id)

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        address = self._get_target_address()
        await self.send_fn(address, self.actor_id, msg)

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        await self.send(envelope.payload, sender=envelope.sender)

    async def ask[R](self, msg: M, timeout: float = 10.0) -> R:
        address = self._get_target_address()
        return await self.ask_fn(address, self.actor_id, msg, timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
