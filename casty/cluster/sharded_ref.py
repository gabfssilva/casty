from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, TYPE_CHECKING

from ..ref import ActorRef

if TYPE_CHECKING:
    from casty.envelope import Envelope
    from .membership import MemberInfo


class ShardResolver(Protocol):
    async def resolve_address(self, node_id: str) -> str: ...
    async def get_leader_id(self, actor_id: str) -> str: ...


@dataclass
class MembershipShardResolver:
    membership_ref: ActorRef
    members: dict[str, "MemberInfo"]
    replicas: int

    async def resolve_address(self, node_id: str) -> str:
        if node_id in self.members:
            return self.members[node_id].address
        from .messages import GetAddress
        address = await self.membership_ref.ask(GetAddress(node_id))
        if address:
            return address
        raise RuntimeError(f"Unknown node: {node_id}")

    async def get_leader_id(self, actor_id: str) -> str:
        from .messages import GetLeaderId
        return await self.membership_ref.ask(GetLeaderId(actor_id, self.replicas))


@dataclass
class ShardedActorRef[M](ActorRef[M]):
    actor_id: str
    resolver: ShardResolver
    send_fn: Callable[[str, str, M], Awaitable[None]]
    ask_fn: Callable[[str, str, M, float], Awaitable[Any]]
    known_leader_id: str | None = None

    async def _get_target_address(self) -> str:
        if self.known_leader_id is None:
            self.known_leader_id = await self.resolver.get_leader_id(self.actor_id)
        return await self.resolver.resolve_address(self.known_leader_id)  # type: ignore[arg-type]

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        address = await self._get_target_address()
        await self.send_fn(address, self.actor_id, msg)

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        await self.send(envelope.payload, sender=envelope.sender)

    async def ask[R](self, msg: M, timeout: float = 10.0) -> R:
        address = await self._get_target_address()
        return await self.ask_fn(address, self.actor_id, msg, timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
