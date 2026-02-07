from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.sharding import ShardEnvelope

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.system import ActorSystem


@dataclass(frozen=True)
class BarrierArrive:
    node: str
    expected: int
    reply_to: ActorRef[BarrierReleased]


@dataclass(frozen=True)
class BarrierReleased:
    pass


type BarrierMsg = BarrierArrive


def barrier_entity(entity_id: str) -> Behavior[BarrierMsg]:
    return barrier_waiting(waiters={})


def barrier_waiting(
    waiters: dict[str, ActorRef[BarrierReleased]],
) -> Behavior[BarrierMsg]:
    async def receive(_ctx: Any, msg: BarrierMsg) -> Behavior[BarrierMsg]:
        match msg:
            case BarrierArrive(node=node, expected=expected, reply_to=reply_to):
                new_waiters = {**waiters, node: reply_to}
                if len(new_waiters) >= expected:
                    for ref in new_waiters.values():
                        ref.tell(BarrierReleased())
                    return barrier_waiting({})
                return barrier_waiting(new_waiters)

    return Behaviors.receive(receive)


class Barrier:
    """Client for a distributed barrier backed by a sharded actor."""

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[BarrierMsg]],
        name: str,
        node_id: str,
        timeout: float = 60.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._node_id = node_id
        self._timeout = timeout

    async def arrive(self, expected: int) -> None:
        """Block until *expected* nodes have reached this barrier."""
        await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                BarrierArrive(node=self._node_id, expected=expected, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
