"""Distributed barrier backed by a sharded actor entity.

Blocks callers until a specified number of nodes have arrived, then releases
all waiters simultaneously.
"""
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


@dataclass(frozen=True)
class DestroyBarrier:
    reply_to: ActorRef[bool]


type BarrierMsg = BarrierArrive | DestroyBarrier


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
            case DestroyBarrier(reply_to=reply_to):
                reply_to.tell(True)
                return Behaviors.stopped()

    return Behaviors.receive(receive)


class Barrier:
    """Client for a distributed barrier backed by a sharded actor.

    Multiple nodes call ``arrive(expected)`` and all block until *expected*
    nodes have arrived.

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[BarrierMsg]]
        Reference to the shard proxy / region.
    name : str
        Barrier name (used as entity ID).
    node_id : str
        Identifier for this node (typically ``host:port``).
    timeout : float
        Default timeout for the barrier wait.

    Examples
    --------
    >>> barrier = d.barrier("init-sync")
    >>> await barrier.arrive(expected=3)
    """

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

    async def destroy(self) -> bool:
        """Destroy this barrier, stopping the backing entity actor.

        Returns
        -------
        bool
            ``True`` if destroyed.
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, DestroyBarrier(reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def arrive(self, expected: int) -> None:
        """Block until *expected* nodes have reached this barrier.

        Parameters
        ----------
        expected : int
            Number of nodes that must arrive before all are released.

        Examples
        --------
        >>> await barrier.arrive(expected=3)
        """
        await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                BarrierArrive(node=self._node_id, expected=expected, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
