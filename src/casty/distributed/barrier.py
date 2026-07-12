"""Distributed barrier backed by a sharded actor entity.

Blocks callers until a specified number of arrivals, then releases all
waiters simultaneously.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.cluster.envelope import ShardEnvelope

if TYPE_CHECKING:
    from casty.distributed.distributed import EntityGateway
    from casty.ref import ActorRef


@dataclass(frozen=True)
class BarrierArrive:
    expected: int
    reply_to: ActorRef[BarrierReleased | BarrierMismatch]


@dataclass(frozen=True)
class BarrierReleased:
    pass


@dataclass(frozen=True)
class BarrierMismatch:
    """The barrier was opened with a different ``expected`` value."""

    expected: int


@dataclass(frozen=True)
class DestroyBarrier:
    reply_to: ActorRef[bool]


type BarrierMsg = BarrierArrive | DestroyBarrier


def barrier_entity(entity_id: str) -> Behavior[BarrierMsg]:
    return barrier_waiting(expected=None, waiters=())


def barrier_waiting(
    expected: int | None,
    waiters: tuple[ActorRef[BarrierReleased | BarrierMismatch], ...],
) -> Behavior[BarrierMsg]:
    async def receive(_ctx: Any, msg: BarrierMsg) -> Behavior[BarrierMsg]:
        match msg:
            case BarrierArrive(expected=arrived_expected, reply_to=reply_to):
                # The first arrival fixes ``expected`` for this round;
                # disagreeing arrivals are rejected instead of silently
                # shrinking or growing the barrier.
                if expected is not None and arrived_expected != expected:
                    reply_to.tell(BarrierMismatch(expected=expected))
                    return Behaviors.same()
                required = expected if expected is not None else arrived_expected
                new_waiters = (*waiters, reply_to)
                if len(new_waiters) >= required:
                    for ref in new_waiters:
                        ref.tell(BarrierReleased())
                    return barrier_waiting(expected=None, waiters=())
                return barrier_waiting(expected=required, waiters=new_waiters)
            case DestroyBarrier(reply_to=reply_to):
                reply_to.tell(True)
                return Behaviors.stopped()

    return Behaviors.receive(receive)


class Barrier:
    """Client for a distributed barrier backed by a sharded actor.

    Multiple callers invoke ``arrive(expected)`` and all block until
    *expected* arrivals have been counted.  Every arrival counts — two
    tasks on the same node are two arrivals.

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[BarrierMsg]]
        Reference to the shard proxy / region.
    name : str
        Barrier name (used as entity ID).
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
        gateway: EntityGateway,
        region_ref: ActorRef[ShardEnvelope[BarrierMsg]],
        name: str,
        timeout: float = 60.0,
    ) -> None:
        self._gateway = gateway
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout

    async def destroy(self) -> bool:
        """Destroy this barrier, stopping the backing entity actor.

        Returns
        -------
        bool
            ``True`` if destroyed.
        """
        return await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                DestroyBarrier(reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def arrive(self, expected: int) -> None:
        """Block until *expected* arrivals have reached this barrier.

        Parameters
        ----------
        expected : int
            Number of arrivals required before all are released.  Must
            match the value given by the first arrival of the round.

        Raises
        ------
        ValueError
            If the barrier is already open with a different ``expected``.

        Examples
        --------
        >>> await barrier.arrive(expected=3)
        """
        reply = await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                BarrierArrive(expected=expected, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
        if isinstance(reply, BarrierMismatch):
            msg = (
                f"barrier {self._name!r} expects {reply.expected} arrivals, "
                f"got arrive(expected={expected})"
            )
            raise ValueError(msg)
