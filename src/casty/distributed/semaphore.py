"""Distributed counting semaphore backed by a sharded actor entity.

Allows up to *max_permits* concurrent holders.  Additional acquirers are
queued and granted a permit as existing holders release.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING
from uuid import uuid4

from casty.actor import Behavior, Behaviors
from casty.cluster.envelope import ShardEnvelope

if TYPE_CHECKING:
    from casty.distributed.distributed import EntityGateway
    from casty.ref import ActorRef


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SemaphoreAcquire:
    owner: str
    reply_to: ActorRef[SemaphoreAcquired]


@dataclass(frozen=True)
class SemaphoreAcquired:
    pass


@dataclass(frozen=True)
class SemaphoreTryAcquire:
    owner: str
    reply_to: ActorRef[SemaphoreTryResult]


@dataclass(frozen=True)
class SemaphoreTryResult:
    acquired: bool


@dataclass(frozen=True)
class SemaphoreRelease:
    owner: str
    reply_to: ActorRef[SemaphoreReleased]


@dataclass(frozen=True)
class SemaphoreReleased:
    released: bool


@dataclass(frozen=True)
class DestroySemaphore:
    reply_to: ActorRef[bool]


type SemaphoreMsg = (
    SemaphoreAcquire | SemaphoreTryAcquire | SemaphoreRelease | DestroySemaphore
)


# ---------------------------------------------------------------------------
# Entity behavior — factory-of-factory (same pattern as persistent_counter_entity)
# ---------------------------------------------------------------------------


def semaphore_entity_factory(
    max_permits: int,
) -> Callable[[str], Behavior[SemaphoreMsg]]:
    def factory(entity_id: str) -> Behavior[SemaphoreMsg]:
        return semaphore_active(available=max_permits, holders=(), waiters=())

    return factory


def semaphore_active(
    available: int,
    holders: tuple[str, ...],
    waiters: tuple[tuple[str, ActorRef[SemaphoreAcquired]], ...],
) -> Behavior[SemaphoreMsg]:
    async def receive(_ctx: Any, msg: SemaphoreMsg) -> Behavior[SemaphoreMsg]:
        match msg:
            case SemaphoreAcquire(owner=owner, reply_to=reply_to) if available > 0:
                reply_to.tell(SemaphoreAcquired())
                return semaphore_active(
                    available=available - 1,
                    holders=(*holders, owner),
                    waiters=waiters,
                )
            case SemaphoreAcquire(owner=owner, reply_to=reply_to):
                return semaphore_active(
                    available=available,
                    holders=holders,
                    waiters=(*waiters, (owner, reply_to)),
                )
            case SemaphoreTryAcquire(owner=owner, reply_to=reply_to) if available > 0:
                reply_to.tell(SemaphoreTryResult(acquired=True))
                return semaphore_active(
                    available=available - 1,
                    holders=(*holders, owner),
                    waiters=waiters,
                )
            case SemaphoreTryAcquire(reply_to=reply_to):
                reply_to.tell(SemaphoreTryResult(acquired=False))
                return Behaviors.same()
            case SemaphoreRelease(owner=owner, reply_to=reply_to) if owner in holders:
                remaining = list(holders)
                remaining.remove(owner)
                new_holders = tuple(remaining)
                reply_to.tell(SemaphoreReleased(released=True))
                if waiters:
                    next_owner, next_reply = waiters[0]
                    next_reply.tell(SemaphoreAcquired())
                    return semaphore_active(
                        available=available,
                        holders=(*new_holders, next_owner),
                        waiters=waiters[1:],
                    )
                return semaphore_active(
                    available=available + 1,
                    holders=new_holders,
                    waiters=waiters,
                )
            case SemaphoreRelease(reply_to=reply_to):
                reply_to.tell(SemaphoreReleased(released=False))
                return Behaviors.same()
            case DestroySemaphore(reply_to=reply_to):
                reply_to.tell(True)
                return Behaviors.stopped()

    return Behaviors.receive(receive)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class Semaphore:
    """Client for a distributed counting semaphore.

    Each ``Semaphore`` instance has a unique owner ID.  ``acquire`` blocks
    until a permit is available; ``try_acquire`` returns immediately.

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[SemaphoreMsg]]
        Reference to the shard proxy / region.
    name : str
        Semaphore name (used as entity ID).
    timeout : float
        Default timeout for each operation.

    Examples
    --------
    >>> sem = d.semaphore("pool", permits=3)
    >>> await sem.acquire()
    >>> await sem.release()
    True
    """

    def __init__(
        self,
        *,
        gateway: EntityGateway,
        region_ref: ActorRef[ShardEnvelope[SemaphoreMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._gateway = gateway
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout
        self._owner = uuid4().hex

    async def destroy(self) -> bool:
        """Destroy this semaphore, stopping the backing entity actor.

        Returns
        -------
        bool
            ``True`` if destroyed.
        """
        return await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                DestroySemaphore(reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def acquire(self) -> None:
        """Block until a permit is acquired.

        Examples
        --------
        >>> await sem.acquire()
        """
        await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                SemaphoreAcquire(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def try_acquire(self) -> bool:
        """Try to acquire a permit without blocking.

        Returns
        -------
        bool
            ``True`` if a permit was acquired, ``False`` if none available.

        Examples
        --------
        >>> await sem.try_acquire()
        True
        """
        result: SemaphoreTryResult = await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                SemaphoreTryAcquire(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
        return result.acquired

    async def release(self) -> bool:
        """Release a permit.

        Returns
        -------
        bool
            ``True`` if released, ``False`` if this instance was not a holder.

        Examples
        --------
        >>> await sem.release()
        True
        """
        result: SemaphoreReleased = await self._gateway.entity_ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                SemaphoreRelease(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
        return result.released
