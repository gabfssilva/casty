"""Distributed mutual-exclusion lock backed by a sharded actor entity.

Waiters are queued in order and granted the lock as the current holder
releases it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING
from uuid import uuid4

from casty.actor import Behavior, Behaviors
from casty.cluster.envelope import ShardEnvelope

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.core.system import ActorSystem


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LockAcquire:
    owner: str
    reply_to: ActorRef[LockAcquired]


@dataclass(frozen=True)
class LockAcquired:
    pass


@dataclass(frozen=True)
class LockTryAcquire:
    owner: str
    reply_to: ActorRef[LockTryResult]


@dataclass(frozen=True)
class LockTryResult:
    acquired: bool


@dataclass(frozen=True)
class LockRelease:
    owner: str
    reply_to: ActorRef[LockReleased]


@dataclass(frozen=True)
class LockReleased:
    released: bool


@dataclass(frozen=True)
class DestroyLock:
    reply_to: ActorRef[bool]


type LockMsg = LockAcquire | LockTryAcquire | LockRelease | DestroyLock


# ---------------------------------------------------------------------------
# Entity behavior — two separate behaviors (free vs held)
# ---------------------------------------------------------------------------


def lock_entity(entity_id: str) -> Behavior[LockMsg]:
    return lock_free()


def lock_free() -> Behavior[LockMsg]:
    async def receive(_ctx: Any, msg: LockMsg) -> Behavior[LockMsg]:
        match msg:
            case LockAcquire(owner=owner, reply_to=reply_to):
                reply_to.tell(LockAcquired())
                return lock_held(holder=owner, waiters=())
            case LockTryAcquire(owner=owner, reply_to=reply_to):
                reply_to.tell(LockTryResult(acquired=True))
                return lock_held(holder=owner, waiters=())
            case LockRelease(reply_to=reply_to):
                reply_to.tell(LockReleased(released=False))
                return Behaviors.same()
            case DestroyLock(reply_to=reply_to):
                reply_to.tell(True)
                return Behaviors.stopped()

    return Behaviors.receive(receive)


def lock_held(
    holder: str,
    waiters: tuple[tuple[str, ActorRef[LockAcquired]], ...],
) -> Behavior[LockMsg]:
    async def receive(_ctx: Any, msg: LockMsg) -> Behavior[LockMsg]:
        match msg:
            case LockAcquire(owner=owner, reply_to=reply_to):
                return lock_held(
                    holder=holder,
                    waiters=(*waiters, (owner, reply_to)),
                )
            case LockTryAcquire(reply_to=reply_to):
                reply_to.tell(LockTryResult(acquired=False))
                return Behaviors.same()
            case LockRelease(owner=owner, reply_to=reply_to) if owner == holder:
                reply_to.tell(LockReleased(released=True))
                if waiters:
                    next_owner, next_reply = waiters[0]
                    next_reply.tell(LockAcquired())
                    return lock_held(holder=next_owner, waiters=waiters[1:])
                return lock_free()
            case LockRelease(reply_to=reply_to):
                reply_to.tell(LockReleased(released=False))
                return Behaviors.same()
            case DestroyLock(reply_to=reply_to):
                reply_to.tell(True)
                return Behaviors.stopped()

    return Behaviors.receive(receive)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class Lock:
    """Client for a distributed mutual-exclusion lock.

    Each ``Lock`` instance has a unique owner ID.  ``acquire`` blocks until
    the lock is granted; ``try_acquire`` returns immediately.

    Parameters
    ----------
    system : ActorSystem
        The actor system for sending messages.
    region_ref : ActorRef[ShardEnvelope[LockMsg]]
        Reference to the shard proxy / region.
    name : str
        Lock name (used as entity ID).
    timeout : float
        Default timeout for each operation.

    Examples
    --------
    >>> lock = d.lock("my-resource")
    >>> await lock.acquire()
    >>> await lock.release()
    True
    """

    def __init__(
        self,
        *,
        system: ActorSystem,
        region_ref: ActorRef[ShardEnvelope[LockMsg]],
        name: str,
        timeout: float = 5.0,
    ) -> None:
        self._system = system
        self._region_ref = region_ref
        self._name = name
        self._timeout = timeout
        self._owner = uuid4().hex

    async def destroy(self) -> bool:
        """Destroy this lock, stopping the backing entity actor.

        Returns
        -------
        bool
            ``True`` if destroyed.
        """
        return await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name, DestroyLock(reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def acquire(self) -> None:
        """Block until the lock is acquired.

        Examples
        --------
        >>> await lock.acquire()
        """
        await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                LockAcquire(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def try_acquire(self) -> bool:
        """Try to acquire the lock without blocking.

        Returns
        -------
        bool
            ``True`` if the lock was acquired, ``False`` if already held.

        Examples
        --------
        >>> await lock.try_acquire()
        True
        """
        result: LockTryResult = await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                LockTryAcquire(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
        return result.acquired

    async def release(self) -> bool:
        """Release the lock.

        Returns
        -------
        bool
            ``True`` if released, ``False`` if this instance was not the holder.

        Examples
        --------
        >>> await lock.release()
        True
        """
        result: LockReleased = await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                LockRelease(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
        return result.released
