from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING
from uuid import uuid4

from casty.actor import Behavior, Behaviors
from casty.sharding import ShardEnvelope

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.system import ActorSystem


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


type LockMsg = LockAcquire | LockTryAcquire | LockRelease


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

    return Behaviors.receive(receive)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class Lock:
    """Client for a distributed lock backed by a sharded actor."""

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

    async def acquire(self) -> None:
        """Block until the lock is acquired."""
        await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                LockAcquire(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )

    async def try_acquire(self) -> bool:
        """Try to acquire the lock without blocking. Returns True if acquired."""
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
        """Release the lock. Returns False if not the holder."""
        result: LockReleased = await self._system.ask(
            self._region_ref,
            lambda reply_to: ShardEnvelope(
                self._name,
                LockRelease(owner=self._owner, reply_to=reply_to),
            ),
            timeout=self._timeout,
        )
        return result.released
