"""Core actor primitives for Casty."""

from abc import ABC, abstractmethod
from asyncio import Future, Queue
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from .system import ActorSystem


@dataclass(frozen=True, slots=True)
class ActorId:
    """Unique identifier for an actor."""

    uid: UUID
    name: str | None = None

    def __str__(self) -> str:
        if self.name:
            return f"Actor({self.name})"
        return f"Actor({self.uid.hex[:8]})"


@dataclass(slots=True)
class Envelope[M]:
    """Internal wrapper for messages (supports ask pattern)."""

    payload: M
    reply_to: Future[Any] | None = None


class ActorRef[M]:
    """Opaque reference to send messages to an actor."""

    __slots__ = ("_id", "_mailbox", "_system")

    def __init__(
        self,
        actor_id: ActorId,
        mailbox: Queue[Envelope[M]],
        system: "ActorSystem",
    ) -> None:
        self._id = actor_id
        self._mailbox = mailbox
        self._system = system

    @property
    def id(self) -> ActorId:
        return self._id

    async def send(self, msg: M) -> None:
        """Send message (fire-and-forget)."""
        await self._mailbox.put(Envelope(msg))

    async def ask[R](self, msg: M, *, timeout: float = 5.0) -> R:
        """Send message and await response."""
        import asyncio

        loop = asyncio.get_running_loop()
        future: Future[R] = loop.create_future()
        await self._mailbox.put(Envelope(msg, reply_to=future))
        return await asyncio.wait_for(future, timeout=timeout)

    def __repr__(self) -> str:
        return f"ActorRef({self._id})"


@dataclass
class Context[M]:
    """Context available to the actor during execution."""

    self_ref: ActorRef[M]
    system: "ActorSystem"
    _current_envelope: Envelope[M] | None = None

    async def spawn[T](
        self,
        actor_cls: type["Actor[T]"],
        *,
        name: str | None = None,
        **kwargs: Any,
    ) -> ActorRef[T]:
        """Create a new actor."""
        return await self.system.spawn(actor_cls, name=name, **kwargs)

    def reply(self, response: Any) -> None:
        """Reply to the current message (used with ask pattern)."""
        if self._current_envelope and self._current_envelope.reply_to:
            if not self._current_envelope.reply_to.done():
                self._current_envelope.reply_to.set_result(response)


class Actor[M](ABC):
    """Base class for all actors."""

    _ctx: Context[M]  # Injected by the system

    async def on_start(self) -> None:
        """Hook called when the actor starts. Optional override."""
        pass

    async def on_stop(self) -> None:
        """Hook called when the actor stops. Optional override."""
        pass

    def on_error(self, exc: Exception) -> bool:
        """Hook called when receive() raises an exception.

        Returns True to continue processing, False to die.
        Default: False (actor dies).
        """
        return False

    @abstractmethod
    async def receive(self, msg: M, ctx: Context[M]) -> None:
        """Process a message. Must be implemented."""
        ...


class EntityRef[M]:
    """Reference to a specific sharded entity.

    Represents a single entity within a sharded actor type.
    Supports send (fire-and-forget) and ask (request-response) patterns.
    """

    __slots__ = ("_sharded_ref", "_entity_id")

    def __init__(self, sharded_ref: "ShardedRef[M]", entity_id: str) -> None:
        self._sharded_ref = sharded_ref
        self._entity_id = entity_id

    @property
    def entity_id(self) -> str:
        """The entity ID this reference points to."""
        return self._entity_id

    async def send(self, msg: M) -> None:
        """Send a message to this entity (fire-and-forget)."""
        await self._sharded_ref.send(self._entity_id, msg)

    async def ask(self, msg: M, *, timeout: float = 5.0) -> Any:
        """Send a message and await response."""
        return await self._sharded_ref.ask(self._entity_id, msg, timeout=timeout)

    def __repr__(self) -> str:
        return f"EntityRef({self._sharded_ref._entity_type}:{self._entity_id})"


class ShardedRef[M]:
    """Reference to a sharded actor type.

    Returned by spawn(..., shards=N). Provides access to individual entities
    via subscription (__getitem__) or direct send/ask methods.
    """

    __slots__ = (
        "_entity_type",
        "_num_shards",
        "_system",
        "_send_callback",
        "_ask_callback",
    )

    def __init__(
        self,
        entity_type: str,
        num_shards: int,
        system: "ActorSystem",
        send_callback: Any,
        ask_callback: Any,
    ) -> None:
        self._entity_type = entity_type
        self._num_shards = num_shards
        self._system = system
        self._send_callback = send_callback
        self._ask_callback = ask_callback

    @property
    def entity_type(self) -> str:
        """The entity type name for this sharded actor."""
        return self._entity_type

    @property
    def num_shards(self) -> int:
        """Number of shards for this entity type."""
        return self._num_shards

    def __getitem__(self, entity_id: str) -> EntityRef[M]:
        """Get a reference to a specific entity by ID."""
        return EntityRef(self, entity_id)

    def _get_shard_id(self, entity_id: str) -> int:
        """Calculate which shard an entity belongs to."""
        return hash(entity_id) % self._num_shards

    async def send(self, entity_id: str, msg: M) -> None:
        """Send a message to an entity (fire-and-forget)."""
        await self._send_callback(self._entity_type, entity_id, msg)

    async def ask(self, entity_id: str, msg: M, *, timeout: float = 5.0) -> Any:
        """Send a message to an entity and await response."""
        return await self._ask_callback(self._entity_type, entity_id, msg, timeout)

    def __repr__(self) -> str:
        return f"ShardedRef({self._entity_type}, shards={self._num_shards})"
