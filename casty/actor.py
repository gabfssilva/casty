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
