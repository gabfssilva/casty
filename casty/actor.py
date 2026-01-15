"""Core actor primitives for Casty."""

from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import Future, Queue
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Union, get_args, get_origin
from types import UnionType
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from .supervision import SupervisorConfig, SupervisionDecision
    from .system import ActorSystem
    from .cluster.remote_ref import RemoteRef


def on(msg_type: type):
    """Decorator to register a message handler by type.

    Usage:
        class Counter(Actor[Increment | GetCount]):
            @on(Increment)
            async def handle_increment(self, msg: Increment, ctx: Context):
                self.count += msg.amount

            @on(GetCount)
            async def handle_query(self, msg: GetCount, ctx: Context):
                ctx.reply(self.count)
    """
    def decorator(fn):
        fn._handler_for = msg_type
        return fn
    return decorator


def _flatten_union_types(tp: type | None) -> tuple[type, ...]:
    """Flatten union types into a tuple of concrete types.

    Handles both typing.Union and PEP 604 unions (X | Y).

    Examples:
        int | str -> (int, str)
        Union[int, str, float] -> (int, str, float)
        int -> (int,)
    """
    if tp is None:
        return ()

    origin = get_origin(tp)

    # Handle Union types (typing.Union or X | Y)
    if origin is Union or isinstance(tp, UnionType):
        args = get_args(tp)
        result: list[type] = []
        for arg in args:
            # Recursively flatten nested unions
            result.extend(_flatten_union_types(arg))
        return tuple(result)

    # Handle regular types
    if isinstance(tp, type):
        return (tp,)

    # For other cases (like generics), try to get the origin
    if origin is not None and isinstance(origin, type):
        return (origin,)

    return ()


def _extract_actor_message_types(actor_cls: type["Actor[Any]"]) -> tuple[type, ...]:
    """Extract the message types M from Actor[M].

    Walks the class hierarchy to find Actor[M] and extracts M.
    """
    for base in getattr(actor_cls, "__orig_bases__", ()):
        origin = get_origin(base)
        if origin is Actor:
            args = get_args(base)
            if args:
                return _flatten_union_types(args[0])

    # Check parent classes
    for parent in actor_cls.__mro__[1:]:
        if parent is Actor or parent is ABC or parent is object:
            continue
        for base in getattr(parent, "__orig_bases__", ()):
            origin = get_origin(base)
            if origin is Actor:
                args = get_args(base)
                if args:
                    return _flatten_union_types(args[0])

    return ()


@dataclass(frozen=True, slots=True)
class ActorId:
    """Unique identifier for an actor."""

    uid: UUID
    name: str | None = None

    def __str__(self) -> str:
        if self.name:
            return f"Actor({self.name})"
        return f"Actor({self.uid.hex[:8]})"

    def __hash__(self) -> int:
        return hash(self.uid)


@dataclass(slots=True)
class Envelope[M]:
    """Internal wrapper for messages (supports ask pattern and sender tracking)."""

    payload: M
    reply_to: Future[Any] | None = None
    sender: "LocalRef[Any] | None" = None


class LocalRef[M]:
    """Opaque reference to send messages to an actor.

    Type-safe reference that allows sending messages to an actor
    without exposing its internal implementation.
    """

    __slots__ = ("_id", "_mailbox", "_system", "_send_impl", "_accepted_types")

    def __init__(
        self,
        actor_id: ActorId,
        mailbox: Queue[Envelope[M]],
        system: "ActorSystem",
        send_impl: Callable[[Envelope[M]], Awaitable[None]] | None = None,
        accepted_types: tuple[type, ...] = (),
    ) -> None:
        self._id = actor_id
        self._mailbox = mailbox
        self._system = system
        self._send_impl = send_impl
        self._accepted_types = accepted_types

    @property
    def accepted_types(self) -> tuple[type, ...]:
        """Get the message types this ref accepts."""
        return self._accepted_types

    def accepts(self, msg: Any) -> bool:
        """Check if this ref accepts the given message type."""
        if not self._accepted_types:
            return True  # No type info, accept anything
        return isinstance(msg, self._accepted_types)

    @property
    def id(self) -> ActorId:
        """Get the actor's unique identifier."""
        return self._id

    async def schedule[R](
        self,
        timeout: float,
        message: R,
        listener: LocalRef[R] | None = None,
    ) -> None:
        await self._system.schedule(timeout=timeout, listener=listener or self, message=message)

    async def send(self, msg: M, *, sender: "LocalRef[Any] | None" = None) -> None:
        """Send message (fire-and-forget).

        Args:
            msg: The message to send
            sender: Optional sender reference for reply routing
        """
        envelope = Envelope(msg, sender=sender)
        if self._send_impl:
            await self._send_impl(envelope)
        else:
            await self._mailbox.put(envelope)

    async def ask[R](
        self,
        msg: M,
        *,
        timeout: float = 5.0,
        sender: "LocalRef[Any] | None" = None,
    ) -> R:
        """Send message and await response.

        Args:
            msg: The message to send
            timeout: Maximum time to wait for response in seconds
            sender: Optional sender reference

        Returns:
            The response from the actor

        Raises:
            asyncio.TimeoutError: If no response received within timeout
        """
        import asyncio

        loop = asyncio.get_running_loop()
        future: Future[R] = loop.create_future()
        envelope = Envelope(msg, reply_to=future, sender=sender)

        if self._send_impl:
            await self._send_impl(envelope)
        else:
            await self._mailbox.put(envelope)

        return await asyncio.wait_for(future, timeout=timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        """Operator >> for send (tell pattern).

        Usage: await (actor >> msg)
        """
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        """Operator << for ask (request-response pattern).

        Usage: result = await (actor << msg)
        """
        return self.ask(msg)

    def __repr__(self) -> str:
        return f"ActorRef({self._id})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LocalRef):
            return self._id == other._id
        return False

    def __hash__(self) -> int:
        return hash(self._id)

    def __or__[T](
        self, other: "LocalRef[T] | RemoteRef[T] | CompositeRef[T]"
    ) -> "CompositeRef[M | T]":
        """Combine refs with type-based routing.

        Usage:
            combined = ref1 | ref2
            await combined.send(msg)  # Routes to ref1 or ref2 based on msg type
        """
        return CompositeRef.from_refs(self, other)


# Type alias for any ref that can be combined
type AnyRef = "LocalRef[Any] | RemoteRef[Any] | CompositeRef[Any]"


class CompositeRef[M]:
    """Reference that routes messages to different actors based on message type.

    Created using the | operator on LocalRef/RemoteRef instances:
        combined = ref1 | ref2  # ref1 accepts Msg1, ref2 accepts Msg2
        await combined.send(some_msg)  # Routes based on isinstance check

    The first ref that accepts the message type wins.
    """

    __slots__ = ("_refs", "_type_to_ref")

    def __init__(
        self,
        refs: tuple["LocalRef[Any] | RemoteRef[Any]", ...],
        type_to_ref: dict[type, "LocalRef[Any] | RemoteRef[Any]"],
    ) -> None:
        self._refs = refs
        self._type_to_ref = type_to_ref

    @classmethod
    def from_refs(
        cls,
        *refs: "LocalRef[Any] | RemoteRef[Any] | CompositeRef[Any]",
    ) -> "CompositeRef[Any]":
        """Create a CompositeRef from multiple refs."""
        all_refs: list[LocalRef[Any] | RemoteRef[Any]] = []
        type_to_ref: dict[type, LocalRef[Any] | RemoteRef[Any]] = {}

        for ref in refs:
            if isinstance(ref, CompositeRef):
                # Flatten nested CompositeRefs
                for inner_ref in ref._refs:
                    all_refs.append(inner_ref)
                type_to_ref.update(ref._type_to_ref)
            else:
                all_refs.append(ref)
                # Map each accepted type to this ref
                for msg_type in ref._accepted_types:
                    if msg_type not in type_to_ref:
                        type_to_ref[msg_type] = ref

        return cls(tuple(all_refs), type_to_ref)

    def _find_ref(self, msg: Any) -> "LocalRef[Any] | RemoteRef[Any]":
        """Find the appropriate ref for a message."""
        msg_type = type(msg)

        # Direct type match
        if msg_type in self._type_to_ref:
            return self._type_to_ref[msg_type]

        # Check isinstance for inheritance
        for accepted_type, ref in self._type_to_ref.items():
            if isinstance(msg, accepted_type):
                return ref

        # Fallback: try refs without type info, or first ref that accepts
        for ref in self._refs:
            if ref.accepts(msg):
                return ref

        raise TypeError(
            f"No actor ref accepts message of type {msg_type.__name__}. "
            f"Known types: {list(self._type_to_ref.keys())}"
        )

    async def send(self, msg: M, *, sender: "LocalRef[Any] | None" = None) -> None:
        """Send message to the appropriate actor based on message type."""
        ref = self._find_ref(msg)
        await ref.send(msg, sender=sender)

    async def ask[R](
        self,
        msg: M,
        *,
        timeout: float = 5.0,
        sender: "LocalRef[Any] | None" = None,
    ) -> R:
        """Send message and await response from the appropriate actor."""
        ref = self._find_ref(msg)
        return await ref.ask(msg, timeout=timeout, sender=sender)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        """Operator >> for send (tell pattern)."""
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        """Operator << for ask (request-response pattern)."""
        return self.ask(msg)

    def __or__[T](
        self, other: "LocalRef[T] | RemoteRef[T] | CompositeRef[T]"
    ) -> "CompositeRef[M | T]":
        """Chain with another ref."""
        return CompositeRef.from_refs(self, other)

    def __repr__(self) -> str:
        types = [t.__name__ for t in self._type_to_ref.keys()]
        return f"CompositeRef({', '.join(types)})"


# Type alias for behavior functions
type Behavior[M] = Callable[["M", "Context[M]"], Awaitable[None]]


@dataclass
class Context[M]:
    """Context available to the actor during message processing.

    Provides access to the actor system, self reference,
    child management, and message reply functionality.
    """

    self_ref: LocalRef[M]
    system: "ActorSystem"
    parent: LocalRef[Any] | None = None
    sender: LocalRef[Any] | None = None
    _current_envelope: Envelope[M] | None = field(default=None, repr=False)
    _supervision_node: Any = field(default=None, repr=False)
    _behavior_stack: list[Behavior[M]] = field(default_factory=list, repr=False)

    async def spawn[T](
        self,
        actor_cls: type["Actor[T]"],
        *,
        name: str | None = None,
        supervision: "SupervisorConfig | None" = None,
        **kwargs: Any,
    ) -> LocalRef[T]:
        """Spawn a new actor as a child of this actor.

        Args:
            actor_cls: The actor class to instantiate
            name: Optional name for the actor
            supervision: Override supervision config for this child
            **kwargs: Constructor arguments for the actor

        Returns:
            Reference to the spawned child actor
        """
        return await self.system._spawn_child(
            parent_ctx=self,
            actor_cls=actor_cls,
            name=name,
            supervision=supervision,
            **kwargs,
        )

    def reply(self, response: Any) -> None:
        """Reply to the current message (used with ask pattern).

        Args:
            response: The response to send back to the caller
        """
        if self._current_envelope and self._current_envelope.reply_to:
            if not self._current_envelope.reply_to.done():
                self._current_envelope.reply_to.set_result(response)

    @property
    def children(self) -> dict[ActorId, LocalRef[Any]]:
        """Get all child actors spawned by this actor."""
        if self._supervision_node is None:
            return {}
        return {
            child_id: node.actor_ref
            for child_id, node in self._supervision_node.children.items()
        }

    async def stop_child(self, child: LocalRef[Any]) -> bool:
        """Stop a specific child actor.

        Args:
            child: Reference to the child to stop

        Returns:
            True if child was found and stopped, False otherwise
        """
        return await self.system._stop_child(self, child.id)

    async def restart_child(self, child: LocalRef[Any]) -> LocalRef[Any]:
        """Manually restart a child actor.

        Args:
            child: Reference to the child to restart

        Returns:
            New reference to the restarted actor
        """
        return await self.system._restart_child(self, child.id)

    async def stop_all_children(self) -> None:
        """Stop all child actors."""
        await self.system._stop_all_children(self)

    def become(self, behavior: Behavior[M], *, discard_old: bool = False) -> None:
        """Switch the actor's message handling behavior.

        Similar to Akka's become(), this allows an actor to change how it
        processes messages dynamically - useful for implementing state machines.

        Args:
            behavior: The new message handler function (async def handler(msg, ctx))
            discard_old: If True, replaces the current behavior without stacking.
                        If False (default), pushes onto behavior stack for unbecome().

        Example:
            async def receive(self, msg, ctx):
                match msg:
                    case Activate():
                        ctx.become(self.active)

            async def active(self, msg, ctx):
                match msg:
                    case Deactivate():
                        ctx.unbecome()
        """
        if discard_old and self._behavior_stack:
            self._behavior_stack[-1] = behavior
        else:
            self._behavior_stack.append(behavior)

    def unbecome(self) -> None:
        """Revert to the previous behavior.

        Pops the current behavior from the stack, reverting to the previous one.
        If the stack is empty, the actor continues using its default receive().
        """
        if self._behavior_stack:
            self._behavior_stack.pop()


class Actor[M](ABC):
    """Base class for all actors.

    Actors are the fundamental unit of computation in Casty.
    Each actor has its own mailbox and processes messages sequentially.

    Type parameter M represents the message type(s) this actor can receive.
    Use union types for multiple message types: Actor[Msg1 | Msg2 | Msg3]

    Message handlers can be registered using the @on decorator:
        class Counter(Actor[Increment | GetCount]):
            @on(Increment)
            async def handle_increment(self, msg: Increment, ctx: Context):
                self.count += msg.amount
    """

    _ctx: Context[M]
    _handlers: dict[type, Callable] = {}

    def __init_subclass__(cls):
        """Collect all @on decorated handlers from the class hierarchy."""
        super().__init_subclass__()
        cls._handlers = {}
        for attr_name in dir(cls):
            try:
                attr = getattr(cls, attr_name)
                if hasattr(attr, '_handler_for'):
                    cls._handlers[attr._handler_for] = attr
            except AttributeError:
                pass

    # Lifecycle hooks

    async def on_start(self) -> None:
        """Hook called when the actor starts.

        Override to perform initialization logic.
        Default: no-op
        """
        pass

    async def on_stop(self) -> None:
        """Hook called when the actor stops.

        Override to perform cleanup logic.
        Default: no-op
        """
        pass

    async def pre_restart(self, exc: Exception, msg: M | None) -> None:
        """Hook called before the actor is restarted due to failure.

        Args:
            exc: The exception that caused the restart
            msg: The message being processed when failure occurred (if any)

        Default: calls on_stop()
        """
        await self.on_stop()

    async def post_restart(self, exc: Exception) -> None:
        """Hook called after the actor is restarted.

        Args:
            exc: The exception that caused the restart

        Default: calls on_start()
        """
        await self.on_start()

    # Supervision hook

    async def on_child_failure(
        self,
        child: LocalRef[Any],
        exc: Exception,
    ) -> "SupervisionDecision | None":
        """Called when a child actor fails.

        Override to provide custom supervision logic.

        Args:
            child: Reference to the failed child
            exc: The exception that caused the failure

        Returns:
            SupervisionDecision to override default, or None for default behavior
        """
        return None

    # State persistence (for replication)

    # Override this to exclude specific fields from state serialization
    __casty_exclude_fields__: set[str] = set()

    def get_state(self) -> dict[str, Any]:
        """Get actor state for replication/persistence.

        Default implementation serializes all public instance attributes
        (those not starting with '_'), excluding fields listed in
        __casty_exclude_fields__.

        Override for custom serialization logic.

        Returns:
            Dictionary of state to persist/replicate
        """
        exclude = {"_ctx"} | self.__casty_exclude_fields__
        return {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in exclude
        }

    def set_state(self, state: dict[str, Any]) -> None:
        """Restore actor state from replication/persistence.

        Default implementation updates instance attributes from the dict.

        Override for custom deserialization logic.

        Args:
            state: Previously persisted state dictionary
        """
        for k, v in state.items():
            setattr(self, k, v)

    async def receive(self, msg: M, ctx: Context[M]) -> None:
        """Process a message.

        This is the main message handler. Can be overridden with custom logic,
        or use @on decorators to register handlers by message type.

        Args:
            msg: The message to process
            ctx: The execution context providing system access
        """
        # Check if there are registered handlers via @on decorators
        if self.__class__._handlers:
            handler = self.__class__._handlers.get(type(msg))
            if handler is not None:
                return await handler(self, msg, ctx)

        # If no handler found and receive was overridden, it will be called
        # If not overridden and no handler, raise NotImplementedError
        raise NotImplementedError(f"No handler for {type(msg).__name__}")


# Sharded actor types


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

    async def ask[R](self, msg: M, *, timeout: float = 5.0) -> R:
        """Send a message and await response."""
        return await self._sharded_ref.ask(self._entity_id, msg, timeout=timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        """Operator >> for send (tell pattern).

        Usage: await (entity >> msg)
        """
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        """Operator << for ask (request-response pattern).

        Usage: result = await (entity << msg)
        """
        return self.ask(msg)

    def __repr__(self) -> str:
        return f"EntityRef({self._sharded_ref._entity_type}:{self._entity_id})"


class ShardedRef[M]:
    """Reference to a sharded actor type.

    Sharded actors are distributed across the cluster using consistent hashing.
    Each entity_id maps to a specific node that owns that entity.

    Usage:
        accounts = await system.spawn(Account, name="accounts", sharded=True)
        await accounts["user-123"].send(Deposit(100))
        balance = await accounts["user-123"].ask(GetBalance())
    """

    __slots__ = (
        "_entity_type",
        "_replication_factor",
        "_system",
        "_send_callback",
        "_ask_callback",
    )

    def __init__(
        self,
        entity_type: str,
        system: "ActorSystem",
        send_callback: Callable[[str, str, Any], Awaitable[None]],
        ask_callback: Callable[[str, str, Any, float], Awaitable[Any]],
        replication_factor: int = 3,
    ) -> None:
        self._entity_type = entity_type
        self._system = system
        self._send_callback = send_callback
        self._ask_callback = ask_callback
        self._replication_factor = replication_factor

    @property
    def entity_type(self) -> str:
        """The entity type name for this sharded actor."""
        return self._entity_type

    @property
    def replication_factor(self) -> int:
        """Number of replicas for each entity."""
        return self._replication_factor

    def __getitem__(self, entity_id: str) -> EntityRef[M]:
        """Get a reference to a specific entity by ID.

        Args:
            entity_id: The unique identifier for the entity

        Returns:
            EntityRef for the specified entity
        """
        return EntityRef(self, entity_id)

    async def send(self, entity_id: str, msg: M) -> None:
        """Send a message to an entity (fire-and-forget).

        Args:
            entity_id: The target entity identifier
            msg: The message to send
        """
        await self._send_callback(self._entity_type, entity_id, msg)

    async def ask(self, entity_id: str, msg: M, *, timeout: float = 5.0) -> Any:
        """Send a message to an entity and await response.

        Args:
            entity_id: The target entity identifier
            msg: The message to send
            timeout: Maximum time to wait for response

        Returns:
            The response from the entity
        """
        return await self._ask_callback(self._entity_type, entity_id, msg, timeout)

    def __repr__(self) -> str:
        return f"ShardedRef({self._entity_type}, rf={self._replication_factor})"
