from __future__ import annotations

import asyncio
from asyncio import Future
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Literal,
    Protocol,
    TYPE_CHECKING,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

from .serializable import serializable
from . import logger

if TYPE_CHECKING:
    from .state import State, Stateful


type MessageStream[M] = AsyncGenerator[tuple[M, Context], None]
type Filter[M] = Callable[[Any, MessageStream[M]], MessageStream[M]]
type DeliverFn[M] = Callable[[Envelope[M]], Awaitable[None]]
type WriteQuorum = int | Literal["async", "all", "quorum"]


@dataclass
class ActorReplicationConfig:
    clustered: bool = False
    replicas: int | None = None
    write_quorum: WriteQuorum = "async"

    def __post_init__(self) -> None:
        if self.replicas is not None:
            if self.replicas <= 0:
                raise ValueError("replicas must be > 0")
            self.clustered = True
        if self.clustered and self.replicas is None:
            self.replicas = 1

    def resolve_write_quorum(self) -> int:
        match self.write_quorum:
            case "async":
                return 0
            case "all":
                return self.replicas or 0
            case "quorum":
                return ((self.replicas or 0) // 2) + 1
            case int() as n:
                return n


class System(Protocol):
    @property
    def node_id(self) -> str: ...

    async def actor[M](
        self,
        behavior: "Behavior | None" = None,
        *,
        name: str,
        filters: "list[Filter] | None" = None,
        node_id: str | None = None,
    ) -> "ActorRef[M] | None": ...

    async def ask[M, R](
        self,
        ref: "ActorRef[M]",
        msg: M,
        timeout: float = 30.0,
        filters: "list[Filter] | None" = None,
    ) -> R: ...

    async def shutdown(self) -> None: ...

    async def __aenter__(self) -> "System": ...

    async def __aexit__(self, *args: Any) -> None: ...


@runtime_checkable
class ActorRef[M](Protocol):
    actor_id: str

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None: ...

    async def send_envelope(self, envelope: "Envelope[M]") -> None: ...

    async def ask(self, msg: M, timeout: float = 10.0) -> Any: ...

    def __rshift__(self, msg: M) -> Awaitable[None]: ...

    def __lshift__[R](self, msg: M) -> Awaitable[R]: ...


@serializable
@dataclass
class Envelope[M]:
    payload: M
    sender: "UnresolvedActorRef | ActorRef[Any] | None" = None
    target: str | None = None
    correlation_id: str | None = None
    reply_to: "Future[Any] | None" = field(default=None, repr=False)


@serializable
@dataclass
class UnresolvedActorRef:
    actor_id: str
    node_id: str

    async def resolve[M](self, system: System) -> "ActorRef[M] | None":
        return await system.actor(name=self.actor_id, node_id=self.node_id)


@dataclass
class LocalActorRef[M](ActorRef[M]):
    actor_id: str
    _deliver: DeliverFn[M]
    _system: "System | None" = field(default=None, repr=False)
    default_timeout: float = 30.0

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        logger.debug("send", actor_id=self.actor_id, msg_type=type(msg).__name__, sender=sender.actor_id if sender else None)
        envelope = Envelope(payload=msg, sender=sender)
        await self._deliver(envelope)

    async def send_envelope(self, envelope: Envelope[M]) -> None:
        await self._deliver(envelope)

    async def ask[R](self, msg: M, timeout: float = 10.0) -> R:
        logger.debug("ask", actor_id=self.actor_id, msg_type=type(msg).__name__, timeout=timeout)
        if self._system is None:
            raise RuntimeError("ActorRef not bound to system")
        return await self._system.ask(self, msg, timeout or self.default_timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)


@dataclass
class Context:
    self_id: str
    sender: "ActorRef[Any] | None" = None
    node_id: str = "local"
    is_leader: bool = True
    _system: Any = field(default=None, repr=False)
    _self_ref: "ActorRef[Any] | None" = field(default=None, repr=False)
    reply_to: "Future[Any] | None" = field(default=None, repr=False)

    async def reply(self, value: Any) -> None:
        if self.reply_to is not None:
            if not self.reply_to.done():
                self.reply_to.set_result(value)
            return

        if self.sender is not None:
            await self.sender.send(Reply(result=value))

    async def forward(self, value: Any, to: ActorRef[Any]) -> None:
        await to.send(msg=value, sender=self.sender)

    async def actor[M](
        self,
        behavior: "Behavior",
        *,
        name: str,
    ) -> "ActorRef[M]":
        if self._system is None:
            raise RuntimeError("Context not bound to system")

        return await self._system._create_child(
            parent_id=self.self_id,
            behavior=behavior,
            name=name,
        )

    async def schedule[M](
        self,
        msg: M,
        *,
        to: "ActorRef[M] | None" = None,
        delay: float | None = None,
        every: float | None = None,
        sender: "ActorRef | None" = None,
    ) -> Any:
        if self._system is None:
            raise RuntimeError("Context not bound to system")

        target = to if to is not None else self._self_ref
        return await self._system.schedule(msg, to=target, delay=delay, every=every, sender=sender)


class Mailbox[M](Protocol):
    def __aiter__(self) -> AsyncIterator[tuple[M, Context]]:
        ...

    async def __anext__(self) -> tuple[M, Context]:
        ...

    async def put(self, envelope: "Envelope[M | Stop]") -> None:
        ...

    async def schedule[T](
        self,
        msg: T,
        *,
        delay: float | None = None,
        every: float | None = None,
        sender: "ActorRef | None" = None,
    ) -> Any: ...

    def ref(self) -> ActorRef[M]:
        pass


class ActorMailbox[M](Mailbox[M]):
    def __init__(
        self,
        state: Any = None,
        filters: list[Filter[M]] | None = None,
        self_id: str = "",
        node_id: str = "local",
        is_leader: bool = True,
        system: Any = None,
        self_ref: Any = None,
    ) -> None:
        self._queue: asyncio.Queue[Envelope[M | Stop]] = asyncio.Queue()
        self._state = state
        self._filters = filters or []
        self._self_id = self_id
        self._node_id = node_id
        self._is_leader = is_leader
        self._system = system
        self._self_ref = self_ref
        self._stream: MessageStream[M] | None = None
        self._stateful: "Stateful | None" = None

    async def _resolve_sender(self, sender: UnresolvedActorRef | ActorRef[Any] | None) -> ActorRef[Any] | None:
        match sender:
            case ActorRef() as ref:
                return ref
            case UnresolvedActorRef() as unresolved:
                return await unresolved.resolve(self._system)
            case None:
                return None

    def _base_stream(self) -> MessageStream[M]:
        async def stream() -> MessageStream[M]:
            while True:
                envelope = await self._queue.get()

                if isinstance(envelope.payload, Stop):
                    return

                sender_ref = await self._resolve_sender(envelope.sender)

                ctx = Context(
                    self_id=self._self_id,
                    sender=sender_ref,
                    node_id=self._node_id,
                    is_leader=self._is_leader,
                    _system=self._system,
                    _self_ref=self._self_ref,
                    reply_to=envelope.reply_to,
                )

                yield envelope.payload, ctx

        return stream()

    def _apply_filter(self, f: Filter[M], stream: MessageStream[M]) -> MessageStream[M]:
        return f(self._state, stream)

    def _build_stream(self) -> MessageStream[M]:
        stream = self._base_stream()
        for f in self._filters:
            stream = self._apply_filter(f, stream)
        return stream

    def __aiter__(self) -> AsyncIterator[tuple[M, Context]]:
        self._stream = self._build_stream()
        return self

    async def __anext__(self) -> tuple[M, Context]:
        if self._stream is None:
            self._stream = self._build_stream()
        try:
            return await self._stream.__anext__()
        except StopAsyncIteration:
            raise

    async def put(self, envelope: Envelope[M | Stop]) -> None:
        await self._queue.put(envelope)

    def set_self_ref(self, ref: Any) -> None:
        self._self_ref = ref

    def set_is_leader(self, value: bool) -> None:
        self._is_leader = value

    @property
    def state(self) -> Any:
        return self._state

    async def schedule[T](
        self,
        msg: T,
        *,
        delay: float | None = None,
        every: float | None = None,
        sender: ActorRef | None = None,
    ) -> Any:
        if self._system is None:
            raise RuntimeError("Mailbox not bound to system")
        return await self._system.schedule(msg, to=self._self_ref, delay=delay, every=every, sender=sender)

    def ref(self) -> ActorRef[M]:
        return self._self_ref


_actor_registry: dict[str, "Behavior"] = {}


def get_registered_actor(name: str) -> "Behavior | None":
    return _actor_registry.get(name)


def register_behavior(name: str, behavior: "Behavior") -> None:
    _actor_registry[name] = behavior


def clear_actor_registry() -> None:
    _actor_registry.clear()


get_behavior = get_registered_actor


@dataclass
class Behavior[**P]:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_kwargs: dict[str, Any] = field(default_factory=dict)
    supervision: Any = None
    state_param: str | None = None
    state_initial: Any = None
    system_param: str | None = None
    __replication_config__: "ActorReplicationConfig | None" = None

    @property
    def __name__(self) -> str:
        return self.func.__name__

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> "Behavior[()]":
        import inspect
        sig = inspect.signature(self.func)
        positional_params = [
            name for name, param in sig.parameters.items()
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD
            )
        ]

        bound_kwargs = dict(kwargs)
        for i, arg in enumerate(args):
            if i < len(positional_params):
                bound_kwargs[positional_params[i]] = arg

        state_initial = None
        if self.state_param and self.state_param in bound_kwargs:
            state_initial = bound_kwargs.pop(self.state_param)

        return Behavior(
            func=self.func,
            initial_kwargs=bound_kwargs,
            supervision=self.supervision,
            state_param=self.state_param,
            state_initial=state_initial,
            system_param=self.system_param,
            __replication_config__=self.__replication_config__,
        )


def _find_param(func: Callable[..., Any], target: type, *, generic: bool = False) -> str | None:
    try:
        hints = get_type_hints(func)
        for name, annotation in hints.items():
            if generic and get_origin(annotation) is target:
                return name
            if not generic and annotation is target:
                return name
    except (NameError, AttributeError, TypeError):
        pass

    target_name = target.__name__
    annotations = getattr(func, "__annotations__", {})
    for name, annotation in annotations.items():
        match annotation:
            case str() if generic and annotation.startswith(f"{target_name}["):
                return name
            case str() if not generic and annotation == target_name:
                return name

    return None


def actor[**P](
    func: Callable[P, Coroutine[Any, Any, None]] | None = None,
    *,
    clustered: bool = False,
    replicas: int | None = None,
    write_quorum: WriteQuorum = "async",
) -> Behavior[P] | Callable[[Callable[P, Coroutine[Any, Any, None]]], Behavior[P]]:
    def decorator(f: Callable[P, Coroutine[Any, Any, None]]) -> Behavior[P]:
        from .state import State

        replication_config = None
        if clustered or replicas is not None:
            replication_config = ActorReplicationConfig(
                clustered=clustered,
                replicas=replicas,
                write_quorum=write_quorum,
            )

        explicit_state_param = _find_param(f, State, generic=True)
        system_param = _find_param(f, System)
        supervision = getattr(f, "__supervision__", None)

        behavior: Behavior[P] = Behavior(
            func=f,
            supervision=supervision,
            state_param=explicit_state_param,
            system_param=system_param,
            __replication_config__=replication_config,
        )

        _actor_registry[f.__name__] = behavior
        return behavior

    if func is not None:
        return decorator(func)
    return decorator


def message[T](cls: type[T] | None = None, *, readonly: bool = False) -> type[T]:
    from dataclasses import dataclass as dc
    def decorator(cls: type[T]) -> type[T]:
        cls = dc(cls)
        cls = serializable(cls)
        cls.__readonly__ = readonly
        return cls

    if cls is None:
        return decorator
    return decorator(cls)


@dataclass
class Stop:
    pass


@message
class Reply[R]:
    result: R | Exception


@message
class Cancel:
    reason: Exception | None


@actor
async def reply[M, R](
    content: M,
    to: ActorRef[M],
    promise: Future[R],
    timeout: float,
    *,
    mailbox: Mailbox[Reply[R] | Cancel]
):
    timeout_task = mailbox.schedule(Cancel(reason=TimeoutError()), delay=timeout)
    forward_task = to.send(content, sender=mailbox.ref())

    await asyncio.gather(timeout_task, forward_task)

    async for msg, ctx in mailbox:
        match msg:
            case Reply(result=e) if isinstance(e, Exception):
                promise.set_exception(e)

            case Reply(result=result):
                promise.set_result(result)

            case Cancel(reason=e):
                if not promise.done():
                    promise.set_exception(e or TimeoutError())

        return
