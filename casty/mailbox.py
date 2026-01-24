from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, AsyncIterator, Callable, Protocol

from .context import Context
from .envelope import Envelope
from .ref import ActorRef, UnresolvedActorRef

type MessageStream[M] = AsyncGenerator[tuple[M, Context], None]
type Filter[M] = Callable[[Any, MessageStream[M]], MessageStream[M]]


@dataclass
class Stop:
    pass


class Mailbox[M](Protocol):
    def __aiter__(self) -> AsyncIterator[tuple[M, Context]]:
        ...

    async def __anext__(self) -> tuple[M, Context]:
        ...

    async def put(self, envelope: Envelope[M | Stop]) -> None:
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
        from .state import Stateful

        self._queue: asyncio.Queue[Envelope[M | Stop]] = asyncio.Queue()
        self._state = state
        self._filters = filters or []
        self._self_id = self_id
        self._node_id = node_id
        self._is_leader = is_leader
        self._system = system
        self._self_ref = self_ref
        self._stream: MessageStream[M] | None = None
        self._stateful: Stateful | None = None

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
