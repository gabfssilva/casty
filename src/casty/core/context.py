"""Actor context protocol for the new Behavior primitive.

Defines the capabilities available inside a behavior handler:
own ref, child spawning, logging, death-watch, interceptors, pipe-to-self.
Also defines ``System``, the protocol for the actor system visible from behaviors.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.core.behavior import Behavior
    from casty.core.event_stream import EventStreamMsg
    from casty.core.mailbox import Mailbox
    from casty.core.ref import ActorRef
    from casty.core.scheduler import SchedulerMsg


class System(Protocol):
    """Protocol exposing the actor system's public API to behaviors."""

    def __make_ref__[M](
        self,
        id: str,
        deliver: Callable[[Any], None],
    ) -> ActorRef[M]: ...

    @property
    def name(self) -> str: ...

    @property
    def event_stream(self) -> ActorRef[EventStreamMsg]: ...

    @property
    def scheduler(self) -> ActorRef[SchedulerMsg]: ...

    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]: ...

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R: ...

    async def ask_or_none[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R | None: ...

    def lookup(self, path: str) -> ActorRef[Any] | None: ...

    async def shutdown(self) -> None: ...


class ActorContext[M](Protocol):
    """Protocol for the context available to actor behavior handlers."""

    @property
    def self(self) -> ActorRef[M]: ...

    @property
    def system(self) -> System: ...

    @property
    def log(self) -> logging.Logger: ...

    def spawn[C](
        self,
        behavior: Behavior[C],
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]: ...

    def stop(self, ref: ActorRef[Any]) -> None: ...

    def watch(self, ref: ActorRef[Any]) -> None: ...

    def unwatch(self, ref: ActorRef[Any]) -> None: ...

    def on_stop(self, callback: Callable[[], Awaitable[None]]) -> None: ...

    def register_interceptor(self, interceptor: Callable[[object], bool]) -> None: ...

    @overload
    def pipe_to_self(
        self,
        coro: Awaitable[M],
        *,
        on_failure: Callable[[Exception], M] | None = None,
    ) -> None: ...

    @overload
    def pipe_to_self[T](
        self,
        coro: Awaitable[T],
        mapper: Callable[[T], M],
        on_failure: Callable[[Exception], M] | None = None,
    ) -> None: ...

    def pipe_to_self[T](
        self,
        coro: Awaitable[T],
        mapper: Callable[[T], M] | None = None,
        on_failure: Callable[[Exception], M] | None = None,
    ) -> None: ...
