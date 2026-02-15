"""Spy behavior — observes all messages an actor processes.

Wraps a behavior's handler to emit ``SpyEvent`` to an observer after
each message. Optionally intercepts child spawns to spy recursively.
Built entirely from ``Behavior.receive`` and ``Behavior.setup``.
"""

from __future__ import annotations

import time as _time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.core.behavior import Behavior

if TYPE_CHECKING:
    from casty.core.context import ActorContext
    from casty.core.mailbox import Mailbox
    from casty.core.ref import ActorRef


@dataclass(frozen=True)
class SpyEvent[M]:
    actor_path: str
    event: M
    timestamp: float


class SpyContext[M]:
    """ActorContext wrapper that intercepts spawn to spy on children."""

    def __init__(
        self,
        inner: ActorContext[M],
        observer: ActorRef[SpyEvent[Any]],
    ) -> None:
        self._inner = inner
        self._observer = observer

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    @property
    def self(self) -> ActorRef[M]:
        return self._inner.self

    @property
    def system(self) -> Any:
        return self._inner.system

    @property
    def log(self) -> Any:
        return self._inner.log

    def spawn[C](
        self,
        behavior: Behavior[C],
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]:
        wrapped = spy(behavior, self._observer, spy_children=True)
        return self._inner.spawn(wrapped, name, mailbox=mailbox)

    def stop(self, ref: ActorRef[Any]) -> None:
        self._inner.stop(ref)

    def watch(self, ref: ActorRef[Any]) -> None:
        self._inner.watch(ref)

    def unwatch(self, ref: ActorRef[Any]) -> None:
        self._inner.unwatch(ref)

    def pipe_to_self(self, *args: Any, **kwargs: Any) -> None:
        self._inner.pipe_to_self(*args, **kwargs)

    def register_interceptor(self, interceptor: Callable[[object], bool]) -> None:
        self._inner.register_interceptor(interceptor)



def spy[M](
    behavior: Behavior[M],
    observer: ActorRef[SpyEvent[M]],
    *,
    spy_children: bool = False,
) -> Behavior[M]:
    """Wrap a behavior to report every processed message to an observer."""

    async def setup(ctx: ActorContext[M]) -> Behavior[M]:
        effective_ctx: ActorContext[M] = SpyContext(ctx, observer) if spy_children else ctx

        inner = behavior
        if inner.on_setup is not None:
            inner = await inner.on_setup(effective_ctx)

        original = inner.on_receive
        if original is None:
            return inner

        def spied(handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]]) -> Behavior[M]:
            async def receive(_ctx: ActorContext[M], msg: M) -> Behavior[M]:
                result = await handler(effective_ctx, msg)
                observer.tell(SpyEvent(
                    actor_path=effective_ctx.self.id,
                    event=msg,
                    timestamp=_time.monotonic(),
                ))

                if result.on_receive is not None:
                    return spied(result.on_receive)

                return result

            return Behavior.receive(receive)

        return spied(original)

    return Behavior.setup(setup)
