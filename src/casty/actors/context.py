from __future__ import annotations

import contextvars
import typing

if typing.TYPE_CHECKING:
    from casty.actors.host import ActorHost, Reply


class ActorContext:
    """Ambient context of the handler currently running (contextvar-based, so
    actor method signatures stay clean)."""

    def __init__(
        self,
        host: ActorHost,
        actor_class: type,
        key: str,
        chain: list[str],
        reply: Reply | None = None,
    ) -> None:
        self._host = host
        self._reply = reply
        self.actor_class = actor_class
        self.key = key
        self.chain = chain
        self.deactivation_requested = False
        self.detached = False

    def deactivate(self) -> None:
        """Deactivate this actor after the current handler returns."""
        self.deactivation_requested = True

    def detach(self) -> Reply:
        """Take ownership of the caller's reply: the host stops resolving it when
        the handler returns, and this activation stays alive until the returned
        `Reply` is set or failed. The basis of services (spec 08)."""
        if self._reply is None:
            raise RuntimeError("detach() is only valid inside a unary handler")
        self.detached = True
        self._reply._activation.inflight += 1
        return self._reply

    def actor[T](self, cls: type[T], key: str) -> T:
        """Proxy to another actor. The ask chain propagates via this context
        (the proxy reads it at call time), so cycles fail fast."""
        return self._host.router.actor(cls, key)


_current: contextvars.ContextVar[ActorContext] = contextvars.ContextVar("casty_actor_context")


def current_context() -> ActorContext:
    """The context of the running handler. Raises LookupError outside one."""
    return _current.get()


def context_or_none() -> ActorContext | None:
    return _current.get(None)


def set_context(ctx: ActorContext) -> contextvars.Token[ActorContext]:
    return _current.set(ctx)


def reset_context(token: contextvars.Token[ActorContext]) -> None:
    _current.reset(token)
