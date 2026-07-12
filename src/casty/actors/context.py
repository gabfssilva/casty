from __future__ import annotations

import contextvars
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

if typing.TYPE_CHECKING:
    import asyncio

    from casty.actors.host import ActorHost, Reply, _Activation


@dataclass(frozen=True, slots=True)
class Schedule:
    """Handle to a scheduled self-call (spec 04 §10).

    Attributes
    ----------
    name : str
        The schedule's name; re-using it in `ctx.schedule` replaces this one.
    """

    name: str
    _task: asyncio.Task[None]

    def cancel(self) -> None:
        """Stop future ticks. Idempotent; a tick already enqueued still runs."""
        self._task.cancel()


class ActorContext:
    """Ambient context of the handler currently running (contextvar-based, so
    actor method signatures stay clean). Obtained with `casty.context()`.

    Attributes
    ----------
    actor_class : type
        The `@casty.actor` class of the running activation.
    key : str
        Key of the running activation.
    chain : list[str]
        The `"wire/key"` call chain that led here; how reentrancy is caught.
    """

    def __init__(
        self,
        host: ActorHost,
        activation: _Activation,
        actor_class: type,
        key: str,
        chain: list[str],
        reply: Reply | None = None,
    ) -> None:
        self._host = host
        self._activation = activation
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
        """Take ownership of the caller's reply.

        The host stops resolving the reply when the handler returns, and this
        activation stays alive until the returned `Reply` is set or failed.
        The basis of services.

        Returns
        -------
        Reply
            The caller's pending result; fire it exactly once.

        Raises
        ------
        RuntimeError
            If called outside a unary handler (streaming handlers own their
            sink already).
        """
        if self._reply is None:
            raise RuntimeError("detach() is only valid inside a unary handler")
        self.detached = True
        self._reply._activation.inflight += 1
        return self._reply

    def actor[T](self, cls: type[T], key: str) -> T:
        """Typed proxy to another actor, from inside a handler.

        The ask chain propagates via this context (the proxy reads it at call
        time), so cycles fail fast with `ReentrancyError`.

        Parameters
        ----------
        cls : type[T]
            A `@casty.actor` class.
        key : str
            The target actor's key.

        Returns
        -------
        T
            A proxy exposing the class's public async methods.
        """
        return self._host.router.actor(cls, key)

    def emit(
        self,
        fn: Callable[..., Awaitable[None]],
        /,
        *args: object,
        **kwargs: object,
    ) -> None:
        """Fire-and-forget message to this actor's own mailbox (spec 04 §10).

        Enqueues synchronously — FIFO position is claimed at the call, ahead
        of anything that arrives later — and returns immediately, with no
        reply. The message runs as its own handler after the current one:
        separate commit, separate supervision. It is dropped, not
        re-dispatched, if the activation closes first.

        This is the "send to self" of the classic actor model: `self._x()` is
        part of the current handler (same commit, same failure); `emit` is the
        next message. For a delayed or periodic send, use `schedule`.

        Parameters
        ----------
        fn : bound async method of this actor
            The message body. Use a `_`-prefixed method: delivery is local,
            and a public method would also be remotely callable.
        *args, **kwargs
            Passed to `fn`; held in memory, never serialized.
        """
        self._host.emit(self._activation, fn.__name__, list(args), dict(kwargs))

    def schedule(
        self,
        fn: Callable[..., Awaitable[None]],
        /,
        *args: object,
        after: float = 0.0,
        every: float | None = None,
        name: str | None = None,
        **kwargs: object,
    ) -> Schedule:
        """Deferred or periodic self-call, bound to this activation (spec 04 §10).

        The tick is delivered to the actor's own mailbox, so it runs like any
        handler: one at a time, committed if it mutated state, supervised if it
        raised. Periodic schedules are fixed-delay — the next tick arms only
        after the previous one completed. Every schedule is cancelled when the
        activation ends, whatever the reason; a schedule never keeps an actor
        alive (it does not block idle deactivation) and never reactivates one.

        Parameters
        ----------
        fn : bound async method of this actor
            The tick body. Use a `_`-prefixed method: ticks are local, and a
            public method would also be remotely callable.
        *args, **kwargs
            Passed to `fn` on every tick; held in memory, never serialized.
        after : float
            Delay before the first tick. `0.0` with `every` set means the
            first tick fires after `every`.
        every : float | None
            Fixed-delay period. `None` means one-shot: the schedule fires once
            and unregisters itself.
        name : str | None
            Defaults to `fn.__name__`. Re-using a name cancels and replaces
            the previous schedule.

        Returns
        -------
        Schedule
            Handle with an idempotent `cancel()`.
        """
        return self._host.start_schedule(
            self._activation,
            name if name is not None else fn.__name__,
            fn.__name__,
            list(args),
            dict(kwargs),
            after=after,
            every=every,
        )


_current: contextvars.ContextVar[ActorContext] = contextvars.ContextVar("casty_actor_context")


def current_context() -> ActorContext:
    """The context of the running handler. Exported as `casty.context`.

    Returns
    -------
    ActorContext
        Context of the handler executing on the current task.

    Raises
    ------
    LookupError
        If called outside an actor handler.
    """
    return _current.get()


def context_or_none() -> ActorContext | None:
    return _current.get(None)


def set_context(ctx: ActorContext) -> contextvars.Token[ActorContext]:
    return _current.set(ctx)


def reset_context(token: contextvars.Token[ActorContext]) -> None:
    _current.reset(token)
