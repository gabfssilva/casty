"""Detached async operations for actors.

Allows actors to remain responsive while long-running async operations
execute in the background. Results are sent directly back to the actor
as messages.
"""

from collections.abc import Coroutine
from asyncio import Task
from typing import Any, TYPE_CHECKING, Callable
import asyncio
import logging

if TYPE_CHECKING:
    from casty import Context
    from casty.ref import ActorRef

log = logging.getLogger(__name__)


class Detached[R]:
    """A detached async operation that sends its result back to an actor.

    Usage:
        # In actor receive():
        ctx.detach(slow_operation())  # Result sent back as message

        # With transformation:
        ctx.detach(fetch_data()).then(lambda data: DataReady(data))

    The result (or transformed result) is sent directly to the actor's
    mailbox when the operation completes. Exceptions are logged but
    not sent.
    """

    task: Task[R]
    _transform: Callable[[R], Any] | type | None
    _bound: bool

    def __init__(self, coro: Coroutine[Any, Any, R]) -> None:
        self.task = asyncio.create_task(coro)
        self._transform = None
        self._bound = False

    def then[T](self, transform: Callable[[R], T] | type[T]) -> "Detached[R]":
        """Transform the result before sending.

        Args:
            transform: Function or class to transform result into a message

        Example:
            ctx.detach(fetch_user(id)).then(UserLoaded)  # Class as constructor
            ctx.detach(fetch_data()).then(lambda d: Ready(d))  # Function
        """
        self._transform = transform
        return self

    def discard(self) -> "Detached[R]":
        """Discard the result - fire-and-forget.

        Example:
            ctx.detach(cleanup_task()).discard()
        """
        return self.bind(None)

    def bind(self, receiver: "ActorRef[R] | Context[R] | None") -> "Detached[R]":
        """Bind this detached operation to send results to receiver.

        Args:
            receiver: Where to send results. If None, results are discarded (fire-and-forget).

        Called automatically by ctx.detach(). You normally don't need
        to call this directly.
        """
        if self._bound:
            return self

        # If receiver is None, fire-and-forget (no callback needed)
        if receiver is None:
            self._bound = True
            return self

        from casty import Context
        match receiver:
            case Context():
                replier = receiver.self_ref
            case _:
                replier = receiver

        def cb(task: Task[R]) -> None:
            try:
                result = task.result()
                # Apply transformation if set
                msg = self._transform(result) if self._transform else result
                asyncio.create_task(replier.send(msg))
            except asyncio.CancelledError:
                # Task was cancelled - don't send anything
                log.debug("Detached task cancelled")
            except Exception as e:
                # Log error but don't crash the actor
                log.warning(f"Detached task failed: {e}")

        self.task.add_done_callback(cb)
        self._bound = True
        return self
