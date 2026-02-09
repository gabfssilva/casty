"""Actor context protocol defining capabilities available inside a behavior.

The ``ActorContext[M]`` protocol is the interface passed to every behavior
handler, providing access to the actor's own ref, child spawning, logging,
and death-watch.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from casty.actor import Behavior
    from casty.mailbox import Mailbox
    from casty.ref import ActorRef


class ActorContext[M](Protocol):
    """Protocol for the context available to actor behavior handlers.

    Every behavior's ``receive`` function receives an ``ActorContext`` as
    its first argument. It provides the actor's own reference, child
    management, logging, and death-watch capabilities.

    Examples
    --------
    >>> async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
    ...     ctx.log.info("Got %s", msg)
    ...     child = ctx.spawn(other_behavior, "child")
    ...     return Behaviors.same()
    """

    @property
    def self(self) -> ActorRef[M]:
        """The actor's own reference.

        Use this to give other actors a handle to send messages back.

        Returns
        -------
        ActorRef[M]
            A typed reference to this actor.

        Examples
        --------
        >>> other.tell(Subscribe(reply_to=ctx.self))
        """
        ...

    @property
    def log(self) -> logging.Logger:
        """A logger scoped to this actor's path.

        Returns
        -------
        logging.Logger
            Logger instance for this actor.

        Examples
        --------
        >>> ctx.log.info("Processing message: %s", msg)
        """
        ...

    def spawn[C](
        self,
        behavior: Behavior[C],
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]:
        """Spawn a child actor under this actor's supervision.

        Parameters
        ----------
        behavior : Behavior[C]
            The initial behavior of the child actor.
        name : str
            The child's name, unique among siblings.
        mailbox : Mailbox[C] | None
            Custom mailbox configuration. Uses system defaults if ``None``.

        Returns
        -------
        ActorRef[C]
            A typed reference to the newly spawned child.

        Examples
        --------
        >>> worker = ctx.spawn(worker_behavior(), "worker-1")
        >>> worker.tell(Task(payload="data"))
        """
        ...

    def stop(self, ref: ActorRef[Any]) -> None:
        """Stop a child actor.

        Parameters
        ----------
        ref : ActorRef[Any]
            Reference to the child actor to stop.

        Examples
        --------
        >>> ctx.stop(child_ref)
        """
        ...

    def watch(self, ref: ActorRef[Any]) -> None:
        """Watch an actor for termination.

        When the watched actor stops, this actor receives a
        ``Terminated`` signal.

        Parameters
        ----------
        ref : ActorRef[Any]
            Reference to the actor to watch.

        Examples
        --------
        >>> ctx.watch(important_worker)
        """
        ...

    def unwatch(self, ref: ActorRef[Any]) -> None:
        """Stop watching an actor for termination.

        Parameters
        ----------
        ref : ActorRef[Any]
            Reference to the actor to stop watching.

        Examples
        --------
        >>> ctx.unwatch(worker_ref)
        """
        ...
