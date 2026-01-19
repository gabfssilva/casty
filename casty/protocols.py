"""Protocols for Casty actor system."""

from __future__ import annotations

from typing import Any, Awaitable, Protocol, runtime_checkable, TYPE_CHECKING, Self

if TYPE_CHECKING:
    from .actor import Actor
    from .cluster.scope import Scope
    from .supervision import SupervisorConfig


@runtime_checkable
class ActorRef[M](Protocol):
    """Protocol for actor references.

    This protocol defines the interface that all actor references must implement.
    Both LocalRef and ClusteredRef implement this protocol.
    """

    async def send(self, msg: M, **kwargs: Any) -> None:
        """Send message (fire-and-forget).

        Args:
            msg: The message to send
            **kwargs: Additional options (e.g., sender, consistency)
        """
        ...

    async def ask[R](self, msg: M, *, timeout: float = 5.0) -> R:
        """Send message and await response.

        Args:
            msg: The message to send
            timeout: Maximum time to wait for response in seconds

        Returns:
            The response from the actor
        """
        ...

    def __rshift__(self, msg: M) -> Awaitable[None]:
        """Operator >> for send (tell pattern).

        Usage: await (actor >> msg)
        """
        ...

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        """Operator << for ask (request-response pattern).

        Usage: result = await (actor << msg)
        """
        ...


@runtime_checkable
class System(Protocol):
    """Protocol for actor systems.

    This protocol defines the interface that all actor systems must implement.
    Both ActorSystem (local) and ClusteredActorSystem implement this protocol.
    """

    async def actor[M](
        self,
        actor_cls: type["Actor[M]"],
        *,
        name: str,
        scope: "Scope" = 'local',
        supervision: "SupervisorConfig | None" = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> ActorRef[M]:
        """Get or create an actor by name.

        Args:
            actor_cls: The actor class to instantiate
            name: Required name for the actor (part of identity)
            scope: 'local', 'cluster', or ClusterScope for distributed actors
            supervision: Override supervision configuration
            durable: If True, persist actor state with WAL
            **kwargs: Constructor arguments for the actor

        Returns:
            Reference to the actor (existing or newly created)
        """
        ...

    async def spawn[M](
        self,
        actor_cls: type["Actor[M]"],
        *,
        name: str | None = None,
        **kwargs: Any,
    ) -> ActorRef[M]:
        """Internal spawn method for ephemeral actors.

        Generates a unique name automatically if not provided.
        Used by LocalActorRef.ask(). For named actors, use actor() instead.

        Args:
            actor_cls: The actor class to instantiate
            name: Optional name for the actor (auto-generated if not provided)
            **kwargs: Constructor arguments for the actor

        Returns:
            Reference to the created actor
        """
        ...

    async def stop(self, ref: ActorRef[Any]) -> bool:
        """Stop an actor by its reference.

        Args:
            ref: Reference to the actor to stop

        Returns:
            True if actor was found and stopped, False otherwise
        """
        ...

    async def schedule[R](
        self,
        timeout: float,
        listener: ActorRef[R],
        message: R,
    ) -> str | None:
        """Schedule a message to be sent after timeout.

        Args:
            timeout: Delay in seconds
            listener: The actor to receive the message
            message: The message to send

        Returns:
            Task ID for cancellation, or None if system is shutting down
        """
        ...

    async def cancel_schedule(self, task_id: str) -> None:
        """Cancel a scheduled message by task_id.

        Args:
            task_id: The task ID returned by schedule()
        """
        ...

    async def tick[R](
        self,
        message: R,
        interval: float,
        listener: ActorRef[R],
    ) -> str | None:
        """Start periodic message delivery.

        Args:
            message: The message to send periodically
            interval: Time between messages in seconds
            listener: The actor to receive the messages

        Returns:
            Subscription ID for cancellation, or None if system is shutting down
        """
        ...

    async def cancel_tick(self, subscription_id: str) -> None:
        """Cancel periodic message delivery by subscription_id.

        Args:
            subscription_id: The subscription ID returned by tick()
        """
        ...

    async def shutdown(self) -> None:
        """Stop all actors gracefully."""
        ...

    async def start(self) -> None:
        """Start the system."""
        ...

    async def __aenter__(self) -> Self:
        """Enter async context manager."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit async context manager."""
        ...
