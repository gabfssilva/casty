"""Singleton actor reference for Casty cluster.

This module provides SingletonRef, a reference to a cluster-wide unique actor
that automatically discovers the singleton's current location and routes
messages accordingly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable

if TYPE_CHECKING:
    from casty import LocalRef
    from casty.actor import CompositeRef


class SingletonRef[M]:
    """Reference to a singleton actor in the cluster.

    SingletonRef provides location-transparent access to a cluster-wide unique
    actor. The singleton may be hosted on any node, and this reference
    automatically discovers and routes to the current owner.

    If the singleton fails over to another node (due to node failure),
    messages are automatically routed to the new location.

    Type parameter M represents the message type(s) the singleton accepts.

    Example:
        # Spawn a singleton
        scheduler = await system.spawn(
            JobScheduler, name="scheduler", singleton=True
        )

        # Send message (fire-and-forget)
        await scheduler.send(ScheduleJob(job))

        # Ask with response
        status = await scheduler.ask(GetStatus())
    """

    __slots__ = ("_name", "_cluster", "_accepted_types")

    def __init__(
        self,
        name: str,
        cluster: "LocalRef[Any]",
        accepted_types: tuple[type, ...] = (),
    ) -> None:
        """Create a singleton actor reference.

        Args:
            name: Name of the singleton actor.
            cluster: Reference to the local Cluster actor for routing.
            accepted_types: Message types this actor accepts.
        """
        self._name = name
        self._cluster = cluster
        self._accepted_types = accepted_types

    @property
    def name(self) -> str:
        """Get the name of the singleton actor."""
        return self._name

    @property
    def accepted_types(self) -> tuple[type, ...]:
        """Get the message types this ref accepts."""
        return self._accepted_types

    def accepts(self, msg: Any) -> bool:
        """Check if this ref accepts the given message type."""
        if not self._accepted_types:
            return True
        return isinstance(msg, self._accepted_types)

    async def send(self, msg: M, *, sender: "LocalRef[Any] | None" = None) -> None:
        """Send a message to the singleton actor (fire-and-forget).

        The message is routed to whichever node currently owns the singleton.

        Args:
            msg: The message to send.
            sender: Optional sender reference (not used for singleton sends).
        """
        from .messages import RemoteSingletonSend

        await self._cluster.send(
            RemoteSingletonSend(singleton_name=self._name, payload=msg)
        )

    async def ask[R](
        self,
        msg: M,
        *,
        timeout: float = 5.0,
        sender: "LocalRef[Any] | None" = None,
    ) -> R:
        """Send a message and await response from the singleton actor.

        The message is routed to whichever node currently owns the singleton.

        Args:
            msg: The message to send.
            timeout: Maximum time to wait for response in seconds.
            sender: Optional sender reference (not used for singleton asks).

        Returns:
            The response from the singleton actor.

        Raises:
            asyncio.TimeoutError: If no response received within timeout.
            SingletonError: If the singleton operation fails.
        """
        from .messages import RemoteSingletonAsk

        return await self._cluster.ask(
            RemoteSingletonAsk(singleton_name=self._name, payload=msg),
            timeout=timeout,
        )

    def __rshift__(self, msg: M) -> Awaitable[None]:
        """Operator >> for send (tell pattern).

        Usage: await (singleton_ref >> msg)
        """
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        """Operator << for ask (request-response pattern).

        Usage: result = await (singleton_ref << msg)
        """
        return self.ask(msg)

    def __repr__(self) -> str:
        return f"SingletonRef({self._name})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SingletonRef):
            return self._name == other._name
        return False

    def __hash__(self) -> int:
        return hash(("singleton", self._name))

    def __or__[T](
        self, other: "LocalRef[T] | SingletonRef[T] | CompositeRef[T]"
    ) -> "CompositeRef[M | T]":
        """Combine refs with type-based routing."""
        from casty.actor import CompositeRef

        return CompositeRef.from_refs(self, other)


class SingletonError(Exception):
    """Error raised when a singleton actor operation fails."""

    def __init__(self, singleton_name: str, message: str) -> None:
        self.singleton_name = singleton_name
        super().__init__(f"Singleton error ({singleton_name}): {message}")
