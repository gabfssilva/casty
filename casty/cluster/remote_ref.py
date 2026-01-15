"""Remote actor reference for Casty cluster.

This module provides RemoteRef, a reference to an actor on a remote node
that transparently routes messages via TCP through the Cluster actor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable

if TYPE_CHECKING:
    from casty import LocalRef
    from casty.actor import CompositeRef
    from .messages import RemoteSend, RemoteAsk


class RemoteRef[M]:
    """Reference to an actor on a remote node.

    RemoteRef provides the same interface as ActorRef but routes messages
    through the Cluster actor to the remote node via TCP.

    Type parameter M represents the message type(s) the remote actor accepts.

    Example:
        # Get a remote reference
        ref = await cluster.ask(GetRemoteRef("worker-1", "node-2"))

        # Send message (fire-and-forget)
        await ref.send(ProcessData(data))

        # Ask with response
        result = await ref.ask(GetStatus())
    """

    __slots__ = ("_actor_name", "_node_id", "_cluster", "_accepted_types")

    def __init__(
        self,
        actor_name: str,
        node_id: str,
        cluster: "LocalRef[Any]",
        accepted_types: tuple[type, ...] = (),
    ) -> None:
        """Create a remote actor reference.

        Args:
            actor_name: Name of the remote actor.
            node_id: ID of the node hosting the actor.
            cluster: Reference to the local Cluster actor for routing.
            accepted_types: Message types this actor accepts.
        """
        self._actor_name = actor_name
        self._node_id = node_id
        self._cluster = cluster
        self._accepted_types = accepted_types

    @property
    def accepted_types(self) -> tuple[type, ...]:
        """Get the message types this ref accepts."""
        return self._accepted_types

    def accepts(self, msg: Any) -> bool:
        """Check if this ref accepts the given message type."""
        if not self._accepted_types:
            return True
        return isinstance(msg, self._accepted_types)

    @property
    def actor_name(self) -> str:
        """Get the name of the remote actor."""
        return self._actor_name

    @property
    def node_id(self) -> str:
        """Get the ID of the node hosting the actor."""
        return self._node_id

    async def send(self, msg: M, *, sender: "LocalRef[Any] | None" = None) -> None:
        """Send a message to the remote actor (fire-and-forget).

        Args:
            msg: The message to send.
            sender: Optional sender reference (not used for remote sends).
        """
        from .messages import RemoteSend

        await self._cluster.send(
            RemoteSend(
                target_actor=self._actor_name,
                target_node=self._node_id,
                payload=msg,
            )
        )

    async def ask[R](
        self,
        msg: M,
        *,
        timeout: float = 5.0,
        sender: "LocalRef[Any] | None" = None,
    ) -> R:
        """Send a message and await response from the remote actor.

        Args:
            msg: The message to send.
            timeout: Maximum time to wait for response in seconds.
            sender: Optional sender reference (not used for remote asks).

        Returns:
            The response from the remote actor.

        Raises:
            asyncio.TimeoutError: If no response received within timeout.
            RemoteActorError: If the remote actor raised an exception.
        """
        from .messages import RemoteAsk

        return await self._cluster.ask(
            RemoteAsk(
                target_actor=self._actor_name,
                target_node=self._node_id,
                payload=msg,
            ),
            timeout=timeout,
        )

    def __rshift__(self, msg: M) -> Awaitable[None]:
        """Operator >> for send (tell pattern).

        Usage: await (remote_ref >> msg)
        """
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        """Operator << for ask (request-response pattern).

        Usage: result = await (remote_ref << msg)
        """
        return self.ask(msg)

    def __repr__(self) -> str:
        return f"RemoteRef({self._actor_name}@{self._node_id})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RemoteRef):
            return (
                self._actor_name == other._actor_name
                and self._node_id == other._node_id
            )
        return False

    def __hash__(self) -> int:
        return hash((self._actor_name, self._node_id))

    def __or__[T](
        self, other: "LocalRef[T] | RemoteRef[T] | CompositeRef[T]"
    ) -> "CompositeRef[M | T]":
        """Combine refs with type-based routing."""
        from casty.actor import CompositeRef

        return CompositeRef.from_refs(self, other)


class RemoteActorError(Exception):
    """Error raised when a remote actor operation fails."""

    def __init__(self, actor_name: str, node_id: str, message: str) -> None:
        self.actor_name = actor_name
        self.node_id = node_id
        super().__init__(f"Remote actor error ({actor_name}@{node_id}): {message}")
