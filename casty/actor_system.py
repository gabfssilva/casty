"""ActorSystem decorator with factory methods."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .cluster.scope import Scope
from .protocols import ActorRef, System

if TYPE_CHECKING:
    from .actor import Actor
    from .supervision import SupervisorConfig


class ActorSystem(System):
    """Decorator over System with factory methods.

    Decorator Pattern: wraps a System implementation,
    delegating all operations while providing convenient
    factories to create local or clustered systems.

    Usage:
        async with ActorSystem.local() as system:
            ref = await system.actor(MyActor, name='my-actor')

        async with ActorSystem.clustered(port=8001) as system:
            ref = await system.actor(MyActor, name='my-actor', scope='cluster')
    """

    def __init__(self, inner: System | None = None) -> None:
        self._inner = inner or ActorSystem.local()

    @classmethod
    def local(cls) -> "ActorSystem":
        """Create a local actor system."""
        from .system import LocalSystem
        return cls(LocalSystem())

    @classmethod
    def clustered(
        cls,
        host: str = "0.0.0.0",
        port: int = 0,
        seeds: list[str] | None = None,
        *,
        node_id: str | None = None,
        advertise_host: str | None = None,
        advertise_port: int | None = None,
    ) -> "ActorSystem":
        """Create a clustered actor system."""
        from .cluster.clustered_system import ClusteredSystem
        from .cluster.config import ClusterConfig

        config = ClusterConfig(
            bind_host=host,
            bind_port=port,
            advertise_host=advertise_host,
            advertise_port=advertise_port,
            node_id=node_id,
            seeds=seeds or [],
        )

        return cls(ClusteredSystem(config))

    # Delegation methods

    async def actor[M](
        self,
        actor_cls: type["Actor[M]"],
        *,
        name: str,
        scope: Scope = 'local',
        supervision: "SupervisorConfig | None" = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> ActorRef[M]:
        """Create and start a new actor."""
        return await self._inner.actor(
            actor_cls,
            name=name,
            scope=scope,
            supervision=supervision,
            durable=durable,
            **kwargs,
        )

    async def spawn[M](
        self,
        actor_cls: type["Actor[M]"],
        *,
        name: str | None = None,
        **kwargs: Any,
    ) -> ActorRef[M]:
        """Internal spawn method for ephemeral actors.

        Delegates to the inner system's spawn method.
        For named actors, use actor() instead.
        """
        return await self._inner.spawn(actor_cls, name=name, **kwargs)

    async def stop(self, ref: ActorRef[Any]) -> bool:
        """Stop an actor by its reference."""
        return await self._inner.stop(ref)

    async def schedule[R](
        self,
        timeout: float,
        listener: ActorRef[R],
        message: R,
    ) -> str | None:
        """Schedule a message to be sent after timeout."""
        return await self._inner.schedule(timeout, listener, message)

    async def cancel_schedule(self, task_id: str) -> None:
        """Cancel a scheduled message."""
        await self._inner.cancel_schedule(task_id)

    async def tick[R](
        self,
        message: R,
        interval: float,
        listener: ActorRef[R],
    ) -> str | None:
        """Start periodic message delivery."""
        return await self._inner.tick(message, interval, listener)

    async def cancel_tick(self, subscription_id: str) -> None:
        """Cancel periodic message delivery."""
        await self._inner.cancel_tick(subscription_id)

    async def shutdown(self) -> None:
        """Stop all actors gracefully."""
        await self._inner.shutdown()

    async def start(self) -> None:
        """Start the system."""
        await self._inner.start()

    async def __aenter__(self) -> "ActorSystem":
        """Enter async context manager."""
        await self._inner.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit async context manager."""
        await self._inner.shutdown()
