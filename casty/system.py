"""Actor system runtime for Casty - local only."""

from __future__ import annotations

import asyncio
import logging
from asyncio import Queue, Task
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .actor import Actor, ActorId, ActorRef, Context, Envelope

if TYPE_CHECKING:
    from .cluster.distributed import DistributedActorSystem

log = logging.getLogger(__name__)


class ActorSystem:
    """Runtime that manages local actors."""

    def __init__(self) -> None:
        self._actors: dict[ActorId, Task[None]] = {}
        self._running = True

    @staticmethod
    def distributed(
        host: str,
        port: int,
        seeds: list[str] | None = None,
    ) -> "DistributedActorSystem":
        """Factory method to create a distributed actor system.

        Args:
            host: Host to bind the server to
            port: Port to bind the server to
            seeds: List of seed nodes in "host:port" format

        Returns:
            A DistributedActorSystem instance
        """
        from .cluster.distributed import DistributedActorSystem

        return DistributedActorSystem(host, port, seeds)

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        **kwargs: Any,
    ) -> ActorRef[M]:
        """Create and start a new actor."""
        actor = actor_cls(**kwargs)
        actor_id = ActorId(uid=uuid4(), name=name)
        mailbox: Queue[Envelope[M]] = Queue()
        ref: ActorRef[M] = ActorRef(actor_id, mailbox, self)
        ctx: Context[M] = Context(self_ref=ref, system=self)
        actor._ctx = ctx

        async def run_actor() -> None:
            await actor.on_start()
            try:
                while self._running:
                    try:
                        envelope = await asyncio.wait_for(mailbox.get(), timeout=0.1)
                    except asyncio.TimeoutError:
                        continue

                    ctx._current_envelope = envelope
                    try:
                        await actor.receive(envelope.payload, ctx)
                    except Exception as exc:
                        log.exception(f"Actor {actor_id} failed processing message")
                        if not actor.on_error(exc):
                            return
                    finally:
                        ctx._current_envelope = None
            except asyncio.CancelledError:
                pass
            finally:
                await actor.on_stop()
                self._actors.pop(actor_id, None)

        task = asyncio.create_task(run_actor())
        self._actors[actor_id] = task
        return ref

    async def shutdown(self) -> None:
        """Stop all actors gracefully.

        Sets _running to False and waits for actors to finish.
        Safe to call from within an actor.
        """
        self._running = False
        current = asyncio.current_task()
        others = [t for t in self._actors.values() if t is not current]
        if others:
            await asyncio.gather(*others, return_exceptions=True)

    async def __aenter__(self) -> "ActorSystem":
        """Enter async context manager."""
        return self

    async def __aexit__(self, _exc_type: type | None, _exc_val: Exception | None, _exc_tb: object) -> None:
        """Exit async context manager, ensuring graceful shutdown."""
        await self.shutdown()
