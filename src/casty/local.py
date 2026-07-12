from __future__ import annotations

import typing
import uuid
from collections.abc import AsyncIterator

from casty.actors import inspection
from casty.actors import registry as actor_registry
from casty.actors.host import ActorHost
from casty.actors.registry import ActorInfo, MethodInfo, Supervisor
from casty.actors.streaming import local_stream_in, local_stream_out
from casty.system import ActorSystem


class Local(ActorSystem):
    """An in-process actor system: no server, no membership, no replication, no
    socket. Actors run exactly as they would on the node that owns them — args
    reach handlers as Python objects, unserialized — minus placement and
    durability."""

    def __init__(
        self,
        *,
        supervisor: Supervisor | None,
        default_idle_timeout: float,
        drain_timeout: float,
        interceptor: inspection.Interceptor | None = None,
    ) -> None:
        self._drain_timeout = drain_timeout
        self._hub = inspection.Hub(node=uuid.uuid4())
        self._host = ActorHost(
            router=self,
            replication=None,
            supervisor=supervisor,
            default_idle_timeout=default_idle_timeout,
            hub=self._hub,
            interceptor=interceptor,
        )

    async def _call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object:
        return await self._host.dispatch(info, key, method.name, args, chain)

    def _stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]:
        return local_stream_out(self._host, info, key, method, kwargs, in_iter, chain)

    async def _stream_in(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> object:
        return await local_stream_in(self._host, info, key, method, kwargs, in_iter, chain)

    def spy(
        self,
        actor: type | str | None = None,
        *,
        key: str = "*",
        scope: typing.Literal["local", "cluster"] = "local",
        payloads: bool = False,
    ) -> inspection.Subscription:
        # local == everything here; the scope flag never matters (spec 11 §2)
        return inspection.LocalSubscription(
            self._hub, inspection.selector_of(actor, key, payloads)
        )

    async def state_of(self, actor: type, key: str) -> dict[str, object] | None:
        info = actor_registry.info_of(actor)
        if info is None or info.kind != "actor":
            raise TypeError(f"{actor.__qualname__} is not a @casty.actor")
        return self._host.state_values(info.wire_name, key)

    async def close(self) -> None:
        """Drain in-flight handlers and run deactivate hooks. Calls after this
        raise ActorUnavailableError."""
        await self._host.stop(self._drain_timeout)
        self._hub.close()


def local(
    *,
    supervisor: Supervisor | None = None,
    default_idle_timeout: float = 300.0,
    drain_timeout: float = 10.0,
    interceptor: inspection.Interceptor | None = None,
) -> ActorSystem:
    """Start an in-process actor system: no networking, no cluster.

    `replicas > 1` actors (including `map()` shards) run single-copy — no
    quorum, no snapshot durability — so idle deactivation loses their state,
    exactly as a `replicas=1` actor does anywhere. This is not a one-node
    cluster: a real cluster of one would fence replicated writes with
    QuorumUnavailableError instead of degrading them.

    Parameters
    ----------
    supervisor : Supervisor | None
        Global failure policy for actors (overridable per class).
    default_idle_timeout : float
        Seconds without messages before an activation deactivates, for actor
        classes that don't set their own `idle_timeout`.
    drain_timeout : float
        Seconds `close()` waits for in-flight handlers before giving up.
    interceptor : Interceptor | None
        Sync hook called inline with every spy event (spec 11); return fast,
        never block, never raise.

    Returns
    -------
    ActorSystem
        Async context manager; `close()` drains handlers and runs deactivate
        hooks.

    Examples
    --------
    >>> async with casty.local() as system:
    ...     counter = system.actor(Counter, "page-views")
    ...     await counter.increment()
    """
    return Local(
        supervisor=supervisor,
        default_idle_timeout=default_idle_timeout,
        drain_timeout=drain_timeout,
        interceptor=interceptor,
    )
