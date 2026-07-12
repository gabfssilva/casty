from __future__ import annotations

from collections.abc import AsyncIterator

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
    ) -> None:
        self._drain_timeout = drain_timeout
        self._host = ActorHost(
            router=self,
            replication=None,
            supervisor=supervisor,
            default_idle_timeout=default_idle_timeout,
        )

    async def call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object:
        return await self._host.dispatch(info, key, method.name, args, chain)

    def stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]:
        return local_stream_out(self._host, info, key, method, kwargs, in_iter, chain)

    async def stream_in(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> object:
        return await local_stream_in(self._host, info, key, method, kwargs, in_iter, chain)

    async def close(self) -> None:
        """Drain in-flight handlers and run deactivate hooks. Calls after this
        raise ActorUnavailableError."""
        await self._host.stop(self._drain_timeout)


def local(
    *,
    supervisor: Supervisor | None = None,
    default_idle_timeout: float = 300.0,
    drain_timeout: float = 10.0,
) -> ActorSystem:
    """Start an in-process actor system: no networking, no cluster.

    `replicas > 1` actors (including `map()` shards) run single-copy — no
    quorum, no snapshot durability — so idle deactivation loses their state,
    exactly as a `replicas=1` actor does anywhere. This is not a one-node
    cluster: a real cluster of one would fence replicated writes with
    QuorumUnavailableError instead of degrading them.
    """
    return Local(
        supervisor=supervisor,
        default_idle_timeout=default_idle_timeout,
        drain_timeout=drain_timeout,
    )
