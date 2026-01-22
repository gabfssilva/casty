from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine

from casty.system import LocalActorSystem
from casty.actor import Behavior
from casty.mailbox import Filter
from casty.protocols import System
from casty.ref import ActorRef
from casty.remote import Listening, Lookup

from .cluster import cluster, CreateActor, GetClusterAddress
from .constants import REMOTE_ACTOR_ID, MEMBERSHIP_ACTOR_ID


class ClusteredActorSystem(System):
    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        seeds: list[tuple[str, str]] | None = None,
        debug_filter: Any = None,
    ) -> None:
        self._system = LocalActorSystem(node_id=node_id, debug_filter=debug_filter)
        self._node_id = node_id
        self._host = host
        self._port = port
        self._seeds = seeds or []
        self._cluster_ref: ActorRef | None = None
        self._address: str | None = None

    @property
    def node_id(self) -> str:
        return self._node_id

    async def address(self) -> str:
        if self._address:
            return self._address
        return f"{self._host}:{self._port}"

    async def start(self) -> None:
        self._cluster_ref = await self._system.actor(
            cluster(self._node_id, self._host, self._port, self._seeds),
            name="cluster"
        )

        # Wait for remote to be ready
        while True:
            remote_ref = await self._system.actor(name=REMOTE_ACTOR_ID)
            if remote_ref:
                break
            await asyncio.sleep(0.01)

        # Get the actual bound address from the cluster
        self._address = await self._cluster_ref.ask(GetClusterAddress())

    async def actor[M](
        self,
        behavior: Behavior | None = None,
        *,
        name: str,
        filters: list[Filter] | None = None,
        node_id: str | None = None,
        replicas: int | None = None,
        write_quorum: int | None = None,
    ) -> ActorRef[M] | None:
        if behavior is None:
            if node_id:
                return await self._lookup_remote(name, node_id)
            return await self._system.actor(name=name)

        if self._cluster_ref is None:
            raise RuntimeError("ClusteredActorSystem not started")

        return await self._cluster_ref.ask(CreateActor(behavior, name))

    async def _lookup_remote[M](self, name: str, node_id: str) -> ActorRef[M] | None:
        from .messages import GetAddress

        membership_ref = await self._system.actor(name=MEMBERSHIP_ACTOR_ID)
        if not membership_ref:
            return None

        address = await membership_ref.ask(GetAddress(node_id))
        if not address:
            return None

        remote_ref = await self._system.actor(name=REMOTE_ACTOR_ID)
        if not remote_ref:
            return None

        result = await remote_ref.ask(Lookup(name, peer=address))
        return result.ref if result else None

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg: M,
        timeout: float = 30.0,
    ) -> R:
        return await self._system.ask(ref, msg, timeout)

    async def schedule[M](
        self,
        msg: M,
        *,
        to: ActorRef[M] | None = None,
        delay: float | None = None,
        every: float | None = None,
        sender: ActorRef | None = None,
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        return await self._system.schedule(msg, to=to, delay=delay, every=every, sender=sender)

    async def shutdown(self) -> None:
        await self._system.shutdown()

    async def __aenter__(self) -> "ClusteredActorSystem":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
