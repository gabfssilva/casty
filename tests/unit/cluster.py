"""An in-process cluster of Replication instances calling each other directly: no
sockets, so every replicated byte is countable. Shared by the paged-state tests."""

from __future__ import annotations

import asyncio
import uuid

import casty
from casty.actors import replication as rep
from casty.actors.host import ActorHost
from casty.actors.registry import ActorInfo
from casty.errors import ConnectionLostError
from casty.placement.ring import Ring
from casty.serde import codec

BATCH = 256 * 1024  # Config.replication_batch_bytes: the size every message aims under
MAX_MESSAGE = 4 * 1024 * 1024


class Cluster:
    """N nodes on a real ring, wired to each other by an in-process pool."""

    def __init__(self, size: int) -> None:
        self.ring = Ring.build([uuid.uuid4() for _ in range(size)])
        self.down: set[uuid.UUID] = set()
        self.replicated = 0  # bytes of REPLICATE bodies
        self.messages = 0  # REPLICATE messages
        self.fetched = 0  # bytes of page data in fetch replies
        self.scans = 0  # FETCH_STATE requests
        self.widest = 0  # biggest body on the wire, either direction
        self.nacks: list[int] = []
        self.nodes = {node_id: Node(node_id, self) for node_id in self.ring.nodes}

    def owner(self, info: ActorInfo, key: str) -> Node:
        return self.nodes[self.ring.owner(f"{info.wire_name}/{key}")]

    def replicas(self, info: ActorInfo, key: str) -> list[Node]:
        token = f"{info.wire_name}/{key}"
        return [self.nodes[n] for n in self.ring.replicas(token, info.replicas)]

    def outsider(self, info: ActorInfo, key: str) -> Node:
        """A node the ring never places this actor on: it holds none of its pages."""
        holders = {node.node_id for node in self.replicas(info, key)}
        return next(node for node in self.nodes.values() if node.node_id not in holders)

    def reset(self) -> None:
        self.replicated = self.messages = self.fetched = self.scans = self.widest = 0
        self.nacks.clear()

    async def settle(self) -> None:
        """Let the background read-repair tasks finish (in process, they only ever wait
        on the event loop)."""
        for _ in range(20):
            await asyncio.sleep(0)


class Node:
    def __init__(self, node_id: uuid.UUID, cluster: Cluster) -> None:
        self.node_id = node_id
        self.cluster = cluster
        self.replication = rep.Replication(
            node_id=node_id,
            pool=_Pool(cluster),
            view=self,
            replication_timeout=1.0,
            handoff_timeout=1.0,
            batch_bytes=BATCH,
            max_message_bytes=MAX_MESSAGE,
        )
        self.host = self.fresh_host()

    def fresh_host(self) -> ActorHost:
        """A host with no activations, over the same replica store: what this node would
        come back as after a deactivation."""
        self.host = ActorHost(
            router=self, replication=self.replication, reactivation_retry=0.05
        )
        return self.host

    async def call(self, info: ActorInfo, key: str, method: str, *args: object) -> object:
        return await self.host.dispatch(info, key, method, list(args), [])

    def pages(self, info: ActorInfo, key: str) -> dict[str, bytes]:
        stored = self.replication.store.get(info.wire_name, key)
        return {} if stored is None else {k: p.data for k, p in stored.pages.items()}

    def page_hlc(self, info: ActorInfo, key: str, page: str) -> rep.Hlc:
        stored = self.replication.store.get(info.wire_name, key)
        assert stored is not None
        return stored.pages[page].hlc

    # --- RingView ---
    @property
    def ring(self) -> Ring:
        return self.cluster.ring

    def addr_of(self, node_id: uuid.UUID) -> str:
        return str(node_id)

    # --- Router ---
    def actor[T](self, cls: type[T], key: str) -> T:
        raise NotImplementedError


class _Pool:
    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster

    async def get(self, addr: str) -> _Conn:
        node = self._cluster.nodes[uuid.UUID(addr)]
        if node.node_id in self._cluster.down:
            raise ConnectionLostError(f"{addr} is unreachable")
        return _Conn(self._cluster, node)


class _Conn:
    def __init__(self, cluster: Cluster, target: Node) -> None:
        self._cluster = cluster
        self._target = target

    async def ask(
        self, msg_type: int, body: bytes, *, timeout: float | None = None, replication: bool = False
    ) -> bytes:
        cluster = self._cluster
        if self._target.node_id in cluster.down:
            raise ConnectionLostError(f"{self._target.node_id} is unreachable")
        handler = self._target.replication
        match codec.decode(body):
            case rep.Replicate() as write:
                cluster.replicated += len(body)
                cluster.messages += 1
                cluster.widest = max(cluster.widest, len(body))
                try:
                    handler.handle_replicate(write)
                except casty.RemoteError as exc:
                    cluster.nacks.append(exc.code)
                    raise
                return b""
            case rep.FetchState() as fetch:
                state = handler.handle_fetch(fetch)
                cluster.scans += 1
                cluster.fetched += sum(len(page.data) for page in state.pages)
                return self._sized(codec.encode(state))
            case rep.FetchPages() as fetch_pages:
                pages = handler.handle_fetch_pages(fetch_pages)
                cluster.fetched += sum(len(page.data) for page in pages.pages)
                return self._sized(codec.encode(pages))
            case request:
                raise AssertionError(f"unexpected request {request!r}")

    def _sized(self, body: bytes) -> bytes:
        self._cluster.widest = max(self._cluster.widest, len(body))
        return body
