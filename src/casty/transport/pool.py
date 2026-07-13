from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import Callable

from casty.config import TLS, TransportConfig
from casty.errors import CastyError, ConnectionLostError
from casty.transport.connection import (
    ActorStreamHandler,
    BulkHandler,
    CloseHandler,
    Connection,
    ControlHandler,
    LocalNode,
    RequestHandler,
)


class Pool:
    """Outbound connection pool: one connection per peer, opened on demand,
    reconnection with exponential backoff and jitter.

    Duplicate connections to the same node (simultaneous dial from both sides)
    are resolved deterministically: the connection whose *initiator* has the
    larger node_id is closed.

    `address_map` rewrites an announced address into the one actually dialed
    (an SSH tunnel, a NAT hop). Connections stay keyed by the announced
    address — the map applies only at the socket."""

    def __init__(
        self,
        *,
        local: LocalNode,
        config: TransportConfig | None = None,
        tls: TLS | None = None,
        on_request: RequestHandler | None = None,
        on_control: ControlHandler | None = None,
        on_bulk: BulkHandler | None = None,
        on_actor_stream: ActorStreamHandler | None = None,
        on_close: CloseHandler | None = None,
        address_map: Callable[[str], str] | None = None,
    ) -> None:
        self._local = local
        self._config = config or TransportConfig()
        self._tls = tls
        self._on_request = on_request
        self._on_control = on_control
        self._on_bulk = on_bulk
        self._on_actor_stream = on_actor_stream
        self._on_close = on_close
        self._address_map = address_map
        self._by_addr: dict[str, Connection] = {}
        self._by_node: dict[uuid.UUID, Connection] = {}
        self._dial_locks: dict[str, asyncio.Lock] = {}
        self._closed = False

    async def get(self, addr: str, *, max_attempts: int = 1) -> Connection:
        """Connection to `addr` ("host:port"), dialing if needed. Retries up to
        `max_attempts` with exponential backoff + jitter between failures."""
        if self._closed:
            raise ConnectionLostError("pool is closed")
        existing = self._by_addr.get(addr)
        if existing is not None and not existing.is_closed:
            return existing
        lock = self._dial_locks.setdefault(addr, asyncio.Lock())
        async with lock:
            existing = self._by_addr.get(addr)
            if existing is not None and not existing.is_closed:
                return existing
            conn = await self._dial(addr, max_attempts)
            # register may pick an existing (e.g. inbound) connection over the
            # one just dialed; always hand out the survivor
            survivor = self.register(conn)
            self._by_addr[addr] = survivor
            return survivor

    def register(self, conn: Connection) -> Connection:
        """Track a connection (inbound or outbound) by peer node_id, resolving
        duplicates. Returns the surviving connection for that peer."""
        assert conn.peer is not None
        if self._closed:
            close_task = asyncio.ensure_future(conn.close(reason="pool is closed"))
            close_task.add_done_callback(lambda _: None)
            return conn
        peer_id = conn.peer.node_id
        existing = self._by_node.get(peer_id)
        if existing is None or existing.is_closed or existing is conn:
            self._by_node[peer_id] = conn
            if conn.peer.listen_addr is not None:
                # inbound connections become dialable by address too, so `get`
                # reuses them instead of racing a duplicate dial
                tracked = self._by_addr.get(conn.peer.listen_addr)
                if tracked is None or tracked.is_closed:
                    self._by_addr[conn.peer.listen_addr] = conn
            return conn
        # Duplicate: keep the connection whose initiator has the SMALLER node_id.
        if self._initiator_id(existing) < self._initiator_id(conn):
            keep, drop = existing, conn
        else:
            keep, drop = conn, existing
        self._by_node[peer_id] = keep
        drop_task = asyncio.ensure_future(drop.close(reason="duplicate connection"))
        drop_task.add_done_callback(lambda _: None)
        for addr, tracked in list(self._by_addr.items()):
            if tracked is drop:
                self._by_addr[addr] = keep
        return keep

    def by_node(self, node_id: uuid.UUID) -> Connection | None:
        conn = self._by_node.get(node_id)
        if conn is not None and conn.is_closed:
            del self._by_node[node_id]
            return None
        return conn

    async def close(self) -> None:
        self._closed = True
        conns = {id(c): c for c in [*self._by_addr.values(), *self._by_node.values()]}
        self._by_addr.clear()
        self._by_node.clear()
        for conn in conns.values():
            await conn.close()

    def _initiator_id(self, conn: Connection) -> uuid.UUID:
        assert conn.peer is not None
        return self._local.node_id if conn.initiator else conn.peer.node_id

    async def _dial(self, addr: str, max_attempts: int) -> Connection:
        if self._address_map is not None:
            addr = self._address_map(addr)
        host, _, port_raw = addr.rpartition(":")
        port = int(port_raw)
        ssl_context = self._tls.client_context() if self._tls is not None else None
        delay = self._config.reconnect_base
        attempt = 0
        while True:
            attempt += 1
            try:
                return await Connection.connect(
                    host,
                    port,
                    local=self._local,
                    config=self._config,
                    ssl_context=ssl_context,
                    on_request=self._on_request,
                    on_control=self._on_control,
                    on_bulk=self._on_bulk,
                    on_actor_stream=self._on_actor_stream,
                    on_close=self._on_close,
                )
            except (CastyError, OSError):
                if attempt >= max_attempts:
                    raise
                jitter = delay * random.uniform(-0.25, 0.25)
                await asyncio.sleep(delay + jitter)
                delay = min(delay * 2, self._config.reconnect_max)
