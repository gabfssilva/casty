"""
Chaos TCP Proxy - Actor-based network chaos simulation.

This proxy sits between a client and server, injecting configurable
chaos (latency, packet loss, corruption) to simulate real-world
network conditions.

Architecture:
    ChaosProxy (supervisor)
    ├── tcp.Server (accepts client connections)
    └── ProxyConnection (per-connection, spawned on accept)
        └── tcp.Client (connects to upstream)

Usage:
    proxy = await system.spawn(ChaosProxy)
    await proxy.send(StartProxy(
        listen_port=9001,
        target_host="127.0.0.1",
        target_port=8001,
        chaos=ChaosConfig.slow_network()
    ))

    # Connect to :9001 instead of :8001
    # Traffic will have simulated latency/loss
"""

import asyncio
import random
import time

from casty import Actor, Context, LocalRef
from casty.io import tcp

from .config import ChaosConfig
from .messages import (
    StartProxy,
    StopProxy,
    UpdateChaos,
    ProxyStarted,
    ProxyStopped,
    ProxyError,
    ConnectionProxied,
    ConnectionClosed,
)


class ChaosProxy(Actor):
    """TCP proxy actor with chaos injection.

    Spawns a TCP server and forwards all connections to an upstream
    target, applying configurable chaos (latency, loss, etc).
    """

    def __init__(self):
        self._server: LocalRef | None = None
        self._target: tuple[str, int] | None = None
        self._chaos: ChaosConfig = ChaosConfig()
        self._connections: set[LocalRef] = set()

    async def receive(self, msg, ctx: Context):
        match msg:
            case StartProxy(listen_port, target_host, target_port, chaos):
                self._target = (target_host, target_port)
                self._chaos = chaos or ChaosConfig()

                self._server = await ctx.spawn(tcp.Server)
                await self._server.send(tcp.Bind("127.0.0.1", listen_port))

            case StopProxy():
                await self._stop_all(ctx)

            case UpdateChaos(chaos):
                self._chaos = chaos
                for conn in self._connections:
                    await conn.send(UpdateChaos(chaos))

            case tcp.Bound(host, port):
                await self._notify_parent(ProxyStarted(host, port), ctx)

            case tcp.Accepted(connection, remote_address):
                # Spawn ProxyConnection first
                proxy_conn = await ctx.spawn(
                    ProxyConnection,
                    client_conn=connection,
                    target=self._target,
                    chaos=self._chaos,
                    proxy=ctx.self_ref,
                )
                self._connections.add(proxy_conn)
                # Register IMMEDIATELY - before any data can arrive
                # ProxyConnection.on_start runs after this returns
                await connection.send(tcp.Register(proxy_conn))
                await self._notify_parent(ConnectionProxied(remote_address), ctx)

            case ConnectionClosed() as event:
                await self._notify_parent(event, ctx)
                if ctx.sender and ctx.sender in self._connections:
                    self._connections.discard(ctx.sender)

            case tcp.CommandFailed(_, cause):
                await self._notify_parent(ProxyError(cause), ctx)

            case tcp.Unbound():
                await self._notify_parent(ProxyStopped(), ctx)

    async def _stop_all(self, ctx: Context):
        """Stop server and all connections."""
        if self._server:
            await self._server.send(tcp.Unbind())

        for conn in list(self._connections):
            await ctx.stop_child(conn)
        self._connections.clear()

    async def _notify_parent(self, event, ctx: Context):
        """Send event to parent if exists."""
        if ctx.parent:
            await ctx.parent.send(event, sender=ctx.self_ref)


class ProxyConnection(Actor):
    """Handles a single proxied connection with chaos injection."""

    def __init__(
        self,
        client_conn: LocalRef,
        target: tuple[str, int],
        chaos: ChaosConfig,
        proxy: LocalRef,
    ):
        self._client_conn = client_conn
        self._target = target
        self._chaos = chaos
        self._proxy = proxy

        self._upstream_client: LocalRef | None = None
        self._upstream_conn: LocalRef | None = None
        self._remote_address: tuple[str, int] = ("unknown", 0)

        # Stats
        self._bytes_sent = 0
        self._bytes_received = 0

        # Buffer for data that arrives before upstream is connected
        self._pending_to_upstream: list[bytes] = []

        # Bandwidth limiting
        self._last_send_time: float = 0

    async def on_start(self):
        """Connect to upstream on start.

        Note: ChaosProxy already registered us as handler before spawning,
        so we just need to connect to upstream.
        """
        self._upstream_client = await self._ctx.spawn(tcp.Client)
        await self._upstream_client.send(
            tcp.Connect(self._target[0], self._target[1])
        )

    async def on_stop(self):
        """Report stats on stop."""
        await self._proxy.send(
            ConnectionClosed(
                remote_address=self._remote_address,
                bytes_sent=self._bytes_sent,
                bytes_received=self._bytes_received,
            )
        )

    async def receive(self, msg, ctx: Context):
        match msg:
            case tcp.Connected(connection, remote, _):
                self._upstream_conn = connection
                self._remote_address = remote
                await connection.send(tcp.Register(ctx.self_ref))

                # Flush any buffered data that arrived before upstream connected
                for pending_data in self._pending_to_upstream:
                    await self._send_with_chaos(self._upstream_conn, pending_data)
                self._pending_to_upstream.clear()

            case tcp.Received(data):
                await self._forward_with_chaos(data, ctx)

            case UpdateChaos(chaos):
                self._chaos = chaos

            case tcp.PeerClosed() | tcp.Closed():
                await self._close_both()

            case tcp.ErrorClosed(_):
                await self._close_both()

            case tcp.CommandFailed(_, _):
                await self._close_both()

    async def _forward_with_chaos(self, data: bytes, ctx: Context):
        """Forward data with chaos injection."""
        sender = ctx.sender

        if sender == self._client_conn:
            self._bytes_sent += len(data)
            # Buffer if upstream not yet connected
            if not self._upstream_conn:
                self._pending_to_upstream.append(data)
                return
            target = self._upstream_conn
        elif sender == self._upstream_conn:
            target = self._client_conn
            self._bytes_received += len(data)
        else:
            return

        if not target:
            return

        await self._send_with_chaos(target, data)

    async def _send_with_chaos(self, target: LocalRef, data: bytes):
        """Send data to target with chaos injection (latency + bandwidth)."""
        # === DELAY ===
        if self._chaos.delay_ms > 0:
            delay = self._chaos.delay_ms / 1000
            if self._chaos.jitter_ms > 0:
                jitter = self._chaos.jitter_ms / 1000
                delay += random.uniform(-jitter, jitter)
            if delay > 0:
                await asyncio.sleep(delay)

        # === BANDWIDTH LIMIT ===
        if self._chaos.bandwidth_kbps > 0:
            await self._apply_bandwidth_limit(len(data))

        # === FORWARD ===
        await target.send(tcp.Write(data))

    async def _apply_bandwidth_limit(self, bytes_count: int):
        """Apply bandwidth limiting by sleeping."""
        bytes_per_sec = self._chaos.bandwidth_kbps * 1024
        time_needed = bytes_count / bytes_per_sec

        now = time.monotonic()
        if self._last_send_time > 0:
            elapsed = now - self._last_send_time
            if elapsed < time_needed:
                await asyncio.sleep(time_needed - elapsed)

        self._last_send_time = time.monotonic()

    async def _close_both(self):
        """Close both client and upstream connections."""
        if self._client_conn:
            try:
                await self._client_conn.send(tcp.Close())
            except Exception:
                pass
            self._client_conn = None

        if self._upstream_conn:
            try:
                await self._upstream_conn.send(tcp.Close())
            except Exception:
                pass
            self._upstream_conn = None
