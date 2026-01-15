"""UDP Client actor - sends/receives datagrams to/from a peer."""

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from casty import Actor, Context

if TYPE_CHECKING:
    from casty import ActorRef

from .messages import (
    Connect,
    Disconnect,
    Connected,
    Disconnected,
    RegisterHandler,
    Send,
    SendTo,
    Received,
    CommandFailed,
)

logger = logging.getLogger(__name__)


@dataclass
class _ClientState:
    """Internal state for connected client."""
    remote_host: str
    remote_port: int
    transport: asyncio.DatagramTransport
    protocol: asyncio.DatagramProtocol


class Client(Actor[Connect | Disconnect | Send | SendTo]):
    """
    UDP client actor - sends/receives datagrams.

    States:
    - receive (default): disconnected, waiting for Connect
    - _connected: connected to peer, can Send/SendTo
    """

    def __init__(self):
        self._state: Optional[_ClientState] = None
        self._handler: Optional['ActorRef'] = None  # noqa: F821
        self._pending_tasks: set[asyncio.Task] = set()  # Track pending tasks

    async def on_stop(self):
        """Cleanup: cancel all pending tasks."""
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
        # Wait for all tasks to be cancelled
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
        self._pending_tasks.clear()

    async def receive(self, msg: Connect | Disconnect | Send | SendTo | RegisterHandler, ctx: Context):
        """Default state: handle connect/disconnect."""
        match msg:
            case Connect(host=host, port=port):
                try:
                    await self._do_connect(host, port, ctx)
                except Exception as e:
                    ctx.reply(CommandFailed(f"Connect failed: {e}", e))

            case Disconnect():
                ctx.reply(CommandFailed("Not connected", None))

            case Send():
                ctx.reply(CommandFailed("Not connected", None))

            case SendTo():
                ctx.reply(CommandFailed("Not connected", None))

            case RegisterHandler(handler=handler):
                self._handler = handler
                ctx.reply(None)

    async def _do_connect(self, host: str, port: int, ctx: Context):
        """Connect to remote peer."""
        loop = asyncio.get_event_loop()

        # Create custom protocol that forwards datagrams to us
        def make_protocol():
            return _UDPClientProtocol(self._schedule_datagram)

        # Open datagram connection (doesn't actually connect, just creates socket)
        transport, protocol = await loop.create_datagram_endpoint(
            make_protocol,
            remote_addr=(host, port),
        )

        self._state = _ClientState(
            remote_host=host,
            remote_port=port,
            transport=transport,
            protocol=protocol,
        )

        # Notify parent
        ctx.reply(Connected((host, port)))

        # Switch to connected state
        ctx.become(self._connected)

    async def _connected(self, msg: Connect | Disconnect | Send | SendTo | RegisterHandler, ctx: Context):
        """Connected state: send/receive messages."""
        match msg:
            case Connect():
                ctx.reply(CommandFailed("Already connected", None))

            case Disconnect():
                self._do_disconnect()
                ctx.unbecome()
                ctx.reply(None)

            case Send(data=data):
                if self._state:
                    try:
                        self._state.transport.sendto(data)
                        # No ack for UDP
                    except Exception as e:
                        ctx.reply(CommandFailed(f"Send failed: {e}", e))

            case SendTo(host=host, port=port, data=data):
                if self._state:
                    try:
                        self._state.transport.sendto(data, (host, port))
                    except Exception as e:
                        ctx.reply(CommandFailed(f"SendTo failed: {e}", e))

            case RegisterHandler(handler=handler):
                self._handler = handler
                ctx.reply(None)

    def _do_disconnect(self):
        """Close socket and cleanup."""
        if self._state:
            self._state.transport.close()
            self._state = None

    def _schedule_datagram(self, data: bytes, addr: tuple[str, int]) -> None:
        """Schedule datagram processing as tracked task."""
        task = asyncio.create_task(self._process_datagram(data, addr))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _process_datagram(self, data: bytes, addr: tuple[str, int]) -> None:
        """Process datagram asynchronously."""
        if self._state and self._handler:
            # Send Received event to registered handler
            event = Received(data=data, from_addr=addr)
            await self._handler.send(event)
        elif self._state:
            logger.debug(f"Received {len(data)} bytes from {addr} (no handler registered)")


class _UDPClientProtocol(asyncio.DatagramProtocol):
    """Internal datagram protocol - forwards to client actor."""

    def __init__(self, schedule_datagram):
        self.schedule_datagram = schedule_datagram
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]):
        """Called when datagram arrives."""
        # Schedule datagram processing with task tracking
        self.schedule_datagram(data, addr)

    def error_received(self, exc: Exception):
        logger.error(f"UDP error: {exc}")

    def connection_lost(self, exc: Optional[Exception]):
        if exc:
            logger.error(f"UDP connection lost: {exc}")
