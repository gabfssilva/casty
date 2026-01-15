"""UDP Server actor - listens for datagrams from multiple peers."""

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from casty import Actor, Context

if TYPE_CHECKING:
    from casty import ActorRef

from .messages import (
    Bind,
    Unbind,
    Bound,
    RegisterHandler,
    Received,
    CommandFailed,
)

logger = logging.getLogger(__name__)


@dataclass
class _ServerState:
    """Internal state for bound server."""
    host: str
    port: int
    transport: asyncio.DatagramTransport
    protocol: asyncio.DatagramProtocol
    task: Optional[asyncio.Task] = None


class Server(Actor[Bind | Unbind]):
    """
    UDP server actor.

    States:
    - receive (default): unbound, waiting for Bind
    - _bound: listening for incoming datagrams
    """

    def __init__(self):
        self._state: Optional[_ServerState] = None
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

    async def receive(self, msg: Bind | Unbind | RegisterHandler, ctx: Context):
        """Default state: handle bind/unbind."""
        match msg:
            case Bind(host=host, port=port):
                try:
                    await self._do_bind(host, port, ctx)
                except Exception as e:
                    ctx.reply(CommandFailed(f"Bind failed: {e}", e))

            case Unbind():
                ctx.reply(CommandFailed("Not bound", None))

            case RegisterHandler(handler=handler):
                self._handler = handler
                ctx.reply(None)

    async def _do_bind(self, host: str, port: int, ctx: Context):
        """Bind to host:port and start listening."""
        loop = asyncio.get_event_loop()

        # Create custom protocol that forwards datagrams to us
        def make_protocol():
            return _UDPServerProtocol(self._schedule_datagram)

        # Bind socket
        transport, protocol = await loop.create_datagram_endpoint(
            make_protocol,
            local_addr=(host, port),
        )

        self._state = _ServerState(
            host=host,
            port=port,
            transport=transport,
            protocol=protocol,
        )

        # Get actual bound address (handles port 0)
        bound_addr = transport.get_extra_info("sockname")
        actual_host, actual_port = bound_addr

        # Notify parent
        ctx.reply(Bound(actual_host, actual_port))

        # Switch to bound state
        ctx.become(self._bound)

    async def _bound(self, msg: Bind | Unbind | RegisterHandler, ctx: Context):
        """Bound state: handle messages while listening."""
        match msg:
            case Bind():
                ctx.reply(CommandFailed("Already bound", None))

            case Unbind():
                self._do_unbind()
                ctx.unbecome()
                ctx.reply(None)

            case RegisterHandler(handler=handler):
                self._handler = handler
                ctx.reply(None)

    def _do_unbind(self) -> None:
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


class _UDPServerProtocol(asyncio.DatagramProtocol):
    """Internal datagram protocol - forwards to server actor."""

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
