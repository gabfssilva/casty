from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from casty.config import TLS, TransportConfig
from casty.errors import CastyError
from casty.transport.connection import (
    ActorStreamHandler,
    BulkHandler,
    CloseHandler,
    Connection,
    ControlHandler,
    LocalNode,
    RequestHandler,
)

logger = logging.getLogger("casty.transport")

type ConnectionHandler = Callable[[Connection], Awaitable[None]]


class Server:
    """TCP listener that upgrades inbound sockets into casty Connections."""

    def __init__(self, server: asyncio.Server, host: str, port: int) -> None:
        self._server = server
        self.host = host
        self.port = port

    @classmethod
    async def start(
        cls,
        host: str,
        port: int,
        *,
        local: LocalNode,
        config: TransportConfig | None = None,
        tls: TLS | None = None,
        on_connection: ConnectionHandler | None = None,
        on_request: RequestHandler | None = None,
        on_control: ControlHandler | None = None,
        on_bulk: BulkHandler | None = None,
        on_actor_stream: ActorStreamHandler | None = None,
        on_close: CloseHandler | None = None,
    ) -> Server:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            peername = writer.get_extra_info("peername")
            try:
                conn = await Connection.accept(
                    reader,
                    writer,
                    local=local,
                    config=config,
                    on_request=on_request,
                    on_control=on_control,
                    on_bulk=on_bulk,
                    on_actor_stream=on_actor_stream,
                    on_close=on_close,
                )
            except CastyError as exc:
                logger.info("handshake with %s failed: %s", peername, exc)
                return
            if on_connection is not None:
                await on_connection(conn)

        ssl_context = tls.server_context() if tls is not None else None
        server = await asyncio.start_server(handle, host, port, ssl=ssl_context)
        sockets = server.sockets
        actual_port = sockets[0].getsockname()[1] if sockets else port
        return cls(server, host, actual_port)

    def stop_accepting(self) -> None:
        """Close the listener without waiting. `wait_closed` (inside `close`)
        only returns once accepted connections are gone (3.12.1+ semantics), so
        shutdown must stop the listener first, then close connections, then
        await `close`."""
        self._server.close()

    async def close(self) -> None:
        self._server.close()
        await self._server.wait_closed()
