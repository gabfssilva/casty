from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.envelope import Envelope
from .connection import tcp_connection
from .framing import RawFramer
from .messages import (
    Bind, Connect,
    Bound, BindFailed, Connected, ConnectFailed,
    InboundEvent,
)

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .framing import Framer


@dataclass
class _AcceptedConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


@actor
async def tcp(*, mailbox: Mailbox[Bind | Connect]):
    listener_count = 0
    conn_count = 0

    async for msg, ctx in mailbox:
        match msg:
            case Bind(handler, port, host, framing):
                try:
                    await ctx.actor(
                        tcp_listener(handler, host, port, framing),
                        name=f"listener-{listener_count}"
                    )
                    listener_count += 1
                except Exception as e:
                    await handler.send(BindFailed(str(e)))

            case Connect(handler, host, port, framing):
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    local = writer.get_extra_info("sockname")
                    remote = writer.get_extra_info("peername")
                    framer = framing or RawFramer()
                    conn = await ctx.actor(
                        tcp_connection(reader, writer, handler, framer),
                        name=f"conn-out-{conn_count}"
                    )
                    conn_count += 1
                    await handler.send(Connected(conn, remote, local))
                except Exception as e:
                    await handler.send(ConnectFailed(str(e)))


@actor
async def tcp_listener(
    handler: "ActorRef[InboundEvent]",
    host: str,
    port: int,
    framing: "Framer | None",
    *,
    mailbox: Mailbox[_AcceptedConnection],
):
    framer = framing or RawFramer()
    conn_count = 0

    async def on_accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await mailbox.put(Envelope(payload=_AcceptedConnection(reader, writer)))

    try:
        server = await asyncio.start_server(on_accept, host, port)
        local = server.sockets[0].getsockname()
        await handler.send(Bound(local))
    except Exception as e:
        await handler.send(BindFailed(str(e)))
        return

    async for msg, ctx in mailbox:
        match msg:
            case _AcceptedConnection(reader, writer):
                remote = writer.get_extra_info("peername")
                local = writer.get_extra_info("sockname")
                conn = await ctx.actor(
                    tcp_connection(reader, writer, handler, framer),
                    name=f"conn-in-{conn_count}"
                )
                conn_count += 1
                await handler.send(Connected(conn, remote, local))
