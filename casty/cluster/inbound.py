from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import uuid4
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.envelope import Envelope
from .connection import connection_actor
from .transport_messages import NewConnection

if TYPE_CHECKING:
    from casty.ref import ActorRef


@dataclass
class GetPort:
    pass


@actor
async def inbound_actor(
    host: str,
    port: int,
    router_ref: "ActorRef",
    *,
    mailbox: Mailbox[NewConnection | GetPort],
):
    actual_port = port

    async def on_accept(reader, writer):
        await mailbox.put(Envelope(payload=NewConnection(reader, writer)))

    server = await asyncio.start_server(on_accept, host, port)
    actual_port = server.sockets[0].getsockname()[1]

    async for msg, ctx in mailbox:
        match msg:
            case NewConnection(reader, writer):
                await ctx.actor(
                    connection_actor(reader, writer, router_ref),
                    name=f"in-{uuid4().hex[:8]}"
                )

            case GetPort():
                await ctx.reply(actual_port)
