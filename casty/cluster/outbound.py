from __future__ import annotations

import asyncio

from casty import actor, Mailbox
from casty.ref import ActorRef
from .connection import connection_actor
from .transport_messages import Connect, GetConnection


@actor
async def outbound_actor(
    router_ref: ActorRef,
    *,
    mailbox: Mailbox[Connect | GetConnection],
):
    connections: dict[str, ActorRef] = {}

    async for msg, ctx in mailbox:
        match msg:
            case Connect(node_id, address):
                if node_id not in connections:
                    host, port_str = address.rsplit(":", 1)
                    reader, writer = await asyncio.open_connection(host, int(port_str))
                    ref = await ctx.actor(
                        connection_actor(reader, writer, router_ref),
                        name=f"out-{node_id}"
                    )
                    connections[node_id] = ref
                await ctx.reply(connections[node_id])

            case GetConnection(node_id):
                await ctx.reply(connections.get(node_id))
