from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.envelope import Envelope
from .transport_messages import Transmit, Received, Disconnect, Deliver

if TYPE_CHECKING:
    from casty.ref import ActorRef


@actor
async def connection_actor(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    router_ref: "ActorRef",
    *,
    mailbox: Mailbox[Transmit | Received | Disconnect],
):
    async def recv_loop():
        try:
            while True:
                length_bytes = await reader.readexactly(4)
                length = int.from_bytes(length_bytes, "big")
                data = await reader.readexactly(length)
                await mailbox.put(Envelope(payload=Received(data)))
        except (asyncio.IncompleteReadError, asyncio.CancelledError):
            await mailbox.put(Envelope(payload=Disconnect()))

    recv_task = asyncio.create_task(recv_loop())

    self_ref = None
    async for msg, ctx in mailbox:
        if self_ref is None:
            self_ref = ctx._self_ref

        match msg:
            case Transmit(data):
                writer.write(len(data).to_bytes(4, "big") + data)
                await writer.drain()

            case Received(data):
                await router_ref.send(Deliver(data=data, reply_connection=self_ref))

            case Disconnect():
                recv_task.cancel()
                try:
                    await recv_task
                except asyncio.CancelledError:
                    pass
                writer.close()
                await writer.wait_closed()
                break
