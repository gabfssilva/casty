from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.envelope import Envelope
from .messages import (
    Register, Write, Close,
    Received, PeerClosed, ErrorClosed, Aborted,
    InboundEvent, OutboundEvent,
)

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .framing import Framer


@dataclass
class _InternalReceived:
    data: bytes


@dataclass
class _InternalClosed:
    reason: PeerClosed | ErrorClosed | Aborted


@actor
async def tcp_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    initial_handler: "ActorRef[InboundEvent]",
    framing: "Framer",
    *,
    mailbox: Mailbox[OutboundEvent | _InternalReceived | _InternalClosed],
):
    handler = initial_handler

    async def recv_loop():
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    await mailbox.put(Envelope(payload=_InternalClosed(PeerClosed())))
                    return
                await mailbox.put(Envelope(payload=_InternalReceived(data)))
        except ConnectionResetError:
            await mailbox.put(Envelope(payload=_InternalClosed(Aborted())))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await mailbox.put(Envelope(payload=_InternalClosed(ErrorClosed(str(e)))))

    recv_task = asyncio.create_task(recv_loop())

    self_ref = None
    async for msg, ctx in mailbox:
        if self_ref is None:
            self_ref = ctx._self_ref

        match msg:
            case Register(new_handler):
                handler = new_handler

            case Write(data):
                encoded = framing.encode(data)
                writer.write(encoded)
                await writer.drain()

            case Close():
                recv_task.cancel()
                try:
                    await recv_task
                except asyncio.CancelledError:
                    pass
                writer.close()
                await writer.wait_closed()
                break

            case _InternalReceived(data):
                for frame in framing.feed(data):
                    await handler.send(Received(frame), sender=self_ref)

            case _InternalClosed(reason):
                await handler.send(reason)
                writer.close()
                await writer.wait_closed()
                break
