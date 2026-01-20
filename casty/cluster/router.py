from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import actor, Mailbox
from casty.ref import ActorRef
from casty.envelope import Envelope
from casty.serializable import deserialize, serialize
from .transport_messages import Register, Deliver, Transmit


@dataclass
class RegisterPending:
    pending: dict[str, asyncio.Future[Any]]


@actor
async def router_actor(
    *,
    mailbox: Mailbox[Register | RegisterPending | Deliver],
):
    registry: dict[str, ActorRef] = {}
    pending_asks: dict[str, asyncio.Future[Any]] = {}

    async for msg, ctx in mailbox:
        match msg:
            case Register(ref):
                registry[ref.actor_id] = ref

            case RegisterPending(pending):
                pending_asks = pending

            case Deliver(data, reply_connection):
                envelope = deserialize(data)

                if envelope.correlation_id and envelope.correlation_id in pending_asks:
                    future = pending_asks.pop(envelope.correlation_id)
                    if not future.done():
                        future.set_result(envelope.payload)
                else:
                    ref = registry.get(envelope.target)
                    if ref:
                        if envelope.correlation_id and reply_connection:
                            reply_future: asyncio.Future[Any] = asyncio.Future()

                            async def send_reply(
                                fut: asyncio.Future[Any],
                                corr_id: str,
                                conn: ActorRef,
                            ):
                                try:
                                    result = await fut
                                    reply_envelope = Envelope(
                                        payload=result,
                                        correlation_id=corr_id,
                                    )
                                    reply_data = serialize(reply_envelope)
                                    await conn.send(Transmit(data=reply_data))
                                except asyncio.CancelledError:
                                    pass

                            asyncio.create_task(
                                send_reply(reply_future, envelope.correlation_id, reply_connection)
                            )
                            envelope.reply_to = reply_future

                        await ref.send_envelope(envelope)
