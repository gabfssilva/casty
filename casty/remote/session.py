from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import actor, Mailbox
from casty.io.messages import Received, Write, PeerClosed, ErrorClosed, Aborted
from .protocol import RemoteEnvelope, RemoteError
from .messages import Lookup, LookupResult
from .ref import SendDeliver, SendAsk, RemoteRef
from .registry import SessionConnected, SessionDisconnected

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .serializer import Serializer


@dataclass
class SendLookup:
    name: str
    correlation_id: str


type SessionMessage = (
    Received | PeerClosed | ErrorClosed | Aborted |
    SendDeliver | SendAsk | SendLookup
)


@actor
async def session_actor(
    connection: "ActorRef",
    registry: "ActorRef",
    serializer: "Serializer",
    *,
    mailbox: Mailbox[SessionMessage],
):
    pending: dict[str, tuple[str, ActorRef | None]] = {}
    self_ref: ActorRef | None = None

    async for msg, ctx in mailbox:
        if self_ref is None:
            self_ref = ctx._self_ref
            await registry.send(SessionConnected(self_ref))

        match msg:
            case Received(data):
                envelope = RemoteEnvelope.from_dict(serializer.decode(data))
                await _handle_envelope(
                    envelope, ctx, registry, connection, pending, serializer, self_ref
                )

            case PeerClosed() | ErrorClosed(_) | Aborted():
                if self_ref:
                    await registry.send(SessionDisconnected(self_ref))
                for request_type, reply_to in pending.values():
                    if reply_to:
                        from casty.reply import Reply
                        await reply_to.send(Reply(result=RemoteError("Connection closed")))
                break

            case SendDeliver(target, payload, sender_name):
                envelope = RemoteEnvelope(
                    type="deliver",
                    target=target,
                    payload=payload,
                    sender_name=sender_name,
                )
                await connection.send(Write(serializer.encode(envelope.to_dict())))

            case SendAsk(target, payload, correlation_id):
                pending[correlation_id] = ("ask", ctx.sender)
                envelope = RemoteEnvelope(
                    type="ask",
                    target=target,
                    payload=payload,
                    correlation_id=correlation_id,
                )
                await connection.send(Write(serializer.encode(envelope.to_dict())))

            case SendLookup(name, correlation_id):
                pending[correlation_id] = ("lookup", ctx.sender)
                envelope = RemoteEnvelope(
                    type="lookup",
                    name=name,
                    correlation_id=correlation_id,
                )
                await connection.send(Write(serializer.encode(envelope.to_dict())))


async def _handle_envelope(
    envelope: RemoteEnvelope,
    ctx,
    registry: "ActorRef",
    connection: "ActorRef",
    pending: dict[str, tuple[str, "ActorRef | None"]],
    serializer: "Serializer",
    self_ref: "ActorRef",
):
    match envelope.type:
        case "deliver":
            result = await registry.ask(Lookup(envelope.target))
            if result.ref:
                msg = serializer.decode(envelope.payload)
                sender = None
                if envelope.sender_name:
                    sender = RemoteRef(
                        actor_id=f"remote/{envelope.sender_name}",
                        name=envelope.sender_name,
                        _session=self_ref,
                        _serializer=serializer,
                    )
                await result.ref.send(msg, sender=sender)

        case "ask":
            result = await registry.ask(Lookup(envelope.target))
            if result.ref:
                msg = serializer.decode(envelope.payload)
                try:
                    response = await result.ref.ask(msg)
                    reply = RemoteEnvelope(
                        type="reply",
                        correlation_id=envelope.correlation_id,
                        payload=serializer.encode(response),
                    )
                    await connection.send(Write(serializer.encode(reply.to_dict())))
                except Exception as e:
                    error = RemoteEnvelope(
                        type="error",
                        correlation_id=envelope.correlation_id,
                        error=str(e),
                    )
                    await connection.send(Write(serializer.encode(error.to_dict())))
            else:
                error = RemoteEnvelope(
                    type="error",
                    correlation_id=envelope.correlation_id,
                    error=f"Actor '{envelope.target}' not found",
                )
                await connection.send(Write(serializer.encode(error.to_dict())))

        case "reply":
            if envelope.correlation_id in pending:
                request_type, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    from casty.reply import Reply
                    await reply_to.send(Reply(result=envelope.payload))

        case "error":
            if envelope.correlation_id in pending:
                request_type, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    from casty.reply import Reply
                    await reply_to.send(Reply(result=RemoteError(envelope.error)))

        case "lookup":
            result = await registry.ask(Lookup(envelope.name))
            exists = result.ref is not None
            reply = RemoteEnvelope(
                type="lookup_result",
                name=envelope.name,
                correlation_id=envelope.correlation_id,
                payload=b"1" if exists else b"0",
            )
            await connection.send(Write(serializer.encode(reply.to_dict())))

        case "lookup_result":
            if envelope.correlation_id in pending:
                request_type, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    from casty.reply import Reply
                    exists = envelope.payload == b"1"
                    await reply_to.send(Reply(result=exists))
