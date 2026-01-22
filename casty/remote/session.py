from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.io.messages import Received, Write, PeerClosed, ErrorClosed, Aborted
from .protocol import RemoteEnvelope, RemoteError
from .ref import SendDeliver, SendAsk, RemoteRef

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .serializer import Serializer


@dataclass
class SendLookup:
    name: str
    correlation_id: str


@dataclass
class SendReplicate:
    actor_id: str
    snapshot: bytes
    version: int


type SessionMessage = (
    Received | PeerClosed | ErrorClosed | Aborted |
    SendDeliver | SendAsk | SendLookup | SendReplicate
)


@actor
async def session_actor(
    connection: "ActorRef",
    remote_ref: "ActorRef",
    serializer: "Serializer",
    peer_id: str,
    *,
    mailbox: Mailbox[SessionMessage],
):
    pending: dict[str, tuple[str, ActorRef | None]] = {}

    from .remote import _SessionConnected, _SessionDisconnected, _LocalLookup
    await remote_ref.send(_SessionConnected(peer_id=peer_id, session=mailbox.ref()))

    async for msg, ctx in mailbox:
        match msg:
            case Received(data):
                envelope = RemoteEnvelope.from_dict(serializer.decode(data))
                await _handle_envelope(
                    envelope, remote_ref, connection, pending, serializer, mailbox.ref(), ctx._system
                )

            case PeerClosed() | ErrorClosed(_) | Aborted():
                await remote_ref.send(_SessionDisconnected(peer_id=peer_id))
                for _, reply_to in pending.values():
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

            case SendReplicate(actor_id, snapshot, version):
                envelope = RemoteEnvelope(
                    type="replicate",
                    target=actor_id,
                    payload=snapshot,
                    version=version,
                )
                await connection.send(Write(serializer.encode(envelope.to_dict())))


async def _handle_envelope(
    envelope: RemoteEnvelope,
    remote_ref: "ActorRef",
    connection: "ActorRef",
    pending: dict[str, tuple[str, "ActorRef | None"]],
    serializer: "Serializer",
    self_ref: "ActorRef",
    system: "Any" = None,
):
    from typing import Any
    from .remote import _LocalLookup

    match envelope.type:
        case "deliver":
            local_ref = await remote_ref.ask(_LocalLookup(envelope.target))
            if local_ref:
                msg = serializer.decode(envelope.payload)
                sender = None
                if envelope.sender_name:
                    sender = RemoteRef(
                        actor_id=f"remote/{envelope.sender_name}",
                        name=envelope.sender_name,
                        _session=self_ref,
                        _serializer=serializer,
                    )
                await local_ref.send(msg, sender=sender)

        case "ask":
            local_ref = await remote_ref.ask(_LocalLookup(envelope.target))

            # Lazy replication: create replica if this node is responsible
            if not local_ref and envelope.target and "/" in envelope.target and system:
                try:
                    from casty.cluster.registry import get_behavior
                    func_name = envelope.target.split("/", 1)[0]
                    behavior = get_behavior(func_name)
                    if behavior:
                        replication_config = getattr(behavior.func, '__replication_config__', None)
                        replicated = replication_config.replicated if replication_config else None
                        if replicated and replicated > 1:
                            actor_name = envelope.target.split("/", 1)[1]
                            local_ref = await system.actor(behavior, name=actor_name)
                            from .messages import Expose
                            await remote_ref.send(Expose(ref=local_ref, name=envelope.target))
                except Exception:
                    pass

            if local_ref:
                msg = serializer.decode(envelope.payload)
                try:
                    response = await local_ref.ask(msg)
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
                _, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    from casty.reply import Reply
                    await reply_to.send(Reply(result=envelope.payload))

        case "error":
            if envelope.correlation_id in pending:
                _, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    from casty.reply import Reply
                    await reply_to.send(Reply(result=RemoteError(envelope.error)))

        case "lookup":
            local_ref = await remote_ref.ask(_LocalLookup(envelope.name))

            # Lazy replication for lookup: create actor if behavior is registered
            if not local_ref and envelope.name and "/" in envelope.name and system:
                try:
                    from casty.cluster.registry import get_behavior
                    func_name = envelope.name.split("/", 1)[0]
                    behavior = get_behavior(func_name)
                    if behavior:
                        replication_config = getattr(behavior.func, '__replication_config__', None)
                        replicated = replication_config.replicated if replication_config else None
                        if replicated and replicated > 1:
                            actor_name = envelope.name.split("/", 1)[1]
                            local_ref = await system.actor(behavior, name=actor_name)
                            from .messages import Expose
                            await remote_ref.send(Expose(ref=local_ref, name=envelope.name))
                except Exception:
                    pass

            exists = local_ref is not None
            reply = RemoteEnvelope(
                type="lookup_result",
                name=envelope.name,
                correlation_id=envelope.correlation_id,
                payload=b"1" if exists else b"0",
            )
            await connection.send(Write(serializer.encode(reply.to_dict())))

        case "lookup_result":
            if envelope.correlation_id in pending:
                _, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    from casty.reply import Reply
                    exists = envelope.payload == b"1"
                    await reply_to.send(Reply(result=exists))

        case "replicate":
            if system and envelope.target and envelope.payload:
                from casty.serializable import deserialize

                # Get or create the actor if it doesn't exist
                mailbox = system._mailboxes.get(envelope.target)
                if not mailbox and "/" in envelope.target:
                    try:
                        from casty.cluster.registry import get_behavior
                        func_name = envelope.target.split("/", 1)[0]
                        behavior = get_behavior(func_name)
                        if behavior:
                            actor_name = envelope.target.split("/", 1)[1]
                            await system.actor(behavior, name=actor_name)
                            from .messages import Expose
                            await remote_ref.send(Expose(ref=system._actors.get(envelope.target), name=envelope.target))
                            mailbox = system._mailboxes.get(envelope.target)
                    except Exception:
                        pass

                if mailbox and mailbox.state is not None:
                    snapshot = deserialize(envelope.payload)
                    mailbox.state.restore(snapshot)
