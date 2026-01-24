from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import actor, Mailbox
from casty.logger import debug as log_debug, info as log_info, warn as log_warn, error as log_error
from casty.io.messages import Received, Write, PeerClosed, ErrorClosed, Aborted
from .protocol import RemoteEnvelope, RemoteError
from .ref import SendDeliver, SendAsk, RemoteRef

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .serializer import Serializer


async def _ensure_actor_replica(
    system: Any,
    behavior_name: str | None,
    actor_name: str,
    remote_ref: "ActorRef",
    initial_state: bytes | None = None,
    check_replicas: bool = True,
) -> "ActorRef | None":
    if not behavior_name or not system:
        return None

    try:
        from casty.actor import get_behavior
        behavior = get_behavior(behavior_name)
        if not behavior:
            return None

        if check_replicas:
            config = behavior.__replication_config__
            replicas = config.replicas if config else None
            if not replicas or replicas <= 1:
                return None

        if initial_state:
            from casty.serializable import deserialize
            state = deserialize(initial_state)
            behavior = behavior(state)

        ref = await system.actor(behavior, name=actor_name)

        from .messages import Expose
        await remote_ref.send(Expose(ref=ref, name=actor_name))

        return ref
    except (KeyError, RuntimeError, TypeError, AttributeError):
        return None


@dataclass
class SendLookup:
    name: str
    correlation_id: str
    ensure: bool = False
    initial_state: bytes | None = None
    behavior: str | None = None


@dataclass
class SendReplicate:
    actor_id: str
    snapshot: bytes
    version: int
    behavior: str | None = None


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
                log_debug("Received envelope", f"session/{peer_id}", type=envelope.type, target=envelope.target, name=envelope.name)
                await _handle_envelope(
                    envelope, remote_ref, connection, pending, serializer, mailbox.ref(), ctx._system
                )

            case PeerClosed() | ErrorClosed(_) | Aborted():
                log_debug("Connection closed", f"session/{peer_id}")
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

            case SendLookup(name, correlation_id, ensure, initial_state, behavior):
                log_debug("SendLookup, sending over wire", f"session/{peer_id}", name=name, correlation_id=correlation_id)
                pending[correlation_id] = ("lookup", ctx.sender)
                envelope = RemoteEnvelope(
                    type="lookup",
                    name=name,
                    correlation_id=correlation_id,
                    ensure=ensure,
                    initial_state=initial_state,
                    behavior=behavior,
                )
                await connection.send(Write(serializer.encode(envelope.to_dict())))

            case SendReplicate(actor_id, snapshot, version, behavior):
                envelope = RemoteEnvelope(
                    type="replicate",
                    target=actor_id,
                    payload=snapshot,
                    version=version,
                    behavior=behavior,
                )
                await connection.send(Write(serializer.encode(envelope.to_dict())))


async def _handle_envelope(
    envelope: RemoteEnvelope,
    remote_ref: "ActorRef",
    connection: "ActorRef",
    pending: dict[str, tuple[str, "ActorRef | None"]],
    serializer: "Serializer",
    self_ref: "ActorRef",
    system: Any = None,
):
    from .remote import _LocalLookup

    match envelope.type:
        case "deliver":
            log_debug("_handle_envelope: deliver", "session", target=envelope.target)
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
            else:
                log_warn("_handle_envelope: deliver target not found", "session", target=envelope.target)

        case "ask":
            log_debug("_handle_envelope: ask", "session", target=envelope.target)
            local_ref = await remote_ref.ask(_LocalLookup(envelope.target))

            # Lazy replication: create replica if this node is responsible
            if not local_ref and envelope.target:
                local_ref = await _ensure_actor_replica(
                    system, envelope.behavior, envelope.target, remote_ref
                )

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
            log_debug("_handle_envelope: lookup", "session", name=envelope.name, ensure=envelope.ensure)
            local_ref = await remote_ref.ask(_LocalLookup(envelope.name))
            log_debug("_handle_envelope: lookup _LocalLookup result", "session", name=envelope.name, found=local_ref is not None)

            # Ensure-create: if ensure=True and actor doesn't exist, create it
            if not local_ref and envelope.ensure and envelope.name and envelope.behavior and system:
                try:
                    from casty.actor import get_registered_actor
                    actor_fn = get_registered_actor(envelope.behavior)
                    if actor_fn:
                        local_ref = await system.actor(actor_fn(), name=envelope.name)
                        from .messages import Expose
                        await remote_ref.send(Expose(ref=local_ref, name=envelope.name))
                except (KeyError, RuntimeError, TypeError, AttributeError):
                    pass

            # Lazy replication for lookup: create actor if behavior is registered
            if not local_ref and envelope.name:
                local_ref = await _ensure_actor_replica(
                    system, envelope.behavior, envelope.name, remote_ref, envelope.initial_state
                )

            exists = local_ref is not None
            log_debug("_handle_envelope: lookup replying", "session", name=envelope.name, exists=exists)
            reply = RemoteEnvelope(
                type="lookup_result",
                name=envelope.name,
                correlation_id=envelope.correlation_id,
                payload=b"1" if exists else b"0",
            )
            await connection.send(Write(serializer.encode(reply.to_dict())))

        case "lookup_result":
            exists = envelope.payload == b"1"
            log_debug("_handle_envelope: lookup_result received", "session", name=envelope.name, exists=exists, correlation_id=envelope.correlation_id)
            if envelope.correlation_id in pending:
                _, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    log_debug("_handle_envelope: lookup_result replying to pending", "session")
                    from casty.reply import Reply
                    await reply_to.send(Reply(result=exists))
            else:
                log_warn("_handle_envelope: lookup_result no pending for correlation_id", "session", correlation_id=envelope.correlation_id)

        case "replicate":
            if system and envelope.target and envelope.payload:
                from casty.serializable import deserialize

                mailbox = system._mailboxes.get(envelope.target)
                if not mailbox and envelope.behavior:
                    ref = await _ensure_actor_replica(
                        system, envelope.behavior, envelope.target, remote_ref,
                        check_replicas=False
                    )
                    if ref:
                        mailbox = system._mailboxes.get(envelope.target)

                if mailbox and mailbox.state is not None:
                    remote_state = deserialize(envelope.payload)
                    for key, value in remote_state.items():
                        setattr(mailbox.state, key, value)
