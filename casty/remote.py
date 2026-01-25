from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Protocol, Awaitable, TYPE_CHECKING

from .serializable import serialize, deserialize
from .core import actor, Mailbox, Reply
from . import logger

if TYPE_CHECKING:
    from .core import ActorRef, Envelope


class Serializer(Protocol):
    def encode(self, obj: Any) -> bytes: ...
    def decode(self, data: bytes) -> Any: ...


class MsgPackSerializer:
    def encode(self, obj: Any) -> bytes:
        return serialize(obj)

    def decode(self, data: bytes) -> Any:
        return deserialize(data)


@dataclass
class RemoteEnvelope:
    type: str
    correlation_id: str | None = None
    target: str | None = None
    name: str | None = None
    payload: bytes | None = None
    error: str | None = None
    sender_name: str | None = None
    version: int | None = None
    ensure: bool = False
    initial_state: bytes | None = None
    behavior: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RemoteEnvelope:
        return cls(
            type=data.get("type", ""),
            correlation_id=data.get("correlation_id"),
            target=data.get("target"),
            name=data.get("name"),
            payload=data.get("payload"),
            error=data.get("error"),
            sender_name=data.get("sender_name"),
            version=data.get("version"),
            ensure=data.get("ensure", False),
            initial_state=data.get("initial_state"),
            behavior=data.get("behavior"),
        )


class RemoteError(Exception):
    pass


@dataclass
class SendDeliver:
    target: str
    payload: bytes
    sender_name: str | None = None


@dataclass
class SendAsk:
    target: str
    payload: bytes
    correlation_id: str


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


@dataclass
class RemoteRef[M]:
    actor_id: str
    name: str
    _session: "ActorRef" = field(repr=False)
    _serializer: Serializer = field(repr=False)
    default_timeout: float = 30.0
    behavior_name: str | None = None

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        logger.debug("remote send", actor_id=self.actor_id, msg_type=type(msg).__name__, target=self.name, sender=sender.actor_id if sender else None)
        payload = self._serializer.encode(msg)
        sender_name = sender.actor_id if sender else None
        await self._session.send(SendDeliver(
            target=self.name,
            payload=payload,
            sender_name=sender_name,
        ))

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        sender = envelope.sender
        await self.send(envelope.payload, sender=sender)

    async def ask[R](self, msg: M, timeout: float | None = None) -> R:
        logger.debug("remote ask", actor_id=self.actor_id, msg_type=type(msg).__name__, target=self.name, timeout=timeout)
        payload = self._serializer.encode(msg)
        correlation_id = uuid.uuid4().hex
        response = await self._session.ask(
            SendAsk(target=self.name, payload=payload, correlation_id=correlation_id),
            timeout=timeout or self.default_timeout,
        )
        logger.debug("remote ask response", actor_id=self.actor_id, target=self.name)
        return self._serializer.decode(response)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)


@dataclass
class _SessionConnected:
    peer_id: str
    session: "ActorRef"


@dataclass
class _SessionDisconnected:
    peer_id: str


@dataclass
class _LocalLookup:
    name: str


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
        from .core import get_registered_actor
        behavior = get_registered_actor(behavior_name)
        if not behavior:
            return None

        if check_replicas:
            config = behavior.__replication_config__
            replicas = config.replicas if config else None
            if not replicas or replicas <= 1:
                return None

        if initial_state:
            state = deserialize(initial_state)
            behavior = behavior(state)

        ref = await system.actor(behavior, name=actor_name)

        from .messages import Expose
        await remote_ref.send(Expose(ref=ref, name=actor_name))

        return ref
    except (KeyError, RuntimeError, TypeError, AttributeError):
        return None


from .messages import (
    Listen, Connect, Listening, RemoteConnected, Connected, ListenFailed, ConnectFailed,
    Lookup, LookupResult, Expose, Unexpose, Exposed, Unexposed,
    Received, Write, PeerClosed, ErrorClosed, Aborted,
    Register, InboundEvent,
)
from .io import tcp, Bind, IoConnect, IoConnected, IoConnectFailed, Bound, BindFailed, LengthPrefixedFramer


type SessionMessage = (
    Received | PeerClosed | ErrorClosed | Aborted |
    SendDeliver | SendAsk | SendLookup | SendReplicate
)


@actor
async def session_actor(
    connection: "ActorRef",
    remote_ref: "ActorRef",
    serializer: Serializer,
    peer_id: str,
    *,
    mailbox: Mailbox[SessionMessage],
):
    pending: dict[str, tuple[str, ActorRef | None]] = {}

    await remote_ref.send(_SessionConnected(peer_id=peer_id, session=mailbox.ref()))

    async for msg, ctx in mailbox:
        match msg:
            case Received(data):
                envelope = RemoteEnvelope.from_dict(serializer.decode(data))
                logger.debug("Received envelope", type=envelope.type, target=envelope.target, name=envelope.name)
                await _handle_envelope(
                    envelope, remote_ref, connection, pending, serializer, mailbox.ref(), ctx._system
                )

            case PeerClosed() | ErrorClosed(_) | Aborted():
                logger.debug("Connection closed")
                await remote_ref.send(_SessionDisconnected(peer_id=peer_id))
                for _, reply_to in pending.values():
                    if reply_to:
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
                logger.debug("SendLookup, sending over wire", name=name, correlation_id=correlation_id)
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
    serializer: Serializer,
    self_ref: "ActorRef",
    system: Any = None,
):
    match envelope.type:
        case "deliver":
            logger.debug("_handle_envelope: deliver", target=envelope.target)
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
                logger.warn("_handle_envelope: deliver target not found", target=envelope.target)

        case "ask":
            logger.debug("_handle_envelope: ask", target=envelope.target)
            local_ref = await remote_ref.ask(_LocalLookup(envelope.target))

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
                    await reply_to.send(Reply(result=envelope.payload))

        case "error":
            if envelope.correlation_id in pending:
                _, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    await reply_to.send(Reply(result=RemoteError(envelope.error)))

        case "lookup":
            logger.debug("_handle_envelope: lookup", name=envelope.name, ensure=envelope.ensure)
            local_ref = await remote_ref.ask(_LocalLookup(envelope.name))
            logger.debug("_handle_envelope: lookup _LocalLookup result", name=envelope.name, found=local_ref is not None)

            if not local_ref and envelope.ensure and envelope.name and envelope.behavior and system:
                try:
                    from .core import get_registered_actor
                    actor_fn = get_registered_actor(envelope.behavior)
                    if actor_fn:
                        local_ref = await system.actor(actor_fn(), name=envelope.name)
                        await remote_ref.send(Expose(ref=local_ref, name=envelope.name))
                except (KeyError, RuntimeError, TypeError, AttributeError):
                    pass

            if not local_ref and envelope.name:
                local_ref = await _ensure_actor_replica(
                    system, envelope.behavior, envelope.name, remote_ref, envelope.initial_state
                )

            exists = local_ref is not None
            logger.debug("_handle_envelope: lookup replying", name=envelope.name, exists=exists)
            reply = RemoteEnvelope(
                type="lookup_result",
                name=envelope.name,
                correlation_id=envelope.correlation_id,
                payload=b"1" if exists else b"0",
            )
            await connection.send(Write(serializer.encode(reply.to_dict())))

        case "lookup_result":
            exists = envelope.payload == b"1"
            logger.debug("_handle_envelope: lookup_result received", name=envelope.name, exists=exists, correlation_id=envelope.correlation_id)
            if envelope.correlation_id in pending:
                _, reply_to = pending.pop(envelope.correlation_id)
                if reply_to:
                    logger.debug("_handle_envelope: lookup_result replying to pending")
                    await reply_to.send(Reply(result=exists))
            else:
                logger.warn("_handle_envelope: lookup_result no pending for correlation_id", correlation_id=envelope.correlation_id)

        case "replicate":
            if system and envelope.target and envelope.payload:
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


type RemoteMessage = (
    Listen | Connect |
    Expose | Unexpose | Lookup |
    _SessionConnected | _SessionDisconnected | _LocalLookup
)


@actor
async def remote_actor(*, mailbox: Mailbox[RemoteMessage]):
    tcp_mgr: ActorRef | None = None
    serializer: Serializer = MsgPackSerializer()
    exposed: dict[str, ActorRef] = {}
    sessions: dict[str, ActorRef] = {}

    async for msg, ctx in mailbox:
        if tcp_mgr is None:
            tcp_mgr = await ctx.actor(tcp(), name="tcp")

        match msg:
            case Listen(port, host):
                logger.debug("Starting listener", host=host, port=port)
                try:
                    await ctx.actor(
                        _remote_listener(tcp_mgr, host, port, serializer, mailbox.ref(), ctx.sender),
                        name=f"listener-{port}"
                    )
                except Exception as e:
                    logger.error("Listen failed", host=host, port=port, error=str(e))
                    await ctx.reply(ListenFailed(str(e)))

            case Connect(host, port):
                peer_addr = f"{host}:{port}"
                if peer_addr in sessions:
                    logger.debug("Already connected to peer", peer=peer_addr)
                    await ctx.reply(RemoteConnected(
                        remote_address=(host, port),
                        peer_id=peer_addr,
                    ))
                    continue

                logger.debug("Connecting to peer", host=host, port=port)
                try:
                    await ctx.actor(
                        _remote_connector(tcp_mgr, host, port, serializer, mailbox.ref(), ctx.sender),
                        name=f"connector-{host}-{port}"
                    )
                except Exception as e:
                    logger.error("Connect failed", host=host, port=port, error=str(e))
                    await ctx.reply(ConnectFailed(str(e)))

            case Expose(ref, name):
                exposed[name] = ref
                logger.debug("Exposed actor", name=name)
                await ctx.reply(Exposed(name))

            case Unexpose(name):
                exposed.pop(name, None)
                await ctx.reply(Unexposed(name))

            case Lookup(name, peer=None, ensure=ensure, initial_state=initial_state, behavior=behavior):
                logger.debug("Lookup without peer", name=name)
                if name in exposed:
                    logger.debug("Found in exposed", name=name)
                    await ctx.reply(LookupResult(ref=exposed[name], peer=None))
                else:
                    found = False
                    logger.debug("Searching in sessions", name=name, sessions=list(sessions.keys()))
                    for peer_id, session in sessions.items():
                        try:
                            correlation_id = uuid.uuid4().hex
                            logger.debug("Sending SendLookup to session", name=name, peer=peer_id)
                            exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id, ensure=ensure, initial_state=initial_state, behavior=behavior))
                            logger.debug("SendLookup result", name=name, peer=peer_id, exists=exists)
                            if exists:
                                ref = RemoteRef(
                                    actor_id=f"remote/{name}",
                                    name=name,
                                    _session=session,
                                    _serializer=serializer,
                                )
                                await ctx.reply(LookupResult(ref=ref, peer=peer_id))
                                found = True
                                break
                        except (TimeoutError, OSError, KeyError) as e:
                            logger.debug("SendLookup failed", name=name, peer=peer_id, error=str(e))
                            continue
                    if not found:
                        logger.debug("Not found anywhere", name=name)
                        await ctx.reply(LookupResult(ref=None, peer=None))

            case Lookup(name, peer=peer_id, ensure=ensure, initial_state=initial_state, behavior=behavior) if peer_id is not None:
                logger.debug("Lookup with peer", name=name, peer=peer_id)
                if peer_id in sessions:
                    session = sessions[peer_id]
                    try:
                        correlation_id = uuid.uuid4().hex
                        logger.debug("Sending SendLookup to peer session", name=name, peer=peer_id)
                        exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id, ensure=ensure, initial_state=initial_state, behavior=behavior))
                        logger.debug("SendLookup result from peer", name=name, peer=peer_id, exists=exists)
                        if exists:
                            ref = RemoteRef(
                                actor_id=f"remote/{name}",
                                name=name,
                                _session=session,
                                _serializer=serializer,
                            )
                            await ctx.reply(LookupResult(ref=ref, peer=peer_id))
                        else:
                            await ctx.reply(LookupResult(ref=None, peer=peer_id))
                    except (TimeoutError, OSError, KeyError) as e:
                        logger.warn("SendLookup to peer failed", name=name, peer=peer_id, error=str(e))
                        await ctx.reply(LookupResult(ref=None, peer=peer_id))
                else:
                    logger.warn("Peer not in sessions", name=name, peer=peer_id, known_sessions=list(sessions.keys()))
                    await ctx.reply(LookupResult(ref=None, peer=peer_id))

            case _SessionConnected(peer_id, session):
                sessions[peer_id] = session
                logger.debug("Session connected", peer=peer_id)

            case _SessionDisconnected(peer_id):
                sessions.pop(peer_id, None)
                logger.debug("Session disconnected", peer=peer_id)

            case _LocalLookup(name):
                logger.debug("_LocalLookup", name=name)
                if name in exposed:
                    logger.debug("_LocalLookup found in exposed", name=name)
                    await ctx.reply(exposed[name])
                else:
                    ref = await ctx._system.actor(name=name)
                    if ref:
                        logger.debug("_LocalLookup found in system", name=name)
                        exposed[name] = ref
                    else:
                        logger.debug("_LocalLookup not found", name=name)
                    await ctx.reply(ref)


@actor
async def _remote_listener(
    tcp_mgr: "ActorRef",
    host: str,
    port: int,
    serializer: Serializer,
    remote_ref: "ActorRef",
    reply_to: "ActorRef | None",
    *,
    mailbox: Mailbox[InboundEvent],
):
    replied = False
    local_address: tuple[str, int] | None = None

    await tcp_mgr.send(Bind(
        handler=mailbox.ref(),
        host=host,
        port=port,
        framing=LengthPrefixedFramer(),
    ))

    async for msg, ctx in mailbox:
        match msg:
            case Bound(addr):
                local_address = addr
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=Listening(address=local_address)))

            case BindFailed(reason):
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=ListenFailed(reason)))
                break

            case IoConnected(connection, remote_addr, _):
                peer_id = f"{remote_addr[0]}:{remote_addr[1]}"
                session = await ctx.actor(
                    session_actor(connection, remote_ref, serializer, peer_id),
                    name=f"session-{peer_id}"
                )
                await connection.send(Register(session))


@actor
async def _remote_connector(
    tcp_mgr: "ActorRef",
    host: str,
    port: int,
    serializer: Serializer,
    remote_ref: "ActorRef",
    reply_to: "ActorRef | None",
    *,
    mailbox: Mailbox[InboundEvent],
):
    replied = False

    await tcp_mgr.send(IoConnect(
        handler=mailbox.ref(),
        host=host,
        port=port,
        framing=LengthPrefixedFramer(),
    ))

    async for msg, ctx in mailbox:
        match msg:
            case IoConnected(connection, remote_addr, _):
                peer_id = f"{remote_addr[0]}:{remote_addr[1]}"
                session = await ctx.actor(
                    session_actor(connection, remote_ref, serializer, peer_id),
                    name="session"
                )
                await connection.send(Register(session))
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=RemoteConnected(
                        remote_address=remote_addr,
                        peer_id=peer_id,
                    )))

            case IoConnectFailed(reason):
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=ConnectFailed(reason)))
                break

            case PeerClosed() | ErrorClosed(_) | Aborted():
                break


remote = remote_actor
