from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.logger import debug as log_debug, info as log_info, warn as log_warn, error as log_error
from casty.io import tcp
from casty.io.messages import (
    Bound, BindFailed, Connected as TcpConnected, ConnectFailed as TcpConnectFailed,
    Register, PeerClosed, ErrorClosed, Aborted,
    InboundEvent,
)
from casty.io import Bind, Connect as TcpConnect, LengthPrefixedFramer
from casty.reply import Reply
from .messages import (
    Listen, Connect, Listening, Connected, ListenFailed, ConnectFailed,
    Lookup, LookupResult, Expose, Unexpose, Exposed, Unexposed,
)
from .serializer import MsgPackSerializer
from .session import session_actor, SendLookup
from .ref import RemoteRef

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .serializer import Serializer


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


type RemoteMessage = (
    Listen | Connect |
    Expose | Unexpose | Lookup |
    _SessionConnected | _SessionDisconnected | _LocalLookup
)


@actor
async def remote(*, mailbox: Mailbox[RemoteMessage]):
    tcp_mgr: ActorRef | None = None
    serializer: Serializer = MsgPackSerializer()
    exposed: dict[str, ActorRef] = {}
    sessions: dict[str, ActorRef] = {}

    async for msg, ctx in mailbox:
        if tcp_mgr is None:
            tcp_mgr = await ctx.actor(tcp(), name="tcp")

        match msg:
            case Listen(port, host, ser):
                s = ser or serializer
                log_debug("Starting listener", "remote", host=host, port=port)
                try:
                    await ctx.actor(
                        _remote_listener(tcp_mgr, host, port, s, mailbox.ref(), ctx.sender),
                        name=f"listener-{port}"
                    )
                except Exception as e:
                    log_error("Listen failed", "remote", host=host, port=port, error=str(e))
                    await ctx.reply(ListenFailed(str(e)))

            case Connect(host, port, ser):
                peer_addr = f"{host}:{port}"
                if peer_addr in sessions:
                    log_debug("Already connected to peer", "remote", peer=peer_addr)
                    await ctx.reply(Connected(
                        remote_address=(host, port),
                        peer_id=peer_addr,
                    ))
                    continue

                s = ser or serializer
                log_debug("Connecting to peer", "remote", host=host, port=port)
                try:
                    await ctx.actor(
                        _remote_connector(tcp_mgr, host, port, s, mailbox.ref(), ctx.sender),
                        name=f"connector-{host}-{port}"
                    )
                except Exception as e:
                    log_error("Connect failed", "remote", host=host, port=port, error=str(e))
                    await ctx.reply(ConnectFailed(str(e)))

            case Expose(ref, name):
                exposed[name] = ref
                log_debug("Exposed actor", "remote", name=name)
                await ctx.reply(Exposed(name))

            case Unexpose(name):
                exposed.pop(name, None)
                await ctx.reply(Unexposed(name))

            case Lookup(name, peer=None, ensure=ensure, initial_state=initial_state, behavior=behavior):
                log_debug("Lookup without peer", "remote", name=name)
                if name in exposed:
                    log_debug("Found in exposed", "remote", name=name)
                    await ctx.reply(LookupResult(ref=exposed[name], peer=None))
                else:
                    found = False
                    log_debug("Searching in sessions", "remote", name=name, sessions=list(sessions.keys()))
                    for peer_id, session in sessions.items():
                        try:
                            correlation_id = uuid.uuid4().hex
                            log_debug("Sending SendLookup to session", "remote", name=name, peer=peer_id)
                            exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id, ensure=ensure, initial_state=initial_state, behavior=behavior))
                            log_debug("SendLookup result", "remote", name=name, peer=peer_id, exists=exists)
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
                            log_debug("SendLookup failed", "remote", name=name, peer=peer_id, error=str(e))
                            continue
                    if not found:
                        log_debug("Not found anywhere", "remote", name=name)
                        await ctx.reply(LookupResult(ref=None, peer=None))

            case Lookup(name, peer=peer_id, ensure=ensure, initial_state=initial_state, behavior=behavior) if peer_id is not None:
                log_debug("Lookup with peer", "remote", name=name, peer=peer_id)
                if peer_id in sessions:
                    session = sessions[peer_id]
                    try:
                        correlation_id = uuid.uuid4().hex
                        log_debug("Sending SendLookup to peer session", "remote", name=name, peer=peer_id)
                        exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id, ensure=ensure, initial_state=initial_state, behavior=behavior))
                        log_debug("SendLookup result from peer", "remote", name=name, peer=peer_id, exists=exists)
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
                        log_warn("SendLookup to peer failed", "remote", name=name, peer=peer_id, error=str(e))
                        await ctx.reply(LookupResult(ref=None, peer=peer_id))
                else:
                    log_warn("Peer not in sessions", "remote", name=name, peer=peer_id, known_sessions=list(sessions.keys()))
                    await ctx.reply(LookupResult(ref=None, peer=peer_id))

            case _SessionConnected(peer_id, session):
                sessions[peer_id] = session
                log_debug("Session connected", "remote", peer=peer_id)

            case _SessionDisconnected(peer_id):
                sessions.pop(peer_id, None)
                log_debug("Session disconnected", "remote", peer=peer_id)

            case _LocalLookup(name):
                log_debug("_LocalLookup", "remote", name=name)
                if name in exposed:
                    log_debug("_LocalLookup found in exposed", "remote", name=name)
                    await ctx.reply(exposed[name])
                else:
                    # Also check system for actors (e.g., lazily created replicas)
                    ref = await ctx._system.actor(name=name)
                    if ref:
                        log_debug("_LocalLookup found in system", "remote", name=name)
                        exposed[name] = ref
                    else:
                        log_debug("_LocalLookup not found", "remote", name=name)
                    await ctx.reply(ref)


@actor
async def _remote_listener(
    tcp_mgr: "ActorRef",
    host: str,
    port: int,
    serializer: "Serializer",
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

            case TcpConnected(connection, remote_addr, _):
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
    serializer: "Serializer",
    remote_ref: "ActorRef",
    reply_to: "ActorRef | None",
    *,
    mailbox: Mailbox[InboundEvent],
):
    replied = False

    await tcp_mgr.send(TcpConnect(
        handler=mailbox.ref(),
        host=host,
        port=port,
        framing=LengthPrefixedFramer(),
    ))

    async for msg, ctx in mailbox:
        match msg:
            case TcpConnected(connection, remote_addr, _):
                peer_id = f"{remote_addr[0]}:{remote_addr[1]}"
                session = await ctx.actor(
                    session_actor(connection, remote_ref, serializer, peer_id),
                    name="session"
                )
                await connection.send(Register(session))
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=Connected(
                        remote_address=remote_addr,
                        peer_id=peer_id,
                    )))

            case TcpConnectFailed(reason):
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=ConnectFailed(reason)))
                break

            case PeerClosed() | ErrorClosed(_) | Aborted():
                break
