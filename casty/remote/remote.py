from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
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
                try:
                    await ctx.actor(
                        _remote_listener(tcp_mgr, host, port, s, mailbox.ref(), ctx.sender),
                        name=f"listener-{port}"
                    )
                except Exception as e:
                    await ctx.reply(ListenFailed(str(e)))

            case Connect(host, port, ser):
                peer_addr = f"{host}:{port}"
                if peer_addr in sessions:
                    await ctx.reply(Connected(
                        remote_address=(host, port),
                        peer_id=peer_addr,
                    ))
                    continue

                s = ser or serializer
                try:
                    await ctx.actor(
                        _remote_connector(tcp_mgr, host, port, s, mailbox.ref(), ctx.sender),
                        name=f"connector-{host}-{port}"
                    )
                except Exception as e:
                    await ctx.reply(ConnectFailed(str(e)))

            case Expose(ref, name):
                exposed[name] = ref
                await ctx.reply(Exposed(name))

            case Unexpose(name):
                exposed.pop(name, None)
                await ctx.reply(Unexposed(name))

            case Lookup(name, peer=None, ensure=ensure):
                if name in exposed:
                    await ctx.reply(LookupResult(ref=exposed[name], peer=None))
                else:
                    found = False
                    for peer_id, session in sessions.items():
                        try:
                            correlation_id = uuid.uuid4().hex
                            exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id, ensure=ensure))
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
                        except (TimeoutError, OSError, KeyError):
                            continue
                    if not found:
                        await ctx.reply(LookupResult(ref=None, peer=None))

            case Lookup(name, peer=peer_id, ensure=ensure) if peer_id is not None:
                if peer_id in sessions:
                    session = sessions[peer_id]
                    try:
                        correlation_id = uuid.uuid4().hex
                        exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id, ensure=ensure))
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
                    except (TimeoutError, OSError, KeyError):
                        await ctx.reply(LookupResult(ref=None, peer=peer_id))
                else:
                    await ctx.reply(LookupResult(ref=None, peer=peer_id))

            case _SessionConnected(peer_id, session):
                sessions[peer_id] = session

            case _SessionDisconnected(peer_id):
                sessions.pop(peer_id, None)

            case _LocalLookup(name):
                if name in exposed:
                    await ctx.reply(exposed[name])
                else:
                    # Also check system for actors (e.g., lazily created replicas)
                    ref = await ctx._system.actor(name=name)
                    if ref:
                        exposed[name] = ref
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
