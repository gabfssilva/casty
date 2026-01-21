from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.io import tcp
from casty.io.messages import (
    Bound, BindFailed, Connected as TcpConnected, ConnectFailed as TcpConnectFailed,
    Received, Register, PeerClosed, ErrorClosed, Aborted,
    InboundEvent,
)
from casty.io import Bind, Connect as TcpConnect, LengthPrefixedFramer
from casty.reply import Reply
from .messages import (
    Listen, Connect, Listening, Connected, ListenFailed, ConnectFailed,
    Lookup, LookupResult, Expose, Unexpose, Exposed, Unexposed,
)
from .serializer import MsgPackSerializer
from .registry import registry_actor, SessionConnected, SessionDisconnected
from .session import session_actor, SendLookup
from .ref import RemoteRef

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .serializer import Serializer


@actor
async def remote(*, mailbox: Mailbox[Listen | Connect]):
    tcp_mgr: ActorRef | None = None

    async for msg, ctx in mailbox:
        if tcp_mgr is None:
            tcp_mgr = await ctx.actor(tcp(), name="tcp")

        match msg:
            case Listen(port, host, serializer):
                ser = serializer or MsgPackSerializer()
                try:
                    await ctx.actor(
                        _remote_listener(tcp_mgr, host, port, ser, ctx.sender),
                        name=f"listener-{port}"
                    )
                except Exception as e:
                    await ctx.reply(ListenFailed(str(e)))

            case Connect(host, port, serializer):
                ser = serializer or MsgPackSerializer()
                try:
                    await ctx.actor(
                        _remote_connector(tcp_mgr, host, port, ser, ctx.sender),
                        name=f"connector-{host}-{port}"
                    )
                except Exception as e:
                    await ctx.reply(ConnectFailed(str(e)))


@actor
async def _remote_listener(
    tcp_mgr: "ActorRef",
    host: str,
    port: int,
    serializer: "Serializer",
    reply_to: "ActorRef | None",
    *,
    mailbox: Mailbox[InboundEvent],
):
    registry: ActorRef | None = None
    replied = False

    await tcp_mgr.send(Bind(
        handler=mailbox.ref(),
        host=host,
        port=port,
        framing=LengthPrefixedFramer(),
    ))

    async for msg, ctx in mailbox:
        match msg:
            case Bound(local_address):
                registry = await ctx.actor(
                    registry_actor(),
                    name="registry"
                )
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=Listening(registry=registry, address=local_address)))

            case BindFailed(reason):
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=ListenFailed(reason)))
                break

            case TcpConnected(connection, remote_addr, local_addr):
                session = await ctx.actor(
                    session_actor(connection, registry, serializer),
                    name=f"session-{remote_addr[0]}-{remote_addr[1]}"
                )
                await connection.send(Register(session))


@actor
async def _remote_connector(
    tcp_mgr: "ActorRef",
    host: str,
    port: int,
    serializer: "Serializer",
    reply_to: "ActorRef | None",
    *,
    mailbox: Mailbox[InboundEvent],
):
    registry: ActorRef | None = None
    session: ActorRef | None = None
    replied = False

    await tcp_mgr.send(TcpConnect(
        handler=mailbox.ref(),
        host=host,
        port=port,
        framing=LengthPrefixedFramer(),
    ))

    async for msg, ctx in mailbox:
        match msg:
            case TcpConnected(connection, remote_addr, local_addr):
                registry = await ctx.actor(
                    _client_registry(serializer),
                    name="registry"
                )
                session = await ctx.actor(
                    session_actor(connection, registry, serializer),
                    name="session"
                )
                await connection.send(Register(session))
                await registry.send(_SetSession(session))
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=Connected(
                        registry=registry,
                        remote_address=remote_addr,
                    )))

            case TcpConnectFailed(reason):
                if reply_to and not replied:
                    replied = True
                    await reply_to.send(Reply(result=ConnectFailed(reason)))
                break

            case PeerClosed() | ErrorClosed(_) | Aborted():
                break


@dataclass
class _SetSession:
    session: "ActorRef"


@actor
async def _client_registry(
    serializer: "Serializer",
    *,
    mailbox: Mailbox,
):
    exposed: dict[str, ActorRef] = {}
    session: ActorRef | None = None

    async for msg, ctx in mailbox:
        match msg:
            case _SetSession(s):
                session = s

            case Expose(ref, name):
                exposed[name] = ref
                await ctx.reply(Exposed(name))

            case Unexpose(name):
                exposed.pop(name, None)
                await ctx.reply(Unexposed(name))

            case Lookup(name):
                if name in exposed:
                    await ctx.reply(LookupResult(ref=exposed[name]))
                elif session:
                    correlation_id = uuid.uuid4().hex
                    exists = await session.ask(SendLookup(name=name, correlation_id=correlation_id))
                    if exists:
                        ref = RemoteRef(
                            actor_id=f"remote/{name}",
                            name=name,
                            _session=session,
                            _serializer=serializer,
                        )
                        await ctx.reply(LookupResult(ref=ref))
                    else:
                        await ctx.reply(LookupResult(ref=None))
                else:
                    await ctx.reply(LookupResult(ref=None))

            case SessionConnected(_):
                pass

            case SessionDisconnected(_):
                session = None
