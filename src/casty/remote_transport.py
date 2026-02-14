"""TCP-based remote transport for cross-node actor communication.

``tcp_transport`` actor manages all TCP connections, spawning a
``peer_connection`` child per peer.  ``RemoteTransport`` bridges the
``MessageTransport`` protocol to the transport actor via ``tell(SendToNode(...))``.

Wire protocol: ``[msg_len:4][frame_type:1][payload]``

- ``frame_type=0x00`` — handshake (JSON: ``{"host": "...", "port": N}``)
- ``frame_type=0x01`` — message (same payload as ``MessageEnvelope.to_bytes()``)

Each node pair shares **one** bidirectional TCP connection.  Simultaneous
connect races are resolved deterministically: the connection initiated by the
smaller ``(host, port)`` wins.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import ssl
import struct
import time
from dataclasses import dataclass, field
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypedDict

from casty.actor import Behavior, Behaviors
from casty.address import ActorAddress
from casty.serialization import Serializer
from casty.transport import LocalTransport

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.ref import ActorRef
    from casty.task_runner import TaskRunnerMsg

type NodeAddr = tuple[str, int]
type AddressMap = dict[NodeAddr, NodeAddr]


class HandshakePayload(TypedDict):
    host: str
    port: int

FRAME_HANDSHAKE: int = 0x00
FRAME_MESSAGE: int = 0x01


class InboundHandler(Protocol):
    async def on_message(self, data: bytes) -> None: ...


class InboundMessageHandler:
    """Deserializes inbound TCP frames and delivers them locally.

    Satisfies ``InboundHandler`` structurally.  Passed directly to
    ``tcp_transport`` at spawn time — no placeholder or delegate wiring
    needed.
    """

    def __init__(
        self,
        *,
        local: LocalTransport,
        serializer: Serializer,
        system_name: str,
    ) -> None:
        self._local = local
        self._serializer = serializer
        self._logger = logging.getLogger(f"casty.remote_transport.{system_name}")

    async def on_message(self, data: bytes) -> None:
        try:
            envelope = MessageEnvelope.from_bytes(data)
            msg = self._serializer.deserialize(envelope.payload)
            target_addr = ActorAddress.from_uri(envelope.target)
            self._logger.debug("Received %s -> %s", envelope.type_hint, target_addr.path)
            self._local.deliver(target_addr, msg)
        except Exception:
            self._logger.warning("Failed to handle inbound message", exc_info=True)


def set_nodelay(writer: asyncio.StreamWriter) -> None:
    sock = writer.transport.get_extra_info("socket")
    if sock is not None:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def make_handshake_frame(self_addr: NodeAddr) -> bytes:
    payload = json.dumps({"host": self_addr[0], "port": self_addr[1]}).encode("utf-8")
    frame = bytes([FRAME_HANDSHAKE]) + payload
    return struct.pack("!I", len(frame)) + frame


def make_message_frame(data: bytes) -> bytes:
    frame = bytes([FRAME_MESSAGE]) + data
    return struct.pack("!I", len(frame)) + frame


def resolve_race(self_addr: NodeAddr, peer_addr: NodeAddr, new_initiator: NodeAddr) -> bool:
    """Return True if *new_initiator*'s connection should win over an existing one.

    The connection initiated by ``min(self_addr, peer_addr)`` wins.
    """
    return new_initiator == min(self_addr, peer_addr)


@dataclass(frozen=True)
class SendFrame:
    data: bytes


@dataclass(frozen=True)
class PeerDisconnected:
    pass


type PeerMsg = SendFrame | PeerDisconnected


@dataclass(frozen=True)
class PeerDown:
    addr: NodeAddr
    peer: ActorRef[PeerMsg]


def peer_connection(
    *,
    addr: NodeAddr,
    writer: asyncio.StreamWriter,
    reader: asyncio.StreamReader,
    handler: InboundHandler,
    parent: ActorRef[Any],
    logger: logging.Logger,
) -> Behavior[PeerMsg]:
    """Actor managing a single bidirectional TCP connection to a peer.

    Starts a read loop as an asyncio task.  Incoming message frames are
    dispatched to *handler*.  On EOF or error the actor tells its parent
    ``PeerDown`` and stops.
    """

    async def setup(ctx: ActorContext[PeerMsg]) -> Behavior[PeerMsg]:
        async def read_loop() -> None:
            try:
                while True:
                    length_bytes = await reader.readexactly(4)
                    msg_len = struct.unpack("!I", length_bytes)[0]
                    frame_data = await reader.readexactly(msg_len)
                    frame_type = frame_data[0]
                    if frame_type == FRAME_MESSAGE:
                        await handler.on_message(frame_data[1:])
            except asyncio.CancelledError:
                return
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
                ctx.self.tell(PeerDisconnected())

        read_task = asyncio.get_running_loop().create_task(read_loop())

        async def receive(ctx: ActorContext[PeerMsg], msg: PeerMsg) -> Behavior[PeerMsg]:
            match msg:
                case SendFrame(data=data):
                    try:
                        writer.write(make_message_frame(data))
                        await writer.drain()
                    except (ConnectionError, OSError):
                        parent.tell(PeerDown(addr=addr, peer=ctx.self))
                        return Behaviors.stopped()
                    return Behaviors.same()

                case PeerDisconnected():
                    parent.tell(PeerDown(addr=addr, peer=ctx.self))
                    return Behaviors.stopped()

                case _:
                    return Behaviors.same()

        async def post_stop(ctx: ActorContext[PeerMsg]) -> None:
            read_task.cancel()
            try:
                await read_task
            except (asyncio.CancelledError, ConnectionError, OSError):
                pass
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

        return Behaviors.with_lifecycle(
            Behaviors.receive(receive),
            post_stop=post_stop,
        )

    return Behaviors.setup(setup)


@dataclass(frozen=True)
class MessageEnvelope:
    """Wire-format envelope for messages sent over TCP.

    Serializes to binary as ``[header_len:4][header_json][payload]``.

    Parameters
    ----------
    target : str
        URI of the target actor.
    sender : str
        URI of the sending actor or system.
    payload : bytes
        Serialized message body.
    type_hint : str
        Fully-qualified type name of the message.

    Examples
    --------
    >>> env = MessageEnvelope(target="casty://sys/user/a", sender="casty://sys/",
    ...                       payload=b'{"x":1}', type_hint="mymod.Msg")
    >>> data = env.to_bytes()
    >>> MessageEnvelope.from_bytes(data).target
    'casty://sys/user/a'
    """

    target: str
    sender: str
    payload: bytes
    type_hint: str

    def to_bytes(self) -> bytes:
        """Serialize the envelope to binary wire format.

        Returns
        -------
        bytes
            Binary data: ``[header_len:4][header_json][payload]``.
        """
        header = json.dumps({
            "target": self.target,
            "sender": self.sender,
            "type_hint": self.type_hint,
        }).encode("utf-8")
        return struct.pack("!I", len(header)) + header + self.payload

    @staticmethod
    def from_bytes(data: bytes) -> MessageEnvelope:
        """Deserialize a ``MessageEnvelope`` from binary wire format.

        Parameters
        ----------
        data : bytes
            Binary data produced by ``to_bytes()``.

        Returns
        -------
        MessageEnvelope
        """
        header_len = struct.unpack("!I", data[:4])[0]
        header_bytes = data[4 : 4 + header_len]
        payload = data[4 + header_len :]
        header: dict[str, str] = json.loads(header_bytes.decode("utf-8"))
        return MessageEnvelope(
            target=header["target"],
            sender=header["sender"],
            payload=payload,
            type_hint=header["type_hint"],
        )


@dataclass(frozen=True)
class TcpTransportConfig:
    host: str
    port: int
    self_address: NodeAddr | None = None
    client_only: bool = False
    server_ssl: ssl.SSLContext | None = None
    client_ssl: ssl.SSLContext | None = None
    connect_timeout: float = 2.0
    blacklist_duration: float = 5.0
    address_map: AddressMap = field(default_factory=lambda: {})


@dataclass(frozen=True)
class SendToNode:
    host: str
    port: int
    data: bytes


@dataclass(frozen=True)
class InboundAccepted:
    peer_addr: NodeAddr
    initiator: NodeAddr
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


@dataclass(frozen=True)
class ClearNodeBlacklist:
    host: str
    port: int


@dataclass(frozen=True)
class GetPort:
    reply_to: ActorRef[int]


type TcpTransportMsg = SendToNode | InboundAccepted | PeerDown | ClearNodeBlacklist | GetPort


def tcp_transport(
    config: TcpTransportConfig,
    handler: InboundHandler,
    logger: logging.Logger | None = None,
) -> Behavior[TcpTransportMsg]:
    """Actor managing TCP connections to all peers.

    On setup, starts a TCP server (unless ``client_only``).  The accept
    callback performs the handshake outside the actor, then tells itself
    ``InboundAccepted``.  Outbound connections are established inside
    the receive handler on first ``SendToNode`` to a new peer.
    """

    log = logger or logging.getLogger("casty.tcp")

    async def connect_and_handshake(
        peer_addr: NodeAddr,
        self_addr: NodeAddr,
        cfg: TcpTransportConfig,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, NodeAddr] | None:
        actual_host, actual_port = cfg.address_map.get(peer_addr, peer_addr)
        log.debug("TCP connect -> %s:%d", actual_host, actual_port)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(actual_host, actual_port, ssl=cfg.client_ssl),
                timeout=cfg.connect_timeout,
            )
        except (ConnectionError, OSError, TimeoutError):
            log.warning("Failed to connect to %s:%d", actual_host, actual_port)
            return None
        set_nodelay(writer)

        try:
            writer.write(make_handshake_frame(self_addr))
            await writer.drain()
        except (ConnectionError, OSError):
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            return None

        canonical = peer_addr
        try:
            length_bytes = await asyncio.wait_for(
                reader.readexactly(4), timeout=cfg.connect_timeout,
            )
            msg_len = struct.unpack("!I", length_bytes)[0]
            frame_data = await reader.readexactly(msg_len)
            frame_type = frame_data[0]
            if frame_type == FRAME_HANDSHAKE:
                hs: HandshakePayload = json.loads(frame_data[1:].decode("utf-8"))
                canonical = (hs["host"], int(hs["port"]))
                log.debug("Server handshake: canonical=%s:%d", canonical[0], canonical[1])
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError,
                asyncio.CancelledError, json.JSONDecodeError, KeyError, TimeoutError):
            log.warning("Failed to read handshake reply from %s:%d", peer_addr[0], peer_addr[1])

        return reader, writer, canonical

    async def setup(ctx: ActorContext[TcpTransportMsg]) -> Behavior[TcpTransportMsg]:
        server: asyncio.Server | None = None
        self_addr: NodeAddr = config.self_address or (config.host, config.port)

        if not config.client_only:
            async def accept_callback(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
            ) -> None:
                set_nodelay(writer)
                peer_addr: NodeAddr | None = None
                try:
                    length_bytes = await reader.readexactly(4)
                    msg_len = struct.unpack("!I", length_bytes)[0]
                    frame_data = await reader.readexactly(msg_len)
                    frame_type = frame_data[0]
                    if frame_type != FRAME_HANDSHAKE:
                        log.warning("Expected handshake, got frame_type=%d", frame_type)
                        writer.close()
                        return
                    hs: HandshakePayload = json.loads(frame_data[1:].decode("utf-8"))
                    peer_addr = (hs["host"], int(hs["port"]))
                except (asyncio.IncompleteReadError, ConnectionResetError, OSError,
                        asyncio.CancelledError, json.JSONDecodeError, KeyError):
                    log.warning("Handshake failed from inbound connection")
                    writer.close()
                    return

                log.debug("Inbound handshake from %s:%d", peer_addr[0], peer_addr[1])

                try:
                    writer.write(make_handshake_frame(self_addr))
                    await writer.drain()
                except (ConnectionError, OSError):
                    writer.close()
                    return

                ctx.self.tell(InboundAccepted(
                    peer_addr=peer_addr,
                    initiator=peer_addr,
                    reader=reader,
                    writer=writer,
                ))

            server = await asyncio.start_server(
                accept_callback, config.host, config.port, ssl=config.server_ssl,
            )
            actual_port: int = server.sockets[0].getsockname()[1]
            if config.port == 0 or actual_port != config.port:
                self_addr = (self_addr[0], actual_port)

        def active(
            srv: asyncio.Server | None,
            self_addr: NodeAddr,
            peers: dict[NodeAddr, ActorRef[PeerMsg]],
            aliases: dict[NodeAddr, NodeAddr],
            blacklist: dict[NodeAddr, float],
            next_id: int = 0,
        ) -> Behavior[TcpTransportMsg]:
            async def receive(
                ctx: ActorContext[TcpTransportMsg],
                msg: TcpTransportMsg,
            ) -> Behavior[TcpTransportMsg]:
                match msg:
                    case SendToNode(host=host, port=port, data=data):
                        peer_addr: NodeAddr = (host, port)
                        canonical = aliases.get(peer_addr, peer_addr)

                        effective_bl = blacklist
                        bl_until = blacklist.get(canonical)
                        if bl_until is not None:
                            if time.monotonic() < bl_until:
                                return Behaviors.same()
                            effective_bl = {k: v for k, v in blacklist.items() if k != canonical}

                        peer_ref = peers.get(canonical)
                        if peer_ref is not None:
                            peer_ref.tell(SendFrame(data=data))
                            return active(srv, self_addr, peers, aliases, effective_bl, next_id)

                        result = await connect_and_handshake(peer_addr, self_addr, config)
                        if result is None:
                            new_bl = {
                                **effective_bl,
                                canonical: time.monotonic() + config.blacklist_duration,
                            }
                            log.debug(
                                "Blacklisted %s:%d for %.1fs",
                                peer_addr[0], peer_addr[1], config.blacklist_duration,
                            )
                            return active(srv, self_addr, peers, aliases, new_bl, next_id)

                        conn_reader, conn_writer, server_canonical = result
                        new_aliases = aliases
                        if peer_addr != server_canonical:
                            new_aliases = {**aliases, peer_addr: server_canonical}

                        existing = peers.get(server_canonical)
                        if existing is not None:
                            if not resolve_race(self_addr, server_canonical, self_addr):
                                conn_writer.close()
                                existing.tell(SendFrame(data=data))
                                return active(srv, self_addr, peers, new_aliases, effective_bl, next_id)
                            ctx.stop(existing)

                        new_peer_ref = ctx.spawn(
                            peer_connection(
                                addr=server_canonical, writer=conn_writer, reader=conn_reader,
                                handler=handler, parent=ctx.self, logger=log,
                            ),
                            f"peer-{server_canonical[0]}-{server_canonical[1]}-{next_id}",
                        )
                        new_peer_ref.tell(SendFrame(data=data))
                        new_peers = {**peers, server_canonical: new_peer_ref}
                        return active(srv, self_addr, new_peers, new_aliases, effective_bl, next_id + 1)

                    case InboundAccepted(
                        peer_addr=pa, initiator=initiator,
                        reader=reader, writer=writer,
                    ):
                        existing = peers.get(pa)
                        if existing is not None:
                            if not resolve_race(self_addr, pa, initiator):
                                log.debug(
                                    "Race: keeping existing connection to %s:%d (inbound loses)",
                                    pa[0], pa[1],
                                )
                                writer.close()
                                return Behaviors.same()
                            log.debug(
                                "Race: replacing existing connection to %s:%d (inbound wins)",
                                pa[0], pa[1],
                            )
                            ctx.stop(existing)

                        peer_ref = ctx.spawn(
                            peer_connection(
                                addr=pa, writer=writer, reader=reader,
                                handler=handler, parent=ctx.self, logger=log,
                            ),
                            f"peer-{pa[0]}-{pa[1]}-{next_id}",
                        )
                        new_peers = {**peers, pa: peer_ref}
                        return active(srv, self_addr, new_peers, aliases, blacklist, next_id + 1)

                    case PeerDown(addr=pa, peer=peer_ref):
                        if peers.get(pa) is not peer_ref:
                            return Behaviors.same()
                        new_peers = {k: v for k, v in peers.items() if k != pa}
                        new_aliases = {k: v for k, v in aliases.items() if v != pa}
                        return active(srv, self_addr, new_peers, new_aliases, blacklist, next_id)

                    case ClearNodeBlacklist(host=host, port=port):
                        addr: NodeAddr = (host, port)
                        new_bl = {k: v for k, v in blacklist.items() if k != addr}
                        new_aliases = {k: v for k, v in aliases.items() if k != addr}
                        return active(srv, self_addr, peers, new_aliases, new_bl, next_id)

                    case GetPort(reply_to=reply_to):
                        if srv is not None and srv.sockets:
                            reply_to.tell(srv.sockets[0].getsockname()[1])
                        else:
                            reply_to.tell(self_addr[1])
                        return Behaviors.same()

            return Behaviors.receive(receive)

        async def post_stop(ctx: ActorContext[TcpTransportMsg]) -> None:
            if server is not None:
                server.close()

        return Behaviors.with_lifecycle(
            active(server, self_addr, {}, {}, {}),
            post_stop=post_stop,
        )

    return Behaviors.setup(setup)


class RemoteTransport:
    """Bridge between ``MessageTransport`` and the TCP transport actor.

    Routes local messages through ``LocalTransport`` and remote messages
    through TCP serialization via ``tell(SendToNode(...))``.
    """

    def __init__(
        self,
        *,
        local: LocalTransport,
        tcp: ActorRef[TcpTransportMsg],
        serializer: Serializer,
        local_host: str,
        local_port: int,
        system_name: str,
        advertised_host: str | None = None,
        advertised_port: int | None = None,
        task_runner: ActorRef[TaskRunnerMsg] | None = None,
        on_send_failure: Callable[[str, int], None] | None = None,
        local_node_id: str | None = None,
    ) -> None:
        self._local = local
        self._tcp = tcp
        self._serializer = serializer
        self._local_host = local_host
        self._local_port = local_port
        self._advertised_host = advertised_host
        self._advertised_port = advertised_port
        self._system_name = system_name
        self._task_runner = task_runner
        self._logger = logging.getLogger(f"casty.remote_transport.{system_name}")
        self._node_index: dict[str, NodeAddr] = {}
        self._local_node_id = local_node_id
        self._on_send_failure = on_send_failure

        self._serializer.set_ref_factory(self._make_ref_from_address)

    @property
    def sender_host(self) -> str:
        """Host used in outbound sender URIs (advertised or local)."""
        return self._advertised_host or self._local_host

    @property
    def sender_port(self) -> int:
        """Port used in outbound sender URIs (advertised or local)."""
        return self._advertised_port or self._local_port

    def update_node_index(self, index: dict[str, NodeAddr]) -> None:
        """Replace the node index used for ``node_id`` resolution."""
        self._node_index = index

    def _is_local(self, address: ActorAddress) -> bool:
        if address.node_id is not None:
            return address.node_id == self._local_node_id
        if address.host is None:
            return True
        return address.host == self._local_host and address.port == self._local_port

    def _resolve_address(self, address: ActorAddress) -> ActorAddress | None:
        if address.host is not None and address.port is not None:
            return address
        if address.node_id is not None:
            resolved = self._node_index.get(address.node_id)
            if resolved is not None:
                host, port = resolved
                return ActorAddress(
                    system=address.system, path=address.path,
                    host=host, port=port, node_id=address.node_id,
                )
        return None

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        """Deliver a message to the given address.

        Local addresses are routed through ``LocalTransport``; remote
        addresses are serialized and sent via TCP.
        """
        if self._is_local(address):
            self._local.deliver(address, msg)
        else:
            resolved = self._resolve_address(address)
            if resolved is None:
                self._logger.warning("Cannot resolve address: %s", address)
                return
            self._fire_remote(resolved, msg)

    def _fire_remote(self, address: ActorAddress, msg: Any) -> None:
        host = address.host
        port = address.port
        if host is None or port is None:
            self._logger.warning("Cannot send to address without host:port: %s", address)
            return
        try:
            payload = self._serializer.serialize(msg)
            msg_cls = msg.__class__
            type_name = f"{msg_cls.__module__}.{msg_cls.__qualname__}"
            envelope = MessageEnvelope(
                target=address.to_uri(),
                sender=f"casty://{self._system_name}@{self.sender_host}:{self.sender_port}/",
                payload=payload,
                type_hint=type_name,
            )
            self._logger.debug("Sending %s -> %s:%d%s", type_name, host, port, address.path)
            self._tcp.tell(SendToNode(host=host, port=port, data=envelope.to_bytes()))
        except Exception:
            self._logger.warning("Failed to serialize message to %s", address, exc_info=True)
            if self._on_send_failure is not None:
                self._on_send_failure(host, port)

    def make_ref(self, address: ActorAddress) -> ActorRef[Any]:
        """Create an ``ActorRef`` with the appropriate transport for the address."""
        from casty.ref import ActorRef as _ActorRef
        if self._is_local(address):
            return _ActorRef[Any](address=address, _transport=self._local)
        return _ActorRef[Any](address=address, _transport=self)

    def _make_ref_from_address(self, address: ActorAddress) -> Any:
        return self.make_ref(address)

    def clear_blacklist(self, host: str, port: int) -> None:
        """Clear the TCP circuit breaker for a specific endpoint."""
        self._tcp.tell(ClearNodeBlacklist(host=host, port=port))
