"""TCP-based remote transport for cross-node actor communication.

Provides ``TcpTransport`` for raw TCP framing, ``MessageEnvelope`` for
wire-format serialization, and ``RemoteTransport`` which bridges the
``MessageTransport`` protocol to TCP for remote addresses.

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
from typing import TYPE_CHECKING, Any, Protocol

from casty.address import ActorAddress
from casty.serialization import Serializer
from casty.transport import LocalTransport

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.task_runner import TaskRunnerMsg


type AddressMap = dict[tuple[str, int], tuple[str, int]]

FRAME_HANDSHAKE: int = 0x00
FRAME_MESSAGE: int = 0x01


class InboundHandler(Protocol):
    async def on_message(self, data: bytes) -> None: ...


@dataclass
class PeerConnection:
    """A bidirectional TCP connection to a peer node."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    read_task: asyncio.Task[None]
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    initiator: tuple[str, int] = ("", 0)


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
        # Format: [header_len:4][header][payload]
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


class TcpTransport:
    """Low-level TCP transport with bidirectional connections.

    Wire format: ``[msg_len:4][frame_type:1][payload]``.  Each node pair
    shares a single TCP connection used for both reading and writing.
    Simultaneous-connect races are resolved deterministically.

    Parameters
    ----------
    host : str
        Bind address for the server.
    port : int
        Bind port (use 0 for OS-assigned).
    self_address : tuple[str, int] or None
        Logical ``(host, port)`` identifying this node.  Defaults to
        ``(host, resolved_port)`` lazily after ``start()``.
    client_only : bool
        When ``True``, ``start()`` sets the handler but does **not** bind a
        TCP server.  Used by ``ClusterClient`` which only dials out.
    logger : logging.Logger or None
        Logger instance. Defaults to ``casty.tcp``.
    server_ssl : ssl.SSLContext or None
        TLS context for the inbound server.
    client_ssl : ssl.SSLContext or None
        TLS context for outbound connections.
    address_map : dict[tuple[str, int], tuple[str, int]] or None
        Optional mapping from logical (host, port) to actual (host, port)
        for SSH tunnels or NAT.

    Examples
    --------
    >>> tcp = TcpTransport("127.0.0.1", 0)
    >>> # await tcp.start(handler)  # starts listening
    >>> # await tcp.send(host, port, data)  # sends raw bytes
    >>> # await tcp.stop()
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        self_address: tuple[str, int] | None = None,
        client_only: bool = False,
        logger: logging.Logger | None = None,
        server_ssl: ssl.SSLContext | None = None,
        client_ssl: ssl.SSLContext | None = None,
        connect_timeout: float = 2.0,
        blacklist_duration: float = 5.0,
        address_map: AddressMap | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._self_address = self_address
        self._client_only = client_only
        self._server: asyncio.Server | None = None
        self._handler: InboundHandler | None = None
        self._logger = logger or logging.getLogger("casty.tcp")
        self._server_ssl = server_ssl
        self._client_ssl = client_ssl
        self._connect_timeout = connect_timeout
        self._blacklist_duration = blacklist_duration
        self._address_map: AddressMap = address_map or {}
        self._blacklist: dict[tuple[str, int], float] = {}
        self._peers: dict[tuple[str, int], PeerConnection] = {}
        self._peer_aliases: dict[tuple[str, int], tuple[str, int]] = {}
        self._pending_accept: list[asyncio.StreamWriter] = []
        self._connect_locks: dict[tuple[str, int], asyncio.Lock] = {}

    @property
    def host(self) -> str:
        """Return the bind address of this transport."""
        return self._host

    @property
    def port(self) -> int:
        """Return the actual listening port (resolved after bind)."""
        if self._server is not None:
            sockets = self._server.sockets
            if sockets:
                addr: tuple[str, int] = sockets[0].getsockname()
                return addr[1]
        return self._port

    def set_self_address(self, host: str, port: int) -> None:
        """Update self address after port resolution."""
        self._self_address = (host, port)

    def _get_self_address(self) -> tuple[str, int]:
        if self._self_address is not None:
            return self._self_address
        return (self._host, self.port)

    @staticmethod
    def _set_nodelay(writer: asyncio.StreamWriter) -> None:
        sock = writer.transport.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    async def start(self, handler: InboundHandler) -> None:
        """Start the TCP transport.

        When ``client_only=False`` (default), binds a TCP server for
        inbound connections.  When ``client_only=True``, only sets the
        handler (needed for the read loop on outbound connections).

        Parameters
        ----------
        handler : InboundHandler
            Callback object whose ``on_message`` is called for each
            inbound message frame.
        """
        self._handler = handler
        if not self._client_only:
            self._server = await asyncio.start_server(
                self._handle_inbound, self._host, self._port, ssl=self._server_ssl
            )

    def _make_handshake_frame(self) -> bytes:
        sa = self._get_self_address()
        payload = json.dumps({"host": sa[0], "port": sa[1]}).encode("utf-8")
        frame = bytes([FRAME_HANDSHAKE]) + payload
        return struct.pack("!I", len(frame)) + frame

    @staticmethod
    def _make_message_frame(data: bytes) -> bytes:
        frame = bytes([FRAME_MESSAGE]) + data
        return struct.pack("!I", len(frame)) + frame

    async def _handle_inbound(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a new inbound TCP connection (server side).

        Reads the handshake frame to identify the peer, resolves any
        simultaneous-connect race, then enters the read loop.
        """
        self._set_nodelay(writer)
        self._pending_accept.append(writer)
        peer_addr: tuple[str, int] | None = None
        try:
            length_bytes = await reader.readexactly(4)
            msg_len = struct.unpack("!I", length_bytes)[0]
            frame_data = await reader.readexactly(msg_len)
            frame_type = frame_data[0]
            if frame_type != FRAME_HANDSHAKE:
                self._logger.warning("Expected handshake, got frame_type=%d", frame_type)
                return
            handshake: dict[str, Any] = json.loads(frame_data[1:].decode("utf-8"))
            peer_addr = (handshake["host"], int(handshake["port"]))
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError, asyncio.CancelledError, json.JSONDecodeError, KeyError):
            return
        finally:
            if writer in self._pending_accept:
                self._pending_accept.remove(writer)

        self._logger.debug("Inbound handshake from %s:%d", peer_addr[0], peer_addr[1])

        # Send our own handshake back so the client learns our canonical address
        try:
            writer.write(self._make_handshake_frame())
            await writer.drain()
        except (ConnectionError, OSError):
            writer.close()
            return

        initiator = peer_addr
        existing = self._peers.get(peer_addr)
        if existing is not None:
            if not self._resolve_race(peer_addr, initiator):
                self._logger.debug(
                    "Race: keeping existing connection to %s:%d (inbound loses)",
                    peer_addr[0], peer_addr[1],
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
                return
            self._logger.debug(
                "Race: replacing existing connection to %s:%d (inbound wins)",
                peer_addr[0], peer_addr[1],
            )
            existing.read_task.cancel()
            existing.writer.close()

        task = asyncio.get_running_loop().create_task(
            self._read_loop(peer_addr, reader, writer)
        )
        self._peers[peer_addr] = PeerConnection(
            reader=reader, writer=writer, read_task=task, initiator=initiator,
        )

    async def _connect_and_handshake(
        self, peer_addr: tuple[str, int],
    ) -> tuple[PeerConnection, tuple[str, int]] | None:
        """Open an outbound connection, send handshake, read server handshake, start read loop.

        Returns the ``PeerConnection`` together with the server's canonical
        address (from its handshake reply).  The caller should register the
        peer under that canonical address rather than the logical
        ``peer_addr``.
        """
        actual_host, actual_port = self._address_map.get(peer_addr, peer_addr)
        self._logger.debug("TCP connect -> %s:%d", actual_host, actual_port)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(actual_host, actual_port, ssl=self._client_ssl),
                timeout=self._connect_timeout,
            )
        except (ConnectionError, OSError, TimeoutError):
            return None
        self._set_nodelay(writer)

        try:
            writer.write(self._make_handshake_frame())
            await writer.drain()
        except (ConnectionError, OSError):
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            return None

        # Read the server's handshake reply to learn its canonical address
        canonical = peer_addr
        try:
            length_bytes = await asyncio.wait_for(
                reader.readexactly(4), timeout=self._connect_timeout,
            )
            msg_len = struct.unpack("!I", length_bytes)[0]
            frame_data = await reader.readexactly(msg_len)
            frame_type = frame_data[0]
            if frame_type == FRAME_HANDSHAKE:
                handshake: dict[str, Any] = json.loads(frame_data[1:].decode("utf-8"))
                canonical = (handshake["host"], int(handshake["port"]))
                self._logger.debug(
                    "Server handshake: canonical=%s:%d", canonical[0], canonical[1],
                )
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError,
                asyncio.CancelledError, json.JSONDecodeError, KeyError, TimeoutError):
            # If we can't read the server handshake, fall back to peer_addr
            pass

        sa = self._get_self_address()
        task = asyncio.get_running_loop().create_task(
            self._read_loop(canonical, reader, writer)
        )
        return PeerConnection(
            reader=reader, writer=writer, read_task=task, initiator=sa,
        ), canonical

    def _resolve_race(
        self,
        peer_addr: tuple[str, int],
        new_initiator: tuple[str, int],
    ) -> bool:
        """Return True if the new connection should replace the existing one.

        The connection initiated by ``min(self_address, peer_addr)`` wins.
        """
        sa = self._get_self_address()
        winner_initiator = min(sa, peer_addr)
        return new_initiator == winner_initiator

    async def _read_loop(
        self,
        peer_addr: tuple[str, int],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Read frames from a peer connection, dispatch message frames."""
        try:
            while True:
                length_bytes = await reader.readexactly(4)
                msg_len = struct.unpack("!I", length_bytes)[0]
                frame_data = await reader.readexactly(msg_len)
                frame_type = frame_data[0]
                if frame_type == FRAME_MESSAGE and self._handler is not None:
                    await self._handler.on_message(frame_data[1:])
                elif frame_type == FRAME_HANDSHAKE:
                    pass
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError, asyncio.CancelledError):
            pass
        finally:
            self._remove_peer(peer_addr, writer)

    def _remove_peer(self, peer_addr: tuple[str, int], writer: asyncio.StreamWriter) -> None:
        """Clean up a disconnected peer and any aliases pointing to it."""
        existing = self._peers.get(peer_addr)
        if existing is not None and existing.writer is writer:
            del self._peers[peer_addr]
            # Remove any aliases that pointed to this canonical address
            stale = [k for k, v in self._peer_aliases.items() if v == peer_addr]
            for k in stale:
                del self._peer_aliases[k]
        writer.close()

    async def send(self, host: str, port: int, data: bytes) -> None:
        """Send raw bytes to a remote host using the bidirectional connection.

        Opens a new connection + handshake if needed. Blacklists the
        endpoint on failure to prevent connection storms to dead nodes.

        Parameters
        ----------
        host : str
            Remote hostname (logical — may be translated by address_map).
        port : int
            Remote port (logical — may be translated by address_map).
        data : bytes
            Raw bytes to send (will be wrapped in a message frame).
        """
        peer_addr = (host, port)

        # Resolve through alias to find the canonical address
        canonical = self._peer_aliases.get(peer_addr, peer_addr)

        blacklisted_until = self._blacklist.get(canonical)
        if blacklisted_until is not None:
            if time.monotonic() < blacklisted_until:
                return
            del self._blacklist[canonical]

        try:
            peer = self._peers.get(canonical)
            if peer is None:
                lock = self._connect_locks.setdefault(peer_addr, asyncio.Lock())
                async with lock:
                    # Re-check alias — may have been set by concurrent connect
                    canonical = self._peer_aliases.get(peer_addr, peer_addr)
                    peer = self._peers.get(canonical)
                    if peer is None:
                        result = await self._connect_and_handshake(peer_addr)
                        if result is None:
                            self._blacklist[canonical] = (
                                time.monotonic() + self._blacklist_duration
                            )
                            self._logger.debug(
                                "Blacklisted %s:%d for %.1fs",
                                host, port, self._blacklist_duration,
                            )
                            return
                        peer, server_canonical = result
                        canonical = server_canonical

                        # Store alias if logical address differs from canonical
                        if peer_addr != canonical:
                            self._peer_aliases[peer_addr] = canonical

                        existing = self._peers.get(canonical)
                        if existing is not None:
                            if not self._resolve_race(canonical, peer.initiator):
                                peer.read_task.cancel()
                                peer.writer.close()
                                peer = existing
                            else:
                                existing.read_task.cancel()
                                existing.writer.close()
                                self._peers[canonical] = peer
                        else:
                            self._peers[canonical] = peer
                self._connect_locks.pop(peer_addr, None)

            async with peer.write_lock:
                peer.writer.write(self._make_message_frame(data))
                await peer.writer.drain()
        except (ConnectionError, OSError, TimeoutError):
            self._peers.pop(canonical, None)
            self._blacklist[canonical] = time.monotonic() + self._blacklist_duration
            self._logger.debug(
                "Blacklisted %s:%d for %.1fs",
                host, port, self._blacklist_duration,
            )

    async def stop(self) -> None:
        """Close all connections and shut down the server."""
        if self._server is not None:
            self._server.close()
        for peer in list(self._peers.values()):
            peer.read_task.cancel()
            peer.writer.close()
            try:
                await peer.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        self._peers.clear()
        self._peer_aliases.clear()
        for writer in list(self._pending_accept):
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        self._pending_accept.clear()
        if self._server is not None:
            await self._server.wait_closed()

    def clear_blacklist(self, host: str, port: int) -> None:
        """Remove a host from the circuit breaker blacklist.

        Called when a node rejoins so the system can immediately communicate
        with it.

        Parameters
        ----------
        host : str
            Remote hostname.
        port : int
            Remote port.
        """
        addr = (host, port)
        self._blacklist.pop(addr, None)
        # Also clear any alias so a fresh connection can re-discover canonical
        self._peer_aliases.pop(addr, None)


class RemoteTransport:
    """Bridge between ``MessageTransport`` and ``TcpTransport``.

    Routes local messages through ``LocalTransport`` and remote messages
    through TCP serialization. Also acts as an ``InboundHandler`` to
    receive and deserialize inbound TCP frames.

    Parameters
    ----------
    local : LocalTransport
        Transport for in-process delivery.
    tcp : TcpTransport
        Transport for remote delivery.
    serializer : Serializer
        Serializer for encoding/decoding messages.
    local_host : str
        This node's hostname.
    local_port : int
        This node's TCP port.
    system_name : str
        Name of the actor system.

    Examples
    --------
    >>> rt = RemoteTransport(
    ...     local=local_transport, tcp=tcp_transport,
    ...     serializer=json_ser, local_host="127.0.0.1",
    ...     local_port=25520, system_name="my-system",
    ... )
    >>> # await rt.start()  # binds TCP server
    >>> # rt.deliver(address, msg)  # routes local or remote
    """

    def __init__(
        self,
        *,
        local: LocalTransport,
        tcp: TcpTransport,
        serializer: Serializer,
        local_host: str,
        local_port: int,
        system_name: str,
        advertised_host: str | None = None,
        advertised_port: int | None = None,
        task_runner: ActorRef[TaskRunnerMsg] | None = None,
        on_send_failure: Callable[[str, int], None] | None = None,
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
        self._node_index: dict[str, tuple[str, int]] = {}
        self._local_node_id: str | None = None
        self._on_send_failure = on_send_failure

        # Wire ref_factory so deserialized ActorRefs get the right transport
        self._serializer.set_ref_factory(self._make_ref_from_address)

    @property
    def sender_host(self) -> str:
        """Host used in outbound sender URIs (advertised or local)."""
        return self._advertised_host or self._local_host

    @property
    def sender_port(self) -> int:
        """Port used in outbound sender URIs (advertised or local)."""
        return self._advertised_port or self._local_port

    def set_task_runner(self, task_runner: ActorRef[TaskRunnerMsg]) -> None:
        """Set the task runner ref after system bootstrap.

        Parameters
        ----------
        task_runner : ActorRef[TaskRunnerMsg]
            Reference to the system's task runner actor.
        """
        self._task_runner = task_runner

    def set_local_node_id(self, node_id: str) -> None:
        """Set the local node ID for ``node_id``-based locality checks.

        Parameters
        ----------
        node_id : str
            This node's human-readable identifier.
        """
        self._local_node_id = node_id

    def update_node_index(self, index: dict[str, tuple[str, int]]) -> None:
        """Replace the node index used for ``node_id`` resolution.

        Parameters
        ----------
        index : dict[str, tuple[str, int]]
            Mapping from node ID to ``(host, port)``.
        """
        self._node_index = index

    def _is_local(self, address: ActorAddress) -> bool:
        if address.node_id is not None:
            return address.node_id == self._local_node_id
        if address.host is None:
            return True
        return address.host == self._local_host and address.port == self._local_port

    def _resolve_address(self, address: ActorAddress) -> ActorAddress | None:
        """Resolve a ``node_id``-based address to one with ``host`` and ``port``."""
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
        addresses are serialized and sent via TCP.  Addresses using
        ``node_id`` are resolved from the shared node index.

        Parameters
        ----------
        address : ActorAddress
            Target actor address.
        msg : Any
            The message to deliver.
        """
        if self._is_local(address):
            self._local.deliver(address, msg)
        else:
            resolved = self._resolve_address(address)
            if resolved is None:
                self._logger.warning("Cannot resolve address: %s", address)
                return
            if self._task_runner is not None:
                from casty.task_runner import RunTask

                self._task_runner.tell(RunTask(self._send_remote, args=(resolved, msg)))
            else:
                try:
                    asyncio.get_running_loop().create_task(self._send_remote(resolved, msg))
                except RuntimeError:
                    self._logger.warning("No running loop for remote deliver to %s", address)

    async def _send_remote(self, address: ActorAddress, msg: Any) -> None:
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
            await self._tcp.send(host, port, envelope.to_bytes())
        except (ConnectionError, OSError):
            self._logger.debug("Cannot reach %s:%s (connection refused or reset)", host, port)
            if self._on_send_failure is not None:
                self._on_send_failure(host, port)
        except Exception:
            self._logger.warning("Failed to send remote message to %s", address, exc_info=True)

    async def on_message(self, data: bytes) -> None:
        """Handle an inbound TCP frame by deserializing and delivering locally.

        Parameters
        ----------
        data : bytes
            Raw frame bytes from ``TcpTransport``.
        """
        try:
            envelope = MessageEnvelope.from_bytes(data)
            msg = self._serializer.deserialize(envelope.payload)
            target_addr = ActorAddress.from_uri(envelope.target)
            self._logger.debug("Received %s -> %s", envelope.type_hint, target_addr.path)
            # Deliver to local transport at the target path
            self._local.deliver(target_addr, msg)
        except Exception:
            self._logger.warning("Failed to handle inbound message", exc_info=True)

    def make_ref(self, address: ActorAddress) -> ActorRef[Any]:
        """Create an ``ActorRef`` with the appropriate transport for the address.

        Local addresses get ``LocalTransport``; remote addresses get this
        ``RemoteTransport``.

        Parameters
        ----------
        address : ActorAddress
            Target actor address.

        Returns
        -------
        ActorRef[Any]
        """
        from casty.ref import ActorRef as _ActorRef
        if self._is_local(address):
            return _ActorRef[Any](address=address, _transport=self._local)
        return _ActorRef[Any](address=address, _transport=self)

    def _make_ref_from_address(self, address: ActorAddress) -> Any:
        """ref_factory callback for deserializer."""
        return self.make_ref(address)

    def set_local_port(self, port: int) -> None:
        """Update local port after binding to an OS-assigned port.

        Parameters
        ----------
        port : int
            The actual port assigned by the OS.
        """
        self._local_port = port

    def clear_blacklist(self, host: str, port: int) -> None:
        """Clear the TCP circuit breaker for a specific endpoint.

        Called when a node rejoins the cluster so it can be reached immediately.

        Parameters
        ----------
        host : str
            Remote hostname.
        port : int
            Remote port.
        """
        self._tcp.clear_blacklist(host, port)

    async def start(self) -> None:
        """Start the underlying TCP server and begin accepting connections."""
        await self._tcp.start(self)
        self._logger.info("Started on %s:%d", self._local_host, self._tcp.port)

    async def stop(self) -> None:
        """Shut down the TCP server and close all connections."""
        self._logger.info("Stopping")
        await self._tcp.stop()
