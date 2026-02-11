"""TCP-based remote transport for cross-node actor communication.

Provides ``TcpTransport`` for raw TCP framing, ``MessageEnvelope`` for
wire-format serialization, and ``RemoteTransport`` which bridges the
``MessageTransport`` protocol to TCP for remote addresses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import ssl
import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from casty.address import ActorAddress
from casty.serialization import Serializer
from casty.transport import LocalTransport

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.task_runner import TaskRunnerMsg


class InboundHandler(Protocol):
    async def on_message(self, data: bytes) -> None: ...


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
    """Low-level TCP transport with binary length-prefix framing.

    Each message is sent as ``[msg_len:4][msg_bytes]``. Manages a connection
    pool for outbound connections and an asyncio server for inbound.

    Parameters
    ----------
    host : str
        Bind address for the server.
    port : int
        Bind port (use 0 for OS-assigned).
    logger : logging.Logger or None
        Logger instance. Defaults to ``casty.tcp``.
    server_ssl : ssl.SSLContext or None
        TLS context for the inbound server.
    client_ssl : ssl.SSLContext or None
        TLS context for outbound connections.

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
        logger: logging.Logger | None = None,
        server_ssl: ssl.SSLContext | None = None,
        client_ssl: ssl.SSLContext | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None
        self._handler: InboundHandler | None = None
        self._logger = logger or logging.getLogger("casty.tcp")
        self._server_ssl = server_ssl
        self._client_ssl = client_ssl
        self._connections: dict[
            tuple[str, int], tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = {}
        self._inbound_writers: list[asyncio.StreamWriter] = []

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

    @staticmethod
    def _set_nodelay(writer: asyncio.StreamWriter) -> None:
        sock = writer.transport.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    async def start(self, handler: InboundHandler) -> None:
        """Start the TCP server and begin accepting connections.

        Parameters
        ----------
        handler : InboundHandler
            Callback object whose ``on_message`` is called for each inbound frame.
        """
        self._handler = handler
        self._server = await asyncio.start_server(
            self._handle_connection, self._host, self._port, ssl=self._server_ssl
        )

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._set_nodelay(writer)
        self._inbound_writers.append(writer)
        try:
            while True:
                length_bytes = await reader.readexactly(4)
                msg_len = struct.unpack("!I", length_bytes)[0]
                data = await reader.readexactly(msg_len)
                if self._handler is not None:
                    await self._handler.on_message(data)
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            if writer in self._inbound_writers:
                self._inbound_writers.remove(writer)

    async def send(self, host: str, port: int, data: bytes) -> None:
        """Send raw bytes to a remote host with length-prefix framing.

        Opens a new connection if needed. Retries once on connection failure.

        Parameters
        ----------
        host : str
            Remote hostname.
        port : int
            Remote port.
        data : bytes
            Raw bytes to send.
        """
        key = (host, port)
        try:
            if key not in self._connections:
                self._logger.debug("TCP connect -> %s:%d", host, port)
                reader, writer = await asyncio.open_connection(host, port, ssl=self._client_ssl)
                self._set_nodelay(writer)
                self._connections[key] = (reader, writer)
            _, writer = self._connections[key]
            writer.write(struct.pack("!I", len(data)) + data)
            await writer.drain()
        except (ConnectionError, OSError):
            # Drop stale connection and retry once
            self._connections.pop(key, None)
            self._logger.warning("TCP reconnect -> %s:%d", host, port)
            reader, writer = await asyncio.open_connection(host, port, ssl=self._client_ssl)
            self._set_nodelay(writer)
            self._connections[key] = (reader, writer)
            writer.write(struct.pack("!I", len(data)) + data)
            await writer.drain()

    async def stop(self) -> None:
        """Close all connections and shut down the server."""
        for _, writer in self._connections.values():
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        self._connections.clear()
        for writer in self._inbound_writers:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        self._inbound_writers.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


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
        task_runner: ActorRef[TaskRunnerMsg] | None = None,
    ) -> None:
        self._local = local
        self._tcp = tcp
        self._serializer = serializer
        self._local_host = local_host
        self._local_port = local_port
        self._system_name = system_name
        self._task_runner = task_runner
        self._logger = logging.getLogger(f"casty.remote_transport.{system_name}")
        self._node_index: dict[str, tuple[str, int]] = {}
        self._local_node_id: str | None = None

        # Wire ref_factory so deserialized ActorRefs get the right transport
        self._serializer.set_ref_factory(self._make_ref_from_address)

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

                self._task_runner.tell(RunTask(self._send_remote(resolved, msg)))
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
                sender=f"casty://{self._system_name}@{self._local_host}:{self._local_port}/",
                payload=payload,
                type_hint=type_name,
            )
            self._logger.debug("Sending %s -> %s:%d%s", type_name, host, port, address.path)
            await self._tcp.send(host, port, envelope.to_bytes())
        except (ConnectionError, OSError):
            self._logger.debug("Cannot reach %s:%s (connection refused or reset)", host, port)
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

    async def start(self) -> None:
        """Start the underlying TCP server and begin accepting connections."""
        await self._tcp.start(self)
        self._logger.info("Started on %s:%d", self._local_host, self._tcp.port)

    async def stop(self) -> None:
        """Shut down the TCP server and close all connections."""
        self._logger.info("Stopping")
        await self._tcp.stop()
