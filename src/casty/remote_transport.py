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
from casty.serialization import JsonSerializer
from casty.transport import LocalTransport

if TYPE_CHECKING:
    from casty.ref import ActorRef


class InboundHandler(Protocol):
    async def on_message(self, data: bytes) -> None: ...


@dataclass(frozen=True)
class MessageEnvelope:
    target: str
    sender: str
    payload: bytes
    type_hint: str

    def to_bytes(self) -> bytes:
        header = json.dumps({
            "target": self.target,
            "sender": self.sender,
            "type_hint": self.type_hint,
        }).encode("utf-8")
        # Format: [header_len:4][header][payload]
        return struct.pack("!I", len(header)) + header + self.payload

    @staticmethod
    def from_bytes(data: bytes) -> MessageEnvelope:
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
        return self._host

    @property
    def port(self) -> int:
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
        for _, writer in self._connections.values():
            writer.close()
            await writer.wait_closed()
        self._connections.clear()
        for writer in self._inbound_writers:
            writer.close()
            await writer.wait_closed()
        self._inbound_writers.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


class RemoteTransport:
    """Bridges MessageTransport protocol to TcpTransport for remote addresses."""

    def __init__(
        self,
        *,
        local: LocalTransport,
        tcp: TcpTransport,
        serializer: JsonSerializer,
        local_host: str,
        local_port: int,
        system_name: str,
    ) -> None:
        self._local = local
        self._tcp = tcp
        self._serializer = serializer
        self._local_host = local_host
        self._local_port = local_port
        self._system_name = system_name
        self._logger = logging.getLogger(f"casty.remote_transport.{system_name}")

        # Wire ref_factory so deserialized ActorRefs get the right transport
        self._serializer.set_ref_factory(self._make_ref_from_address)

    def _is_local(self, address: ActorAddress) -> bool:
        if address.host is None:
            return True
        return address.host == self._local_host and address.port == self._local_port

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        """MessageTransport protocol. Local -> LocalTransport, remote -> TCP."""
        if self._is_local(address):
            self._local.deliver(address, msg)
        else:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._send_remote(address, msg))
            except RuntimeError:
                self._logger.warning("No running loop for remote deliver to %s", address)

    _PRIMITIVE_TYPES: frozenset[type] = frozenset({int, float, str, bool, type(None)})

    async def _send_remote(self, address: ActorAddress, msg: Any) -> None:
        host = address.host
        port = address.port
        if host is None or port is None:
            self._logger.warning("Cannot send to address without host:port: %s", address)
            return
        try:
            payload = self._serializer.serialize(msg)
            msg_type = type(msg)  # pyright: ignore[reportUnknownVariableType]
            if msg_type in self._PRIMITIVE_TYPES:
                type_name = f"builtins.{msg_type.__name__}"
            else:
                type_name = self._serializer.registry.type_name(msg_type)  # pyright: ignore[reportUnknownArgumentType]
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
        """InboundHandler protocol. TCP -> deserialize -> LocalTransport."""
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
        """Create ActorRef with appropriate transport for the address."""
        from casty.ref import ActorRef as _ActorRef
        if self._is_local(address):
            return _ActorRef[Any](address=address, _transport=self._local)
        return _ActorRef[Any](address=address, _transport=self)

    def _make_ref_from_address(self, address: ActorAddress) -> Any:
        """ref_factory callback for deserializer."""
        return self.make_ref(address)

    def set_local_port(self, port: int) -> None:
        """Update local port after binding to OS-assigned port."""
        self._local_port = port

    async def start(self) -> None:
        await self._tcp.start(self)
        self._logger.info("Started on %s:%d", self._local_host, self._tcp.port)

    async def stop(self) -> None:
        self._logger.info("Stopping")
        await self._tcp.stop()
