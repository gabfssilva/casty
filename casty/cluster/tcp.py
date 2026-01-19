from __future__ import annotations

import asyncio
import socket
import struct
from dataclasses import dataclass

from typing import Any

from casty import Context, LocalActorRef

from .messages import (
    Handshake,
    HandshakeAck,
    TransportSend,
    TransportEvent,
)
from .serializable import deserialize
from .transport import Transport, Connect, Disconnect, TransportMessage


def _serialize(obj: object) -> bytes:
    codec = getattr(type(obj), "Codec", None)
    if codec is None:
        raise ValueError(f"{type(obj).__name__} is not serializable")
    return codec.serialize(obj)


@dataclass(frozen=True, slots=True)
class _ConnectionClosed:
    node_id: str


@dataclass(frozen=True, slots=True)
class _DataReceived:
    node_id: str
    data: bytes


@dataclass(slots=True)
class _IncomingConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    remote_node_id: str
    remote_address: tuple[str, int]


type _TcpInternal = _ConnectionClosed | _DataReceived | _IncomingConnection


class TcpTransport(Transport[_TcpInternal]):

    def __init__(
        self,
        cluster: LocalActorRef[TransportEvent],
        bind_address: tuple[str, int],
        node_id: str,
    ):
        super().__init__(cluster)
        self._bind_address = bind_address
        self._node_id = node_id
        self._connections: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._server: asyncio.Server | None = None
        self._accept_detached: Any = None
        self._read_detached: dict[str, Any] = {}

    async def on_start(self) -> None:
        host, port = self._bind_address
        self._server = await asyncio.start_server(
            self._handle_client,
            host,
            port,
            reuse_address=True,
        )
        self._accept_detached = self._ctx.detach(self._accept_loop())

    async def on_stop(self) -> None:
        if self._server:
            self._server.close()

        if self._accept_detached and not self._accept_detached.done():
            self._accept_detached.cancel()

        for detached in self._read_detached.values():
            if not detached.done():
                detached.cancel()

        for _, (_reader, writer) in list(self._connections.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._connections.clear()

        if self._server:
            await self._server.wait_closed()

    async def receive(self, msg: TransportMessage | _TcpInternal, ctx: Context) -> None:
        match msg:
            case TransportSend(node_id=node_id, payload=payload):
                await self._handle_send(node_id, payload)
            case Connect(node_id=node_id, address=address):
                await self._handle_connect(node_id, address, ctx)
            case Disconnect(node_id=node_id):
                await self._handle_disconnect(node_id)
            case _DataReceived(node_id=node_id, data=data):
                await self._handle_data_received(node_id, data)
            case _ConnectionClosed(node_id=node_id):
                await self._handle_connection_closed(node_id)
            case _IncomingConnection(reader=reader, writer=writer, remote_node_id=remote_node_id, remote_address=remote_address):
                await self._process_incoming_connection(reader, writer, remote_node_id, remote_address, ctx)

    async def _handle_send(self, node_id: str, payload: object) -> None:
        conn = self._connections.get(node_id)
        if conn is None:
            return

        _, writer = conn
        try:
            data = _serialize(payload)
            frame = struct.pack(">I", len(data)) + data
            writer.write(frame)
            await writer.drain()
        except Exception:
            pass

    async def _handle_connect(self, node_id: str, address: tuple[str, int], ctx: Context) -> None:
        if node_id in self._connections:
            return

        try:
            host, port = address
            reader, writer = await asyncio.open_connection(host, port)

            sock = writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            remote_node_id = await self._do_handshake(reader, writer)
            if remote_node_id is None:
                writer.close()
                await writer.wait_closed()
                return

            # Check again after handshake - another connection may have been established
            if remote_node_id in self._connections:
                writer.close()
                await writer.wait_closed()
                return

            self._connections[remote_node_id] = (reader, writer)
            self._read_detached[remote_node_id] = ctx.detach(self._read_loop(remote_node_id, reader))
            await self._notify_connected(remote_node_id, address)
        except Exception:
            pass

    async def _handle_disconnect(self, node_id: str) -> None:
        conn = self._connections.pop(node_id, None)
        if conn is None:
            return

        _, writer = conn
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    async def _handle_data_received(self, node_id: str, data: bytes) -> None:
        try:
            payload = deserialize(data)
            await self._notify_received(node_id, payload)
        except Exception:
            pass

    async def _handle_connection_closed(self, node_id: str) -> None:
        conn = self._connections.pop(node_id, None)
        if conn is not None:
            _, writer = conn
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        await self._notify_disconnected(node_id)

    async def _accept_loop(self) -> None:
        if self._server is None:
            return

        try:
            async with self._server:
                await self._server.serve_forever()
        except asyncio.CancelledError:
            pass

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Do handshake synchronously (so connecting side gets response)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        result = await self._handle_incoming_handshake(reader, writer)
        if result is None:
            writer.close()
            await writer.wait_closed()
            return

        remote_node_id, remote_address = result

        # Route connection storage through actor mailbox to serialize with _handle_connect
        await self._ctx.self_ref.send(_IncomingConnection(
            reader=reader,
            writer=writer,
            remote_node_id=remote_node_id,
            remote_address=remote_address,
        ))

    async def _process_incoming_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        remote_node_id: str,
        remote_address: tuple[str, int],
        ctx: Context,
    ) -> None:
        # Check if we already have a connection
        if remote_node_id in self._connections:
            writer.close()
            await writer.wait_closed()
            return

        self._connections[remote_node_id] = (reader, writer)
        await self._notify_connected(remote_node_id, remote_address)
        self._read_detached[remote_node_id] = ctx.detach(self._read_loop(remote_node_id, reader))

    async def _read_loop(self, node_id: str, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                length_bytes = await reader.readexactly(4)
                length = struct.unpack(">I", length_bytes)[0]

                if length > 10 * 1024 * 1024:
                    break

                data = await reader.readexactly(length)
                await self._ctx.self_ref.send(_DataReceived(node_id=node_id, data=data))
        except asyncio.IncompleteReadError:
            pass
        except Exception:
            pass
        finally:
            await self._ctx.self_ref.send(_ConnectionClosed(node_id=node_id))

    async def _do_handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> str | None:
        try:
            handshake = Handshake(node_id=self._node_id, address=self._bind_address)
            data = _serialize(handshake)
            frame = struct.pack(">I", len(data)) + data
            writer.write(frame)
            await writer.drain()

            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
            length = struct.unpack(">I", length_bytes)[0]

            if length > 10 * 1024 * 1024:
                return None

            response_data = await asyncio.wait_for(reader.readexactly(length), timeout=5.0)
            response = deserialize(response_data)

            if not isinstance(response, HandshakeAck):
                return None

            return response.node_id
        except Exception:
            return None

    async def _handle_incoming_handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> tuple[str, tuple[str, int]] | None:
        try:
            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
            length = struct.unpack(">I", length_bytes)[0]

            if length > 10 * 1024 * 1024:
                return None

            data = await asyncio.wait_for(reader.readexactly(length), timeout=5.0)
            msg = deserialize(data)

            if not isinstance(msg, Handshake):
                return None

            ack = HandshakeAck(node_id=self._node_id, address=self._bind_address)
            ack_data = _serialize(ack)
            frame = struct.pack(">I", len(ack_data)) + ack_data
            writer.write(frame)
            await writer.drain()

            return (msg.node_id, msg.address)
        except Exception:
            return None
