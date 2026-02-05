from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass
from typing import Protocol


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
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None
        self._handler: InboundHandler | None = None
        self._connections: dict[
            tuple[str, int], tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = {}

    @property
    def port(self) -> int:
        if self._server is not None:
            sockets = self._server.sockets
            if sockets:
                addr: tuple[str, int] = sockets[0].getsockname()
                return addr[1]
        return self._port

    async def start(self, handler: InboundHandler) -> None:
        self._handler = handler
        self._server = await asyncio.start_server(
            self._handle_connection, self._host, self._port
        )

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                length_bytes = await reader.readexactly(4)
                msg_len = struct.unpack("!I", length_bytes)[0]
                data = await reader.readexactly(msg_len)
                if self._handler is not None:
                    await self._handler.on_message(data)
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            pass
        finally:
            writer.close()

    async def send(self, host: str, port: int, data: bytes) -> None:
        key = (host, port)
        if key not in self._connections:
            reader, writer = await asyncio.open_connection(host, port)
            self._connections[key] = (reader, writer)
        _, writer = self._connections[key]
        writer.write(struct.pack("!I", len(data)) + data)
        await writer.drain()

    async def stop(self) -> None:
        for _, writer in self._connections.values():
            writer.close()
        self._connections.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
