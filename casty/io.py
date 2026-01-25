from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, TYPE_CHECKING

from .core import actor, Mailbox, Envelope

if TYPE_CHECKING:
    from .core import ActorRef


class Framer(Protocol):
    def feed(self, data: bytes) -> list[bytes]: ...
    def encode(self, data: bytes) -> bytes: ...


class RawFramer:
    def feed(self, data: bytes) -> list[bytes]:
        return [data]

    def encode(self, data: bytes) -> bytes:
        return data


class LengthPrefixedFramer:
    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buffer.extend(data)
        frames: list[bytes] = []
        while len(self._buffer) >= 4:
            length = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + length:
                break
            frames.append(bytes(self._buffer[4 : 4 + length]))
            del self._buffer[: 4 + length]
        return frames

    def encode(self, data: bytes) -> bytes:
        return len(data).to_bytes(4, "big") + data


from .messages import (
    Bind, IoConnect,
    Bound, BindFailed, IoConnected, IoConnectFailed,
    Register, Write, Close,
    Received, PeerClosed, ErrorClosed, Aborted,
    InboundEvent, OutboundEvent,
)


@dataclass
class _InternalReceived:
    data: bytes


@dataclass
class _InternalClosed:
    reason: PeerClosed | ErrorClosed | Aborted


@actor
async def tcp_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    initial_handler: "ActorRef[InboundEvent]",
    framing: Framer,
    *,
    mailbox: Mailbox[OutboundEvent | _InternalReceived | _InternalClosed],
):
    handler = initial_handler

    async def recv_loop():
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    await mailbox.put(Envelope(payload=_InternalClosed(PeerClosed())))
                    return
                await mailbox.put(Envelope(payload=_InternalReceived(data)))
        except ConnectionResetError:
            await mailbox.put(Envelope(payload=_InternalClosed(Aborted())))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await mailbox.put(Envelope(payload=_InternalClosed(ErrorClosed(str(e)))))

    recv_task = asyncio.create_task(recv_loop())

    self_ref = None
    async for msg, ctx in mailbox:
        if self_ref is None:
            self_ref = ctx._self_ref

        match msg:
            case Register(new_handler):
                handler = new_handler

            case Write(data):
                encoded = framing.encode(data)
                writer.write(encoded)
                await writer.drain()

            case Close():
                recv_task.cancel()
                try:
                    await recv_task
                except asyncio.CancelledError:
                    pass
                writer.close()
                await writer.wait_closed()
                break

            case _InternalReceived(data):
                for frame in framing.feed(data):
                    await handler.send(Received(frame), sender=self_ref)

            case _InternalClosed(reason):
                await handler.send(reason)
                writer.close()
                await writer.wait_closed()
                break


@dataclass
class _AcceptedConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


@actor
async def tcp(*, mailbox: Mailbox[Bind | IoConnect]):
    listener_count = 0
    conn_count = 0

    async for msg, ctx in mailbox:
        match msg:
            case Bind(handler, port, host, framing):
                try:
                    await ctx.actor(
                        tcp_listener(handler, host, port, framing),
                        name=f"listener-{listener_count}"
                    )
                    listener_count += 1
                except Exception as e:
                    await handler.send(BindFailed(str(e)))

            case IoConnect(handler, host, port, framing):
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    local = writer.get_extra_info("sockname")
                    remote = writer.get_extra_info("peername")
                    framer = framing or RawFramer()
                    conn = await ctx.actor(
                        tcp_connection(reader, writer, handler, framer),
                        name=f"conn-out-{conn_count}"
                    )
                    conn_count += 1
                    await handler.send(IoConnected(conn, remote, local))
                except Exception as e:
                    await handler.send(IoConnectFailed(str(e)))


@actor
async def tcp_listener(
    handler: "ActorRef[InboundEvent]",
    host: str,
    port: int,
    framing: "Framer | None",
    *,
    mailbox: Mailbox[_AcceptedConnection],
):
    framer = framing or RawFramer()
    conn_count = 0

    async def on_accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await mailbox.put(Envelope(payload=_AcceptedConnection(reader, writer)))

    try:
        server = await asyncio.start_server(on_accept, host, port)
        local = server.sockets[0].getsockname()
        await handler.send(Bound(local))
    except Exception as e:
        await handler.send(BindFailed(str(e)))
        return

    async for msg, ctx in mailbox:
        match msg:
            case _AcceptedConnection(reader, writer):
                remote = writer.get_extra_info("peername")
                local = writer.get_extra_info("sockname")
                conn = await ctx.actor(
                    tcp_connection(reader, writer, handler, framer),
                    name=f"conn-in-{conn_count}"
                )
                conn_count += 1
                await handler.send(IoConnected(conn, remote, local))
