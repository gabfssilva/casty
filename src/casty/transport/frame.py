from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum, IntFlag

from casty.errors import ProtocolError

PROTOCOL_VERSION = 1
HEADER = struct.Struct("!BBHII")
HEADER_SIZE = HEADER.size  # 12


class FrameType(IntEnum):
    DATA = 0x0
    WINDOW_UPDATE = 0x1
    PING = 0x2
    GO_AWAY = 0x3


class Flags(IntFlag):
    NONE = 0x0
    SYN = 0x1
    ACK = 0x2
    FIN = 0x4
    RST = 0x8
    COMPRESSED = 0x10


_KNOWN_FLAGS = Flags.SYN | Flags.ACK | Flags.FIN | Flags.RST | Flags.COMPRESSED
_MAX_U32 = 0xFFFFFFFF


@dataclass(frozen=True, slots=True)
class Frame:
    type: FrameType
    flags: Flags
    stream_id: int
    payload: bytes = b""
    # WINDOW_UPDATE carries its credit count in the header length field and has
    # no payload; `credits` mirrors that field on both encode and decode.
    credits: int = 0


def encode(frame: Frame) -> bytes:
    if frame.type is FrameType.WINDOW_UPDATE:
        if frame.payload:
            raise ProtocolError("WINDOW_UPDATE carries no payload")
        if not 0 < frame.credits <= _MAX_U32:
            raise ProtocolError(f"invalid credits {frame.credits}")
        length = frame.credits
    else:
        length = len(frame.payload)
    header = HEADER.pack(
        PROTOCOL_VERSION, frame.type, frame.flags, frame.stream_id, length
    )
    return header + frame.payload if frame.payload else header


class Decoder:
    """Incremental frame decoder. Feed bytes, get frames; raises ProtocolError
    on any wire violation (the connection must then die, per spec)."""

    def __init__(self, *, max_frame_bytes: int) -> None:
        self._buffer = bytearray()
        self._max_frame_bytes = max_frame_bytes

    def feed(self, data: bytes) -> list[Frame]:
        self._buffer.extend(data)
        frames: list[Frame] = []
        while True:
            frame = self._next()
            if frame is None:
                return frames
            frames.append(frame)

    def _next(self) -> Frame | None:
        if len(self._buffer) < HEADER_SIZE:
            return None
        version, raw_type, raw_flags, stream_id, length = HEADER.unpack_from(self._buffer)
        if version != PROTOCOL_VERSION:
            raise ProtocolError(f"unknown protocol version {version}")
        try:
            frame_type = FrameType(raw_type)
        except ValueError:
            raise ProtocolError(f"unknown frame type 0x{raw_type:x}") from None
        if raw_flags & ~int(_KNOWN_FLAGS):
            raise ProtocolError(f"reserved flag bits set: 0x{raw_flags:x}")
        flags = Flags(raw_flags)

        if frame_type is FrameType.WINDOW_UPDATE:
            del self._buffer[:HEADER_SIZE]
            if length == 0:
                raise ProtocolError("WINDOW_UPDATE with zero credits")
            return Frame(frame_type, flags, stream_id, credits=length)

        if frame_type is FrameType.DATA and length > self._max_frame_bytes:
            raise ProtocolError(
                f"DATA frame of {length} bytes exceeds max_frame_bytes {self._max_frame_bytes}"
            )
        if frame_type is FrameType.PING and length != 8:
            raise ProtocolError(f"PING payload must be 8 bytes, got {length}")
        if len(self._buffer) < HEADER_SIZE + length:
            return None
        payload = bytes(self._buffer[HEADER_SIZE : HEADER_SIZE + length])
        del self._buffer[: HEADER_SIZE + length]
        return Frame(frame_type, flags, stream_id, payload)
