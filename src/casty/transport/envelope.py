from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

from casty.errors import ProtocolError

# u32 length (bytes after this field) | u8 msg_type | u64 correlation_id | body
_LENGTH = struct.Struct("!I")
_HEAD = struct.Struct("!BQ")
_OVERHEAD = _HEAD.size  # 9


class MsgType(IntEnum):
    HELLO = 0x01
    HELLO_ACK = 0x02
    HELLO_REJECT = 0x03
    RESPONSE = 0x10
    ERROR = 0x11
    BULK_OPEN = 0x12
    STREAM_ITEM = 0x13  # one streamed element (spec 07), body = encode_raw payload
    STREAM_ERROR = 0x14  # terminal stream failure, body = msgpack {code, message}
    # 0x20-0x3F membership (spec 02); 0x40-0x5F actors (spec 04); 0x60-0x7F replication (spec 05)


@dataclass(frozen=True, slots=True)
class Envelope:
    msg_type: int
    correlation_id: int
    body: bytes


def encode(msg_type: int, correlation_id: int, body: bytes) -> bytes:
    return _LENGTH.pack(_OVERHEAD + len(body)) + _HEAD.pack(msg_type, correlation_id) + body


def wire_size(body_len: int) -> int:
    """Total bytes one envelope occupies on the stream."""
    return _LENGTH.size + _OVERHEAD + body_len


class Decoder:
    """Incremental envelope decoder for one channel (stream). Envelopes may span
    DATA frames and one frame may carry several envelopes."""

    def __init__(self, *, max_message_bytes: int) -> None:
        self._buffer = bytearray()
        self._max = max_message_bytes

    def drain(self) -> bytes:
        """Remaining undecoded bytes; empties the buffer. Used when a channel
        switches from envelope framing to raw bytes (bulk streams)."""
        remaining = bytes(self._buffer)
        self._buffer.clear()
        return remaining

    def feed(self, data: bytes) -> list[Envelope]:
        self._buffer.extend(data)
        envelopes: list[Envelope] = []
        while True:
            if len(self._buffer) < _LENGTH.size:
                return envelopes
            (length,) = _LENGTH.unpack_from(self._buffer)
            if length < _OVERHEAD:
                raise ProtocolError(f"envelope length {length} below header size")
            if length > _OVERHEAD + self._max:
                raise ProtocolError(
                    f"message of {length - _OVERHEAD} bytes exceeds max_message_bytes {self._max}"
                )
            total = _LENGTH.size + length
            if len(self._buffer) < total:
                return envelopes
            msg_type, correlation_id = _HEAD.unpack_from(self._buffer, _LENGTH.size)
            body = bytes(self._buffer[_LENGTH.size + _OVERHEAD : total])
            del self._buffer[:total]
            envelopes.append(Envelope(msg_type, correlation_id, body))
