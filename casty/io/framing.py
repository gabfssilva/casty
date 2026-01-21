from __future__ import annotations

from typing import Protocol


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
