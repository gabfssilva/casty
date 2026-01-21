from __future__ import annotations

from typing import Any, Protocol

from casty.serializable import serialize, deserialize


class Serializer(Protocol):
    def encode(self, obj: Any) -> bytes: ...
    def decode(self, data: bytes) -> Any: ...


class MsgPackSerializer:
    def encode(self, obj: Any) -> bytes:
        return serialize(obj)

    def decode(self, data: bytes) -> Any:
        return deserialize(data)
