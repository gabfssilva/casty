from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from casty.address import ActorAddress


@runtime_checkable
class MessageTransport(Protocol):
    def deliver(self, address: ActorAddress, msg: Any) -> None: ...


class LocalTransport:
    def __init__(self) -> None:
        self._handlers: dict[str, Callable[[Any], None]] = {}

    def register(self, path: str, handler: Callable[[Any], None]) -> None:
        self._handlers[path] = handler

    def unregister(self, path: str) -> None:
        self._handlers.pop(path, None)

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        handler = self._handlers.get(address.path)
        if handler is not None:
            handler(msg)
