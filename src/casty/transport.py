from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from casty.address import ActorAddress

logger = logging.getLogger("casty.transport")


@runtime_checkable
class MessageTransport(Protocol):
    def deliver(self, address: ActorAddress, msg: Any) -> None: ...


class LocalTransport:
    def __init__(self, *, max_pending_per_path: int = 64) -> None:
        self._handlers: dict[str, Callable[[Any], None]] = {}
        self._pending: dict[str, list[Any]] = {}
        self._dead: set[str] = set()
        self._max_pending_per_path = max_pending_per_path

    def register(self, path: str, handler: Callable[[Any], None]) -> None:
        self._handlers[path] = handler
        self._dead.discard(path)
        for msg in self._pending.pop(path, []):
            handler(msg)

    def unregister(self, path: str) -> None:
        self._handlers.pop(path, None)
        self._pending.pop(path, None)
        self._dead.add(path)

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        handler = self._handlers.get(address.path)
        if handler is not None:
            handler(msg)
            return
        if address.path in self._dead:
            logger.warning("No handler for path %s, dropping %s", address.path, type(msg).__name__)
            return
        buf = self._pending.get(address.path)
        if buf is not None and len(buf) >= self._max_pending_per_path:
            logger.warning("Pending buffer full for path %s, dropping %s", address.path, type(msg).__name__)
            return
        self._pending.setdefault(address.path, []).append(msg)
