"""Message transport abstraction for local actor delivery.

Defines the ``MessageTransport`` protocol and provides ``LocalTransport``,
an in-process implementation that routes messages by actor path.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from casty.core.address import ActorAddress

logger = logging.getLogger("casty.transport")


@runtime_checkable
class MessageTransport(Protocol):
    """Protocol for delivering messages to actors by address.

    Any object with a ``deliver(address, msg)`` method satisfies this protocol.

    Examples
    --------
    Minimal implementation:

    >>> class NullTransport:
    ...     def deliver(self, address: ActorAddress, msg: object) -> None:
    ...         pass  # discard all messages
    """

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        """Deliver a message to the actor at the given address.

        Parameters
        ----------
        address : ActorAddress
            Target actor address.
        msg : Any
            The message to deliver.
        """
        ...


class LocalTransport:
    """In-process message transport that routes by actor path.

    Messages to unregistered paths are buffered (up to ``max_pending_per_path``)
    until a handler registers. Messages to paths that have been unregistered
    (dead actors) are dropped.

    Parameters
    ----------
    max_pending_per_path : int
        Maximum buffered messages per path before dropping. Default is 64.

    Examples
    --------
    >>> transport = LocalTransport()
    >>> received = []
    >>> transport.register("/user/actor", received.append)
    >>> transport.deliver(ActorAddress(system="sys", path="/user/actor"), "hello")
    >>> received
    ['hello']
    """

    def __init__(self, *, max_pending_per_path: int = 64) -> None:
        self._handlers: dict[str, Callable[[Any], None]] = {}
        self._pending: dict[str, list[Any]] = {}
        self._dead: set[str] = set()
        self._max_pending_per_path = max_pending_per_path
        self._waiters: dict[str, asyncio.Event] = {}
        self._path_factories: list[tuple[str, Callable[[str], None]]] = []

    def register_path_factory(
        self,
        prefix: str,
        factory: Callable[[str], None],
    ) -> None:
        """Register a factory that lazily spawns handlers for paths with a given prefix.

        When ``deliver()`` encounters an unregistered path starting with *prefix*,
        it calls ``factory(path)`` which should register a handler for that path.

        Parameters
        ----------
        prefix : str
            Path prefix to match (e.g. ``"/_coord-"``).
        factory : Callable[[str], None]
            Called with the full path; must call ``register()`` for that path.
        """
        self._path_factories.append((prefix, factory))

    def register(self, path: str, handler: Callable[[Any], None]) -> None:
        """Register a message handler for the given path.

        Any messages buffered for this path are flushed to the handler
        immediately.

        Parameters
        ----------
        path : str
            Actor path (e.g. ``/user/counter``).
        handler : Callable[[Any], None]
            Callback invoked with each delivered message.
        """
        self._handlers[path] = handler
        self._dead.discard(path)
        for msg in self._pending.pop(path, []):
            handler(msg)
        waiter = self._waiters.pop(path, None)
        if waiter is not None:
            waiter.set()

    def unregister(self, path: str) -> None:
        """Remove the handler for the given path and mark it dead.

        Future messages to this path are dropped rather than buffered.

        Parameters
        ----------
        path : str
            Actor path to unregister.
        """
        self._handlers.pop(path, None)
        self._pending.pop(path, None)
        self._dead.add(path)

    async def wait_for_path(self, path: str) -> None:
        """Wait until a handler is registered for the given path."""
        if path in self._handlers:
            return
        event = self._waiters.get(path)
        if event is None:
            event = asyncio.Event()
            self._waiters[path] = event
        await event.wait()

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        """Deliver a message to the handler registered for the address path.

        If no handler is registered and the path is not dead, the message
        is buffered. If the buffer is full, the message is dropped.

        Parameters
        ----------
        address : ActorAddress
            Target actor address.
        msg : Any
            The message to deliver.
        """
        handler = self._handlers.get(address.path)
        if handler is not None:
            handler(msg)
            return
        if address.path in self._dead:
            logger.warning(
                "No handler for path %s, dropping %s", address.path, type(msg).__name__
            )
            return
        for prefix, factory in self._path_factories:
            if address.path.startswith(prefix) and address.path not in self._handlers:
                factory(address.path)
                break
        handler = self._handlers.get(address.path)
        if handler is not None:
            handler(msg)
            return
        buf = self._pending.get(address.path)
        if buf is not None and len(buf) >= self._max_pending_per_path:
            logger.warning(
                "Pending buffer full for path %s, dropping %s",
                address.path,
                type(msg).__name__,
            )
            return
        self._pending.setdefault(address.path, []).append(msg)
