"""Message transport abstraction for local actor delivery.

Defines the ``MessageTransport`` protocol and provides ``LocalTransport``,
an in-process implementation that routes messages by actor path.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from casty.address import ActorAddress

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
            logger.warning("No handler for path %s, dropping %s", address.path, type(msg).__name__)
            return
        buf = self._pending.get(address.path)
        if buf is not None and len(buf) >= self._max_pending_per_path:
            logger.warning("Pending buffer full for path %s, dropping %s", address.path, type(msg).__name__)
            return
        self._pending.setdefault(address.path, []).append(msg)
