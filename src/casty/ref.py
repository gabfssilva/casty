"""Typed actor references for fire-and-forget messaging.

Defines ``ActorRef[M]``, the primary handle for sending messages to actors,
and ``BroadcastRef[M]`` for broadcast-capable refs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from casty.address import ActorAddress
from casty.transport import MessageTransport

ref_restore_hook: Callable[[str], ActorRef[Any]] | None = None


def _restore_actor_ref(uri: str) -> ActorRef[Any]:
    if ref_restore_hook is None:
        msg = "Cannot unpickle ActorRef: no restore hook installed (set casty.ref.ref_restore_hook)"
        raise RuntimeError(msg)
    return ref_restore_hook(uri)


@dataclass(frozen=True)
class ActorRef[M]:
    """Typed handle to an actor, used for fire-and-forget messaging.

    ``ActorRef`` is the only way to communicate with an actor from the
    outside world or from other actors. It wraps an ``ActorAddress`` and
    a ``MessageTransport`` to deliver messages transparently, whether the
    target is local or remote.

    Parameters
    ----------
    address : ActorAddress
        The logical address of the target actor.
    _transport : MessageTransport
        The transport layer used to deliver messages.

    Examples
    --------
    >>> ref: ActorRef[str] = system.spawn(my_behavior, "greeter")
    >>> ref.tell("Hello!")
    """

    address: ActorAddress
    _transport: MessageTransport

    def tell(self, msg: M) -> None:
        """Send a message to this actor (fire-and-forget).

        The message is enqueued in the actor's mailbox and processed
        asynchronously. This method never blocks.

        Parameters
        ----------
        msg : M
            The message to send.

        Examples
        --------
        >>> ref.tell(Deposit(amount=100))
        """
        self._transport.deliver(self.address, msg)

    def __reduce__(self) -> tuple[Callable[[str], ActorRef[Any]], tuple[str]]:
        return (_restore_actor_ref, (self.address.to_uri(),))


@dataclass(frozen=True)
class BroadcastRef[M](ActorRef[M]):
    """Typed handle for a broadcasted actor.

    Inherits ``tell()`` from ``ActorRef``. The subclass exists so that
    ``spawn`` and ``ask`` overloads can distinguish broadcast refs at the
    type level -- ``ask(BroadcastRef, ...)`` returns ``tuple[R, ...]``.

    Examples
    --------
    >>> ref: BroadcastRef[MyMsg] = system.spawn(behavior, "broadcast")
    >>> ref.tell(Ping())
    """
