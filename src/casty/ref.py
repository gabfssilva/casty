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
    address: ActorAddress
    _transport: MessageTransport

    def tell(self, msg: M) -> None:
        self._transport.deliver(self.address, msg)

    def __reduce__(self) -> tuple[Callable[[str], ActorRef[Any]], tuple[str]]:
        return (_restore_actor_ref, (self.address.to_uri(),))


@dataclass(frozen=True)
class BroadcastRef[M](ActorRef[M]):
    """Typed handle for a broadcasted actor.

    Inherits ``tell()`` from ``ActorRef``.  The subclass exists so that
    ``spawn`` and ``ask`` overloads can distinguish broadcast refs at the
    type level — ``ask(BroadcastRef, ...)`` returns ``tuple[R, ...]``.
    """
