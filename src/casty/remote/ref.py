"""Remote actor references — address-carrying refs for cross-node messaging.

``RemoteActorRef`` delivers via ``MessageTransport`` and carries an
``ActorAddress`` for serialization/routing. ``BroadcastRef`` is a
thin subclass used by the broadcast layer.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from casty.core.address import ActorAddress
from casty.core.transport import MessageTransport

ref_restore_hook: Callable[[str], RemoteActorRef[Any]] | None = None


def restore_actor_ref(uri: str) -> RemoteActorRef[Any]:
    if ref_restore_hook is None:
        msg = "Cannot unpickle ActorRef: no restore hook installed"
        raise RuntimeError(msg)
    return ref_restore_hook(uri)


@dataclass(frozen=True)
class RemoteActorRef[M]:
    """Concrete ref for remote actors, carrying address + transport."""

    address: ActorAddress
    _transport: MessageTransport

    @property
    def id(self) -> str:
        return self.address.path

    def tell(self, msg: M) -> None:
        self._transport.deliver(self.address, msg)

    def __reduce__(self) -> tuple[Callable[[str], RemoteActorRef[Any]], tuple[str]]:
        return (restore_actor_ref, (self.address.to_uri(),))


@dataclass(frozen=True)
class BroadcastRef[M](RemoteActorRef[M]):
    pass
