"""Actor reference re-exports.

``ActorRef`` (the protocol) comes from ``core.ref``.
``RemoteActorRef`` and ``BroadcastRef`` come from ``remote.ref``.
"""

from __future__ import annotations

from casty.core.ref import ActorRef as ActorRef
from casty.remote.ref import BroadcastRef as BroadcastRef, RemoteActorRef as RemoteActorRef

__all__ = [
    "ActorRef",
    "BroadcastRef",
    "RemoteActorRef",
]
