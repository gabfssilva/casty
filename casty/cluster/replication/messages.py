from __future__ import annotations

from casty.message import message


@message
class Replicate:
    actor_id: str
    version: int
    snapshot: bytes


@message
class ReplicateAck:
    actor_id: str
    version: int
    node_id: str


@message
class SyncRequest:
    actor_id: str


@message
class SyncResponse:
    actor_id: str
    version: int
    snapshot: bytes
