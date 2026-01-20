from __future__ import annotations

from dataclasses import dataclass

from casty.serializable import serializable


@serializable
@dataclass
class Replicate:
    actor_id: str
    version: int
    snapshot: bytes


@serializable
@dataclass
class ReplicateAck:
    actor_id: str
    version: int
    node_id: str


@serializable
@dataclass
class SyncRequest:
    actor_id: str


@serializable
@dataclass
class SyncResponse:
    actor_id: str
    version: int
    snapshot: bytes
