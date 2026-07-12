from __future__ import annotations

import uuid

from casty.serde.registry import message

JOIN = 0x20
FORWARD_JOIN = 0x21
NEIGHBOR = 0x22
NEIGHBOR_REPLY = 0x23
DISCONNECT = 0x24
SHUFFLE = 0x25
SHUFFLE_REPLY = 0x26
GOSSIP = 0x27
IHAVE = 0x28
GRAFT = 0x29
PRUNE = 0x2A
SYNC = 0x2B
SYNC_REPLY = 0x2C


@message(name="casty.Member")
class MemberMsg:
    node_id: uuid.UUID
    addr: str
    role: str = "member"


@message(name="casty.NodeJoined")
class NodeJoined:
    member: MemberMsg
    incarnation: int


@message(name="casty.NodeLeft")
class NodeLeft:
    node_id: uuid.UUID
    incarnation: int


@message(name="casty.NodeSuspect")
class NodeSuspect:
    node_id: uuid.UUID
    incarnation: int


@message(name="casty.NodeAlive")
class NodeAlive:
    member: MemberMsg
    incarnation: int


@message(name="casty.NodeDead")
class NodeDead:
    node_id: uuid.UUID
    incarnation: int


type ClusterEvent = NodeJoined | NodeLeft | NodeSuspect | NodeAlive | NodeDead


@message(name="casty.Join")
class Join:
    member: MemberMsg


@message(name="casty.ForwardJoin")
class ForwardJoin:
    member: MemberMsg
    ttl: int


@message(name="casty.Neighbor")
class Neighbor:
    member: MemberMsg
    high_priority: bool


@message(name="casty.NeighborReply")
class NeighborReply:
    member: MemberMsg
    accepted: bool


@message(name="casty.Disconnect")
class Disconnect:
    node_id: uuid.UUID


@message(name="casty.Shuffle")
class Shuffle:
    origin: MemberMsg
    sample: list[MemberMsg]
    ttl: int


@message(name="casty.ShuffleReply")
class ShuffleReply:
    sample: list[MemberMsg]


@message(name="casty.Gossip")
class Gossip:
    msg_id: bytes  # origin node_id (16) + seq u64
    round: int
    event: NodeJoined | NodeLeft | NodeSuspect | NodeAlive | NodeDead


@message(name="casty.IHave")
class IHave:
    msg_ids: list[bytes]


@message(name="casty.Graft")
class Graft:
    msg_id: bytes


@message(name="casty.Prune")
class Prune:
    node_id: uuid.UUID


@message(name="casty.SyncRecord")
class SyncRecord:
    member: MemberMsg
    incarnation: int
    status: int


@message(name="casty.Sync")
class Sync:
    records: list[SyncRecord]


@message(name="casty.SyncReply")
class SyncReply:
    records: list[SyncRecord]
