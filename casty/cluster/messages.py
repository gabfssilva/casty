from __future__ import annotations

from dataclasses import dataclass, field

from casty.serializable import serializable


@serializable
@dataclass
class MembershipUpdate:
    node_id: str
    status: str  # "alive" | "down"
    incarnation: int


@serializable
@dataclass
class Ping:
    updates: list[MembershipUpdate] = field(default_factory=list)


@serializable
@dataclass
class Ack:
    updates: list[MembershipUpdate] = field(default_factory=list)


@serializable
@dataclass
class PingReq:
    target: str
    updates: list[MembershipUpdate] = field(default_factory=list)


@serializable
@dataclass
class PingReqAck:
    target: str
    success: bool
    updates: list[MembershipUpdate] = field(default_factory=list)


@serializable
@dataclass
class Join:
    node_id: str
    address: str


@serializable
@dataclass
class GetAliveMembers:
    pass


@serializable
@dataclass
class GetAllMembers:
    pass


@serializable
@dataclass
class ApplyUpdate:
    update: MembershipUpdate


# SWIM timing messages
@serializable
@dataclass
class SwimTick:
    pass


@serializable
@dataclass
class ProbeTimeout:
    target: str


@serializable
@dataclass
class PingReqTimeout:
    target: str


# Gossip messages
@serializable
@dataclass
class StateUpdate:
    actor_id: str
    sequence: int
    state: bytes


@serializable
@dataclass
class StatePull:
    actor_id: str
    since_sequence: int


@serializable
@dataclass
class GossipTick:
    pass
