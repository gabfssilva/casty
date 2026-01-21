from __future__ import annotations

from dataclasses import dataclass, field

from casty.serializable import serializable


@serializable
@dataclass
class MemberSnapshot:
    node_id: str
    address: str
    state: str  # "alive" | "down"
    incarnation: int


@serializable
@dataclass
class Ping:
    sender: str
    members: list[MemberSnapshot] = field(default_factory=list)


@serializable
@dataclass
class Ack:
    sender: str
    members: list[MemberSnapshot] = field(default_factory=list)


@serializable
@dataclass
class PingReq:
    sender: str
    target: str
    members: list[MemberSnapshot] = field(default_factory=list)


@serializable
@dataclass
class PingReqAck:
    sender: str
    target: str
    success: bool
    members: list[MemberSnapshot] = field(default_factory=list)


@serializable
@dataclass
class Join:
    node_id: str
    address: str


@dataclass
class SetLocalAddress:
    address: str


@serializable
@dataclass
class GetAliveMembers:
    pass


@serializable
@dataclass
class GetAllMembers:
    pass


@dataclass
class GetResponsibleNodes:
    actor_id: str
    count: int = 1


@dataclass
class GetAddress:
    node_id: str


@dataclass
class MergeMembership:
    members: list[MemberSnapshot]


@dataclass
class MarkDown:
    node_id: str


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


