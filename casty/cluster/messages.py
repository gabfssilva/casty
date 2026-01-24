from __future__ import annotations

from dataclasses import dataclass, field

from casty.message import message


@message
class MemberSnapshot:
    node_id: str
    address: str
    state: str  # "alive" | "down"
    incarnation: int


@message
class Ping:
    sender: str
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class Ack:
    sender: str
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class PingReq:
    sender: str
    target: str
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class PingReqAck:
    sender: str
    target: str
    success: bool
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class Join:
    node_id: str
    address: str


@dataclass
class SetLocalAddress:
    address: str


@message
class GetAliveMembers:
    pass


@message
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


@dataclass
class MarkAlive:
    node_id: str
    address: str


@message
class GetLeaderId:
    actor_id: str
    replicas: int = 3


@message
class IsLeader:
    actor_id: str
    replicas: int = 3


@message
class GetReplicaIds:
    actor_id: str
    replicas: int = 3


@message
class SwimTick:
    pass


@message
class ProbeTimeout:
    target: str


@message
class PingReqTimeout:
    target: str
