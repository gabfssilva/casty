from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from functools import total_ordering


class MemberStatus(Enum):
    joining = auto()
    up = auto()
    leaving = auto()
    down = auto()
    removed = auto()


@total_ordering
@dataclass(frozen=True)
class NodeAddress:
    host: str
    port: int

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, NodeAddress):
            return NotImplemented
        return (self.host, self.port) < (other.host, other.port)


@dataclass(frozen=True)
class Member:
    address: NodeAddress
    status: MemberStatus
    roles: frozenset[str]


@dataclass(frozen=True)
class VectorClock:
    _versions: dict[NodeAddress, int] = field(
        default_factory=lambda: dict[NodeAddress, int]()
    )

    def version_of(self, node: NodeAddress) -> int:
        return self._versions.get(node, 0)

    def increment(self, node: NodeAddress) -> VectorClock:
        new_versions = dict(self._versions)
        new_versions[node] = new_versions.get(node, 0) + 1
        return VectorClock(_versions=new_versions)

    def merge(self, other: VectorClock) -> VectorClock:
        all_nodes = set(self._versions) | set(other._versions)
        merged = {
            node: max(self.version_of(node), other.version_of(node))
            for node in all_nodes
        }
        return VectorClock(_versions=merged)

    def is_before(self, other: VectorClock) -> bool:
        all_nodes = set(self._versions) | set(other._versions)
        at_least_one_less = False
        for node in all_nodes:
            mine = self.version_of(node)
            theirs = other.version_of(node)
            if mine > theirs:
                return False
            if mine < theirs:
                at_least_one_less = True
        return at_least_one_less

    def is_concurrent_with(self, other: VectorClock) -> bool:
        return (
            not self.is_before(other)
            and not other.is_before(self)
            and self._versions != other._versions
        )


@dataclass(frozen=True)
class ClusterState:
    members: frozenset[Member] = field(
        default_factory=lambda: frozenset[Member]()
    )
    unreachable: frozenset[NodeAddress] = field(
        default_factory=lambda: frozenset[NodeAddress]()
    )
    version: VectorClock = field(default_factory=VectorClock)

    def add_member(self, member: Member) -> ClusterState:
        return ClusterState(
            members=self.members | {member},
            unreachable=self.unreachable,
            version=self.version,
        )

    def update_status(
        self, address: NodeAddress, status: MemberStatus
    ) -> ClusterState:
        new_members = frozenset(
            Member(address=m.address, status=status, roles=m.roles)
            if m.address == address
            else m
            for m in self.members
        )
        return ClusterState(
            members=new_members,
            unreachable=self.unreachable,
            version=self.version,
        )

    def mark_unreachable(self, address: NodeAddress) -> ClusterState:
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable | {address},
            version=self.version,
        )

    def mark_reachable(self, address: NodeAddress) -> ClusterState:
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable - {address},
            version=self.version,
        )

    @property
    def leader(self) -> NodeAddress | None:
        up_members = sorted(
            (m.address for m in self.members if m.status == MemberStatus.up)
        )
        return up_members[0] if up_members else None
