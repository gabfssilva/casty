from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from functools import total_ordering
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty.replication import ShardAllocation


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
    shard_allocations: dict[str, dict[int, ShardAllocation]] = field(
        default_factory=lambda: {}  # noqa: C408
    )
    allocation_epoch: int = 0
    seen: frozenset[NodeAddress] = field(
        default_factory=lambda: frozenset[NodeAddress]()
    )

    @property
    def is_converged(self) -> bool:
        active = frozenset(
            m.address for m in self.members
            if m.status not in (MemberStatus.down, MemberStatus.removed)
        )
        return active <= self.seen

    def add_member(self, member: Member) -> ClusterState:
        filtered = frozenset(m for m in self.members if m.address != member.address)
        return ClusterState(
            members=filtered | {member},
            unreachable=self.unreachable,
            version=self.version,
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
        )

    def merge_members(self, other: ClusterState) -> frozenset[Member]:
        by_addr = {m.address: m for m in other.members}
        by_addr.update({m.address: m for m in self.members})
        return frozenset(by_addr.values())

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
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
        )

    def mark_unreachable(self, address: NodeAddress) -> ClusterState:
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable | {address},
            version=self.version,
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
        )

    def mark_reachable(self, address: NodeAddress) -> ClusterState:
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable - {address},
            version=self.version,
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
        )

    def with_allocations(
        self,
        shard_type: str,
        allocations: dict[int, ShardAllocation],
        epoch: int,
    ) -> ClusterState:
        new_shard_allocs = {**self.shard_allocations, shard_type: allocations}
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable,
            version=self.version,
            shard_allocations=new_shard_allocs,
            allocation_epoch=epoch,
            seen=self.seen,
        )

    def __str__(self) -> str:
        nodes = ", ".join(
            f"{m.address.host}:{m.address.port}({m.status.name})"
            for m in sorted(self.members, key=lambda m: m.address)
        )
        leader = self.leader
        leader_str = f"{leader.host}:{leader.port}" if leader else "none"
        return f"[{nodes}] leader={leader_str}"

    @property
    def leader(self) -> NodeAddress | None:
        up_members = sorted(
            (m.address for m in self.members if m.status == MemberStatus.up)
        )
        return up_members[0] if up_members else None

    def diff(self, previous: ClusterState | None) -> ClusterChanges:
        old_by_addr = {m.address: m for m in previous.members} if previous else {}
        new_by_addr = {m.address: m for m in self.members}

        joined = tuple(
            new_by_addr[addr]
            for addr in sorted(new_by_addr.keys() - old_by_addr.keys())
        )
        removed = tuple(
            old_by_addr[addr]
            for addr in sorted(old_by_addr.keys() - new_by_addr.keys())
        )

        old_leader = previous.leader if previous else None
        elected = old_leader != self.leader

        return ClusterChanges(
            joined=joined,
            removed=removed,
            leader=self.leader,
            just_elected=elected,
        )


@dataclass(frozen=True)
class ClusterChanges:
    joined: tuple[Member, ...] = ()
    removed: tuple[Member, ...] = ()
    leader: NodeAddress | None = None
    just_elected: bool = False

    def __bool__(self) -> bool:
        return bool(self.joined or self.removed or self.just_elected)
