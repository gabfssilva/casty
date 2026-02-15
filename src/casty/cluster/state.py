"""Cluster state primitives for CRDT-based membership.

Provides the core data structures for cluster membership: ``NodeAddress``,
``Member``, ``MemberStatus``, ``VectorClock``, and ``ClusterState``.  State is
fully immutable (frozen dataclasses) and merges follow CRDT semantics so that
nodes converge without coordination.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from functools import total_ordering

type NodeId = str


class MemberStatus(Enum):
    """Lifecycle status of a cluster member.

    Members progress through statuses in order::

        joining -> up -> leaving -> down -> removed

    Only the cluster leader promotes ``joining`` to ``up`` once gossip has
    converged.

    Examples
    --------
    >>> MemberStatus.joining
    <MemberStatus.joining: ...>
    >>> MemberStatus.up.name
    'up'
    """

    joining = auto()
    up = auto()
    leaving = auto()
    down = auto()
    removed = auto()

    @property
    def merge_priority(self) -> tuple[bool, int]:
        """Priority key for CRDT merge on concurrent vector clocks.

        Returns a ``(alive, value)`` tuple where alive statuses always win
        over terminal ones.  This ensures a rejoining node (``joining``)
        beats stale ``down``/``removed`` gossip.
        """
        alive = self in (MemberStatus.joining, MemberStatus.up, MemberStatus.leaving)
        return (alive, self.value)


@total_ordering
@dataclass(frozen=True)
class NodeAddress:
    """Network address identifying a cluster node.

    Ordered lexicographically by ``(host, port)`` so that deterministic leader
    election can sort members by address.

    Parameters
    ----------
    host : str
        Hostname or IP address of the node.
    port : int
        TCP port the node listens on.

    Examples
    --------
    >>> addr = NodeAddress(host="127.0.0.1", port=2551)
    >>> addr.host
    '127.0.0.1'
    >>> NodeAddress("a", 1) < NodeAddress("b", 1)
    True
    """

    host: str
    port: int

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, NodeAddress):
            return NotImplemented
        return (self.host, self.port) < (other.host, other.port)


@dataclass(frozen=True)
class Member:
    """A single node's membership record in the cluster.

    Parameters
    ----------
    address : NodeAddress
        Network address of the member.
    status : MemberStatus
        Current lifecycle status.
    roles : frozenset[str]
        Roles advertised by this member (e.g. ``{"frontend", "backend"}``).
    id : NodeId
        Human-readable identifier for this node (e.g. ``"worker-1"``).

    Examples
    --------
    >>> m = Member(NodeAddress("10.0.0.1", 2551), MemberStatus.up, frozenset(), id="node-1")
    >>> m.status
    <MemberStatus.up: ...>
    """

    address: NodeAddress
    status: MemberStatus
    roles: frozenset[str]
    id: NodeId


@dataclass(frozen=True)
class VectorClock:
    """Logical clock for tracking causal ordering of cluster state versions.

    Each node increments its own entry on every state mutation.  Two clocks can
    be compared for happens-before or concurrency, and merged to produce a
    least upper bound.

    Examples
    --------
    >>> node_a = NodeAddress("a", 1)
    >>> node_b = NodeAddress("b", 1)
    >>> vc = VectorClock().increment(node_a).increment(node_a)
    >>> vc.version_of(node_a)
    2
    >>> merged = vc.merge(VectorClock().increment(node_b))
    >>> merged.version_of(node_a), merged.version_of(node_b)
    (2, 1)
    """

    _versions: dict[NodeAddress, int] = field(
        default_factory=lambda: dict[NodeAddress, int]()
    )

    def version_of(self, node: NodeAddress) -> int:
        """Return the version counter for *node*, defaulting to 0.

        Parameters
        ----------
        node : NodeAddress
            The node whose version to look up.

        Returns
        -------
        int
            The current counter for *node*.
        """
        return self._versions.get(node, 0)

    def increment(self, node: NodeAddress) -> VectorClock:
        """Return a new clock with *node*'s counter incremented by one.

        Parameters
        ----------
        node : NodeAddress
            The node whose counter to increment.

        Returns
        -------
        VectorClock
            A new clock with the updated counter.
        """
        new_versions = dict(self._versions)
        new_versions[node] = new_versions.get(node, 0) + 1
        return VectorClock(_versions=new_versions)

    def merge(self, other: VectorClock) -> VectorClock:
        """Merge two clocks by taking the element-wise maximum.

        This is the least upper bound (join) operation, guaranteeing CRDT
        convergence.

        Parameters
        ----------
        other : VectorClock
            The clock to merge with.

        Returns
        -------
        VectorClock
            A new clock where each entry is ``max(self[node], other[node])``.
        """
        all_nodes = set(self._versions) | set(other._versions)
        merged = {
            node: max(self.version_of(node), other.version_of(node))
            for node in all_nodes
        }
        return VectorClock(_versions=merged)

    def is_before(self, other: VectorClock) -> bool:
        """Return whether this clock strictly happens-before *other*.

        Parameters
        ----------
        other : VectorClock
            The clock to compare against.

        Returns
        -------
        bool
            ``True`` if every entry in *self* is ``<=`` the corresponding
            entry in *other* and at least one is strictly less.
        """
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
        """Return whether this clock is concurrent with *other*.

        Two clocks are concurrent when neither happens-before the other and
        they are not equal.

        Parameters
        ----------
        other : VectorClock
            The clock to compare against.

        Returns
        -------
        bool
            ``True`` if the clocks are causally unrelated.
        """
        return (
            not self.is_before(other)
            and not other.is_before(self)
            and self._versions != other._versions
        )


@dataclass(frozen=True)
class ServiceEntry:
    """A registered service in the cluster registry.

    Parameters
    ----------
    key : str
        Service name (e.g. ``"payment-service"``).
    node : NodeAddress
        Node where the actor lives.
    path : str
        Actor path on that node.
    """

    key: str
    node: NodeAddress
    path: str


@dataclass(frozen=True)
class ShardAllocation:
    """Tracks which node owns a shard and where its replicas live.

    Parameters
    ----------
    primary : NodeAddress
        Node that owns the shard and handles writes.
    replicas : tuple[NodeAddress, ...]
        Nodes holding passive replica copies.

    Examples
    --------
    >>> alloc = ShardAllocation(primary=NodeAddress("10.0.0.1", 25520))
    >>> alloc.replicas
    ()
    """
    primary: NodeAddress
    replicas: tuple[NodeAddress, ...] = ()


@dataclass(frozen=True)
class ClusterState:
    """Immutable, CRDT-mergeable snapshot of cluster membership.

    Every node maintains its own ``ClusterState`` and periodically gossips it
    to peers.  Merging two states produces a new state that is the union of
    members with vector-clock-ordered conflict resolution, guaranteeing
    convergence without coordination.

    Parameters
    ----------
    members : frozenset[Member]
        Current cluster members.
    unreachable : frozenset[NodeAddress]
        Nodes detected as unreachable by the failure detector.
    version : VectorClock
        Causal version of this state.
    shard_allocations : dict[str, dict[int, ShardAllocation]]
        Per-shard-type allocation map (populated by the coordinator).
    allocation_epoch : int
        Monotonically increasing epoch for shard allocation changes.
    seen : frozenset[NodeAddress]
        Nodes that have acknowledged this version (for convergence check).

    Examples
    --------
    >>> node = NodeAddress("10.0.0.1", 2551)
    >>> m = Member(node, MemberStatus.up, frozenset(), id="node-1")
    >>> s1 = ClusterState().add_member(m)
    >>> s2 = ClusterState().add_member(m)
    >>> merged = s1.merge_members(s2)
    >>> len(merged)
    1
    """

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
    registry: frozenset[ServiceEntry] = field(
        default_factory=lambda: frozenset[ServiceEntry]()
    )

    @property
    def is_converged(self) -> bool:
        """Whether all active members have seen the current state version.

        Returns
        -------
        bool
            ``True`` when every non-down, non-removed member is in ``seen``.
        """
        active = frozenset(
            m.address for m in self.members
            if m.status not in (MemberStatus.down, MemberStatus.removed)
        )
        return active <= self.seen

    def add_member(self, member: Member) -> ClusterState:
        """Return a new state with *member* added or replaced.

        If a member with the same address already exists it is replaced.

        Parameters
        ----------
        member : Member
            The member to add.

        Returns
        -------
        ClusterState
            New state containing the member.
        """
        filtered = frozenset(m for m in self.members if m.address != member.address)
        return ClusterState(
            members=filtered | {member},
            unreachable=self.unreachable,
            version=self.version,
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
            registry=self.registry,
        )

    def merge_members(self, other: ClusterState) -> frozenset[Member]:
        """Merge member sets from two states, keeping self's version on conflict.

        Parameters
        ----------
        other : ClusterState
            The remote state to merge with.

        Returns
        -------
        frozenset[Member]
            Union of members; when both states contain the same address,
            self's entry wins (last-writer in dict update order).
        """
        by_addr = {m.address: m for m in other.members}
        by_addr.update({m.address: m for m in self.members})
        return frozenset(by_addr.values())

    def update_status(
        self, address: NodeAddress, status: MemberStatus
    ) -> ClusterState:
        """Return a new state with the given node's status changed.

        Parameters
        ----------
        address : NodeAddress
            The node to update.
        status : MemberStatus
            The new status.

        Returns
        -------
        ClusterState
            New state with the updated member.
        """
        new_members = frozenset(
            Member(address=m.address, status=status, roles=m.roles, id=m.id)
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
            registry=self.registry,
        )

    def mark_unreachable(self, address: NodeAddress) -> ClusterState:
        """Return a new state with *address* added to the unreachable set.

        Parameters
        ----------
        address : NodeAddress
            The node to mark unreachable.

        Returns
        -------
        ClusterState
            New state with updated unreachable set.
        """
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable | {address},
            version=self.version,
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
            registry=self.registry,
        )

    def mark_reachable(self, address: NodeAddress) -> ClusterState:
        """Return a new state with *address* removed from the unreachable set.

        Parameters
        ----------
        address : NodeAddress
            The node to mark reachable again.

        Returns
        -------
        ClusterState
            New state with updated unreachable set.
        """
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable - {address},
            version=self.version,
            shard_allocations=self.shard_allocations,
            allocation_epoch=self.allocation_epoch,
            seen=self.seen,
            registry=self.registry,
        )

    def with_allocations(
        self,
        shard_type: str,
        allocations: dict[int, ShardAllocation],
        epoch: int,
    ) -> ClusterState:
        """Return a new state with updated shard allocations for *shard_type*.

        Parameters
        ----------
        shard_type : str
            The shard type name (e.g. ``"counter"``).
        allocations : dict[int, ShardAllocation]
            Shard-id to allocation mapping.
        epoch : int
            Allocation epoch for consistency tracking.

        Returns
        -------
        ClusterState
            New state with the updated allocations.
        """
        new_shard_allocs = {**self.shard_allocations, shard_type: allocations}
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable,
            version=self.version,
            shard_allocations=new_shard_allocs,
            allocation_epoch=epoch,
            seen=self.seen,
            registry=self.registry,
        )

    def __str__(self) -> str:
        nodes = ", ".join(
            f"{m.id}@{m.address.host}:{m.address.port}({m.status.name})"
            for m in sorted(self.members, key=lambda m: m.address)
        )
        leader = self.leader
        leader_str = f"{leader.host}:{leader.port}" if leader else "none"
        return f"[{nodes}] leader={leader_str}"

    @property
    def leader(self) -> NodeAddress | None:
        """The current cluster leader, determined by lowest sorted address.

        Returns
        -------
        NodeAddress or None
            Address of the leader, or ``None`` if no members are ``up``.
        """
        up_members = sorted(
            (m.address for m in self.members if m.status == MemberStatus.up)
        )
        return up_members[0] if up_members else None

    def diff(self, previous: ClusterState | None) -> ClusterChanges:
        """Compute the changes between *previous* and this state.

        Parameters
        ----------
        previous : ClusterState or None
            The previous state to diff against.  ``None`` means empty.

        Returns
        -------
        ClusterChanges
            A summary of joined/removed members and leader election.
        """
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
    """Summary of differences between two ``ClusterState`` snapshots.

    Truthy when any membership or leader change occurred.

    Examples
    --------
    >>> changes = ClusterChanges()
    >>> bool(changes)
    False
    """

    joined: tuple[Member, ...] = ()
    removed: tuple[Member, ...] = ()
    leader: NodeAddress | None = None
    just_elected: bool = False

    def __bool__(self) -> bool:
        return bool(self.joined or self.removed or self.just_elected)
