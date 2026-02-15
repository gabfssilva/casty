"""Cluster membership event types.

Events emitted when cluster members change status: joining, leaving,
becoming unreachable, or recovering.
"""

from __future__ import annotations

from dataclasses import dataclass

from casty.cluster.state import Member


@dataclass(frozen=True)
class MemberUp:
    """Emitted when a cluster member transitions to the ``up`` status.

    Parameters
    ----------
    member : Member
        The cluster member that came up.

    Examples
    --------
    >>> from casty import MemberUp
    >>> event = MemberUp(member=member)
    """

    member: Member


@dataclass(frozen=True)
class MemberLeft:
    """Emitted when a cluster member leaves the cluster.

    Parameters
    ----------
    member : Member
        The cluster member that left.

    Examples
    --------
    >>> from casty import MemberLeft
    >>> event = MemberLeft(member=member)
    """

    member: Member


@dataclass(frozen=True)
class UnreachableMember:
    """Emitted when the failure detector marks a member as unreachable.

    Parameters
    ----------
    member : Member
        The cluster member that became unreachable.

    Examples
    --------
    >>> from casty import UnreachableMember
    >>> event = UnreachableMember(member=member)
    """

    member: Member


@dataclass(frozen=True)
class ReachableMember:
    """Emitted when a previously unreachable member becomes reachable again.

    Parameters
    ----------
    member : Member
        The cluster member that became reachable.

    Examples
    --------
    >>> from casty import ReachableMember
    >>> event = ReachableMember(member=member)
    """

    member: Member
