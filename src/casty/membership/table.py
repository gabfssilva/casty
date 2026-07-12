from __future__ import annotations

import enum
import typing
import uuid
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Member:
    node_id: uuid.UUID
    addr: str
    role: str = "member"


class Status(enum.IntEnum):
    ALIVE = 1
    SUSPECT = 2
    LEFT = 3
    DEAD = 4


type EventKind = typing.Literal["joined", "suspect", "alive", "dead", "left"]

_EVENT_OF: dict[Status, EventKind] = {
    Status.ALIVE: "alive",
    Status.SUSPECT: "suspect",
    Status.LEFT: "left",
    Status.DEAD: "dead",
}


@dataclass(frozen=True, slots=True)
class ViewEvent:
    kind: EventKind
    member: Member


@dataclass(slots=True)
class Record:
    member: Member
    incarnation: int
    status: Status
    changed_at: float = 0.0  # monotonic clock, stamped by the service


class MemberTable:
    """Full member list with SWIM-style merge semantics. Pure: no I/O, no clock —
    the service stamps `now` in. Higher incarnation wins; on ties, status
    precedence DEAD > LEFT > SUSPECT > ALIVE."""

    def __init__(self, self_member: Member) -> None:
        self.self_member = self_member
        self.incarnation = 0  # own incarnation, bumped on refutation
        self._records: dict[uuid.UUID, Record] = {}

    def merge(
        self, member: Member, incarnation: int, status: Status, now: float
    ) -> ViewEvent | None:
        """Apply one observation. Returns the view event it produced, if any.
        Observations about self are ignored here (refutation is the service's
        call, via `needs_refutation`)."""
        if member.node_id == self.self_member.node_id:
            return None
        current = self._records.get(member.node_id)
        if current is None:
            self._records[member.node_id] = Record(member, incarnation, status, now)
            if status in (Status.ALIVE, Status.SUSPECT):
                return ViewEvent("joined", member)
            return None
        if incarnation < current.incarnation:
            return None
        if incarnation == current.incarnation and status <= current.status:
            return None
        was_gone = current.status in (Status.DEAD, Status.LEFT)
        current.incarnation = incarnation
        current.status = status
        current.changed_at = now
        current.member = member
        if status in (Status.DEAD, Status.LEFT):
            return ViewEvent(_EVENT_OF[status], member)
        if was_gone:
            return ViewEvent("joined", member)
        return ViewEvent(_EVENT_OF[status], member)

    def needs_refutation(self, node_id: uuid.UUID, incarnation: int, status: Status) -> bool:
        """True if an observation claims *we* are suspect/dead with an incarnation
        that our current one does not already override."""
        return (
            node_id == self.self_member.node_id
            and status in (Status.SUSPECT, Status.DEAD)
            and incarnation >= self.incarnation
        )

    def refute(self, observed_incarnation: int) -> int:
        self.incarnation = max(self.incarnation, observed_incarnation) + 1
        return self.incarnation

    def get(self, node_id: uuid.UUID) -> Record | None:
        return self._records.get(node_id)

    def alive_members(self) -> frozenset[Member]:
        members = {
            r.member
            for r in self._records.values()
            if r.status in (Status.ALIVE, Status.SUSPECT)
        }
        members.add(self.self_member)
        return frozenset(members)

    def records(self) -> list[Record]:
        return list(self._records.values())

    def suspects_older_than(self, deadline: float) -> list[Record]:
        return [
            r
            for r in self._records.values()
            if r.status is Status.SUSPECT and r.changed_at <= deadline
        ]

    def sweep_tombstones(self, deadline: float) -> None:
        gone = [
            node_id
            for node_id, r in self._records.items()
            if r.status in (Status.DEAD, Status.LEFT) and r.changed_at <= deadline
        ]
        for node_id in gone:
            del self._records[node_id]
