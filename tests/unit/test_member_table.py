from __future__ import annotations

import uuid

from casty.membership.table import Member, MemberTable, Status


def member(name: str) -> Member:
    return Member(node_id=uuid.uuid5(uuid.NAMESPACE_DNS, name), addr=f"{name}:7001")


def test_new_member_emits_joined() -> None:
    table = MemberTable(member("self"))
    event = table.merge(member("a"), 0, Status.ALIVE, now=1.0)
    assert event is not None
    assert event.kind == "joined"
    assert member("a") in table.alive_members()


def test_self_observations_are_ignored() -> None:
    table = MemberTable(member("self"))
    assert table.merge(member("self"), 5, Status.DEAD, now=1.0) is None
    assert member("self") in table.alive_members()


def test_higher_incarnation_wins() -> None:
    table = MemberTable(member("self"))
    table.merge(member("a"), 1, Status.SUSPECT, now=1.0)
    event = table.merge(member("a"), 2, Status.ALIVE, now=2.0)
    assert event is not None and event.kind == "alive"
    assert member("a") in table.alive_members()
    # stale lower incarnation cannot resurrect suspicion
    assert table.merge(member("a"), 1, Status.SUSPECT, now=3.0) is None


def test_status_precedence_on_incarnation_tie() -> None:
    table = MemberTable(member("self"))
    table.merge(member("a"), 3, Status.ALIVE, now=1.0)
    assert table.merge(member("a"), 3, Status.ALIVE, now=2.0) is None  # idempotent
    event = table.merge(member("a"), 3, Status.SUSPECT, now=3.0)
    assert event is not None and event.kind == "suspect"
    event = table.merge(member("a"), 3, Status.DEAD, now=4.0)
    assert event is not None and event.kind == "dead"
    # alive at the same incarnation does not beat dead
    assert table.merge(member("a"), 3, Status.ALIVE, now=5.0) is None
    assert member("a") not in table.alive_members()


def test_left_beats_alive_but_not_dead() -> None:
    table = MemberTable(member("self"))
    table.merge(member("a"), 1, Status.ALIVE, now=1.0)
    event = table.merge(member("a"), 1, Status.LEFT, now=2.0)
    assert event is not None and event.kind == "left"
    assert table.merge(member("a"), 1, Status.DEAD, now=3.0) is not None


def test_refutation() -> None:
    table = MemberTable(member("self"))
    self_id = member("self").node_id
    assert table.needs_refutation(self_id, 0, Status.SUSPECT)
    new_incarnation = table.refute(0)
    assert new_incarnation == 1
    assert not table.needs_refutation(self_id, 0, Status.SUSPECT)
    assert table.needs_refutation(self_id, 1, Status.DEAD)
    assert not table.needs_refutation(member("a").node_id, 9, Status.DEAD)
    assert not table.needs_refutation(self_id, 9, Status.ALIVE)


def test_rejoin_after_death_emits_joined() -> None:
    table = MemberTable(member("self"))
    table.merge(member("a"), 1, Status.DEAD, now=1.0)
    event = table.merge(member("a"), 2, Status.ALIVE, now=2.0)
    assert event is not None and event.kind == "joined"


def test_suspects_older_than_and_tombstone_sweep() -> None:
    table = MemberTable(member("self"))
    table.merge(member("a"), 0, Status.SUSPECT, now=1.0)
    table.merge(member("b"), 0, Status.SUSPECT, now=5.0)
    stale = table.suspects_older_than(2.0)
    assert [r.member for r in stale] == [member("a")]
    table.merge(member("c"), 0, Status.DEAD, now=1.0)
    table.sweep_tombstones(2.0)
    assert table.get(member("c").node_id) is None
    assert table.get(member("a").node_id) is not None
