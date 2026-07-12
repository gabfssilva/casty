from __future__ import annotations

import random
import uuid

from casty.membership.hyparview import Views
from casty.membership.table import Member


def member(name: str) -> Member:
    return Member(node_id=uuid.uuid5(uuid.NAMESPACE_DNS, name), addr=f"{name}:7001")


def make_views(active: int = 3, passive: int = 5, seed: int = 42) -> Views:
    return Views(
        member("self"), active_size=active, passive_size=passive, rng=random.Random(seed)
    )


def test_add_active_within_capacity() -> None:
    views = make_views()
    assert views.add_active(member("a")) is None
    assert views.add_active(member("b")) is None
    assert views.in_active(member("a").node_id)
    assert not views.active_full()


def test_add_active_evicts_to_passive_when_full() -> None:
    views = make_views(active=2)
    views.add_active(member("a"))
    views.add_active(member("b"))
    evicted = views.add_active(member("c"))
    assert evicted in (member("a"), member("b"))
    assert len(views.active) == 2
    assert evicted in views.passive


def test_self_and_duplicates_never_added() -> None:
    views = make_views()
    assert views.add_active(member("self")) is None
    assert not views.in_active(member("self").node_id)
    views.add_active(member("a"))
    assert views.add_active(member("a")) is None
    assert len(views.active) == 1
    views.add_passive(member("self"))
    views.add_passive(member("a"))  # already active
    assert views.passive == []


def test_active_member_moves_out_of_passive() -> None:
    views = make_views()
    views.add_passive(member("a"))
    views.add_active(member("a"))
    assert member("a") not in views.passive
    assert views.in_active(member("a").node_id)


def test_remove_active_to_passive() -> None:
    views = make_views()
    views.add_active(member("a"))
    removed = views.remove_active(member("a").node_id, to_passive=True)
    assert removed == member("a")
    assert member("a") in views.passive
    assert not views.in_active(member("a").node_id)


def test_passive_capacity_evicts() -> None:
    views = make_views(passive=2)
    views.add_passive(member("a"))
    views.add_passive(member("b"))
    views.add_passive(member("c"))
    assert len(views.passive) == 2


def test_promotion_candidate_from_passive() -> None:
    views = make_views()
    assert views.promotion_candidate() is None
    views.add_passive(member("a"))
    assert views.promotion_candidate() == member("a")


def test_shuffle_sample_includes_self() -> None:
    views = make_views()
    views.add_active(member("a"))
    views.add_passive(member("p"))
    sample = views.shuffle_sample(1, 1)
    assert sample[0] == member("self")
    assert member("a") in sample
    assert member("p") in sample


def test_integrate_shuffle_fills_passive() -> None:
    views = make_views()
    views.add_active(member("a"))
    views.integrate_shuffle([member("self"), member("a"), member("x")])
    assert views.passive == [member("x")]


def test_random_active_excludes() -> None:
    views = make_views()
    views.add_active(member("a"))
    assert views.random_active(exclude=member("a").node_id) is None
    assert views.random_active() == member("a")
