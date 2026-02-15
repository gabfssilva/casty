from __future__ import annotations

from typing import Any

import pytest

from casty.core.address import ActorAddress
from casty.cluster.state import Member, MemberStatus, NodeAddress
from casty.cluster.events import (
    MemberLeft,
    MemberUp,
    ReachableMember,
    UnreachableMember,
)
from casty.core.events import (
    ActorRestarted,
    DeadLetter,
    UnhandledMessage,
)
from casty.remote.ref import RemoteActorRef
from casty.core.transport import LocalTransport


def _dummy_ref() -> RemoteActorRef[Any]:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/dummy")
    return RemoteActorRef(address=addr, _transport=transport)


async def test_dead_letter_event() -> None:
    ref = _dummy_ref()
    dl = DeadLetter(message="hello", intended_ref=ref)
    assert dl.message == "hello"
    assert dl.intended_ref is ref


async def test_actor_restarted_event() -> None:
    ref = _dummy_ref()
    exc = RuntimeError("boom")
    event = ActorRestarted(ref=ref, exception=exc)
    assert event.ref is ref
    assert event.exception is exc


async def test_unhandled_message_event() -> None:
    ref = _dummy_ref()
    event = UnhandledMessage(message="test", ref=ref)
    assert event.message == "test"
    assert event.ref is ref


def _dummy_member() -> Member:
    return Member(
        id="node-1",
        address=NodeAddress(host="127.0.0.1", port=2551),
        status=MemberStatus.up,
        roles=frozenset({"backend"}),
    )


async def test_member_up_event() -> None:
    member = _dummy_member()
    event = MemberUp(member=member)
    assert event.member is member
    assert event.member.status == MemberStatus.up


async def test_member_left_event() -> None:
    member = Member(
        id="node-1",
        address=NodeAddress(host="127.0.0.1", port=2551),
        status=MemberStatus.leaving,
        roles=frozenset(),
    )
    event = MemberLeft(member=member)
    assert event.member is member
    assert event.member.status == MemberStatus.leaving


async def test_unreachable_member_event() -> None:
    member = _dummy_member()
    event = UnreachableMember(member=member)
    assert event.member is member


async def test_reachable_member_event() -> None:
    member = _dummy_member()
    event = ReachableMember(member=member)
    assert event.member is member


async def test_cluster_events_are_frozen() -> None:
    member = _dummy_member()
    for event in (
        MemberUp(member=member),
        MemberLeft(member=member),
        UnreachableMember(member=member),
        ReachableMember(member=member),
    ):
        with pytest.raises(AttributeError):
            event.member = member  # type: ignore[misc]
