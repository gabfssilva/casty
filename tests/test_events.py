from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from casty.address import ActorAddress
from casty.cluster_state import Member, MemberStatus, NodeAddress
from casty.events import (
    ActorRestarted,
    ActorStarted,
    ActorStopped,
    DeadLetter,
    EventStream,
    MemberLeft,
    MemberUp,
    ReachableMember,
    UnhandledMessage,
    UnreachableMember,
)
from casty.ref import ActorRef
from casty.transport import LocalTransport


def _dummy_ref() -> ActorRef[Any]:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/dummy")
    return ActorRef(address=addr, _transport=transport)


async def test_subscribe_and_publish() -> None:
    stream = EventStream()
    received: list[ActorStarted] = []

    async def handler(event: ActorStarted) -> None:
        received.append(event)

    ref = _dummy_ref()
    stream.subscribe(ActorStarted, handler)
    await stream.publish(ActorStarted(ref=ref))

    assert len(received) == 1
    assert received[0].ref is ref


async def test_publish_only_notifies_matching_subscribers() -> None:
    stream = EventStream()
    started: list[ActorStarted] = []
    stopped: list[ActorStopped] = []

    stream.subscribe(ActorStarted, lambda e: started.append(e))  # type: ignore[arg-type,return-value]
    stream.subscribe(ActorStopped, lambda e: stopped.append(e))  # type: ignore[arg-type,return-value]

    ref = _dummy_ref()
    await stream.publish(ActorStarted(ref=ref))

    assert len(started) == 1
    assert len(stopped) == 0


async def test_unsubscribe() -> None:
    stream = EventStream()
    received: list[ActorStarted] = []

    async def handler(event: ActorStarted) -> None:
        received.append(event)

    ref = _dummy_ref()
    stream.subscribe(ActorStarted, handler)
    stream.unsubscribe(ActorStarted, handler)
    await stream.publish(ActorStarted(ref=ref))

    assert received == []


async def test_multiple_subscribers_same_event() -> None:
    stream = EventStream()
    a: list[ActorStarted] = []
    b: list[ActorStarted] = []

    stream.subscribe(ActorStarted, lambda e: a.append(e))  # type: ignore[arg-type,return-value]
    stream.subscribe(ActorStarted, lambda e: b.append(e))  # type: ignore[arg-type,return-value]

    ref = _dummy_ref()
    await stream.publish(ActorStarted(ref=ref))

    assert len(a) == 1
    assert len(b) == 1


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


async def test_publish_cluster_events_on_event_stream() -> None:
    stream = EventStream()
    received_up: list[MemberUp] = []
    received_left: list[MemberLeft] = []
    received_unreachable: list[UnreachableMember] = []
    received_reachable: list[ReachableMember] = []

    stream.subscribe(MemberUp, lambda e: received_up.append(e))  # type: ignore[arg-type,return-value]
    stream.subscribe(MemberLeft, lambda e: received_left.append(e))  # type: ignore[arg-type,return-value]
    stream.subscribe(UnreachableMember, lambda e: received_unreachable.append(e))  # type: ignore[arg-type,return-value]
    stream.subscribe(ReachableMember, lambda e: received_reachable.append(e))  # type: ignore[arg-type,return-value]

    member = _dummy_member()
    await stream.publish(MemberUp(member=member))
    await stream.publish(MemberLeft(member=member))
    await stream.publish(UnreachableMember(member=member))
    await stream.publish(ReachableMember(member=member))

    assert len(received_up) == 1
    assert received_up[0].member is member
    assert len(received_left) == 1
    assert received_left[0].member is member
    assert len(received_unreachable) == 1
    assert received_unreachable[0].member is member
    assert len(received_reachable) == 1
    assert received_reachable[0].member is member
