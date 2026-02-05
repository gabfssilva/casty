from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from casty.events import (
    ActorRestarted,
    ActorStarted,
    ActorStopped,
    DeadLetter,
    EventStream,
    UnhandledMessage,
)
from casty.ref import ActorRef


async def test_subscribe_and_publish() -> None:
    stream = EventStream()
    received: list[ActorStarted] = []

    async def handler(event: ActorStarted) -> None:
        received.append(event)

    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
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

    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
    await stream.publish(ActorStarted(ref=ref))

    assert len(started) == 1
    assert len(stopped) == 0


async def test_unsubscribe() -> None:
    stream = EventStream()
    received: list[ActorStarted] = []

    async def handler(event: ActorStarted) -> None:
        received.append(event)

    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
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

    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
    await stream.publish(ActorStarted(ref=ref))

    assert len(a) == 1
    assert len(b) == 1


async def test_dead_letter_event() -> None:
    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
    dl = DeadLetter(message="hello", intended_ref=ref)
    assert dl.message == "hello"
    assert dl.intended_ref is ref


async def test_actor_restarted_event() -> None:
    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
    exc = RuntimeError("boom")
    event = ActorRestarted(ref=ref, exception=exc)
    assert event.ref is ref
    assert event.exception is exc


async def test_unhandled_message_event() -> None:
    ref: ActorRef[Any] = ActorRef(_send=lambda m: None)
    event = UnhandledMessage(message="test", ref=ref)
    assert event.message == "test"
    assert event.ref is ref
