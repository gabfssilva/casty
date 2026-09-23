import logging
from datetime import timedelta
from uuid import UUID

import pytest

import casty
from casty import (
    ActivationEnded,
    ActivationFailed,
    ActivationStarted,
    ActorSystem,
    ConnectionLost,
    Event,
    HandoffEnded,
    HandoffStarted,
    LoggingObserver,
    MemberChanged,
    MessageDropped,
    NodeId,
    WriteFailed,
)
from tests.app import Nap, sleepy
from tests.cluster import Harness
from tests.support import eventually

NODE = NodeId("127.0.0.1:7001", UUID("0b6f0c9e-5d4a-4c3e-8f7e-2b1a9d3c4e5f"))
NAMED = "127.0.0.1:7001 (0b6f0c9e)"
BROKEN = RuntimeError("the body broke")

LOGGED: list[tuple[Event, int, str]] = [
    (MemberChanged(NODE, "alive", None), logging.INFO, f"member {NAMED} is alive"),
    (MemberChanged(NODE, "suspect", "alive"), logging.WARNING, f"member {NAMED} went from alive to suspect"),
    (MemberChanged(NODE, "alive", "suspect"), logging.INFO, f"member {NAMED} went from suspect to alive"),
    (MemberChanged(NODE, "dead", "suspect"), logging.WARNING, f"member {NAMED} went from suspect to dead"),
    (MemberChanged(NODE, "leaving", "alive"), logging.INFO, f"member {NAMED} went from alive to leaving"),
    (MemberChanged(NODE, "left", "dead"), logging.INFO, f"member {NAMED} went from dead to left"),
    (ActivationStarted("app:account", "a-1"), logging.DEBUG, "activated app:account/a-1"),
    (ActivationEnded("app:account", "a-1"), logging.DEBUG, "deactivated app:account/a-1"),
    (ActivationFailed("app:account", "a-1", BROKEN), logging.WARNING, "app:account/a-1 failed: the body broke"),
    (
        WriteFailed("app:account", "a-1", "activate", "unavailable", "app:account/a-1: too few replicas answered"),
        logging.WARNING,
        "app:account/a-1 was not taken over: app:account/a-1: too few replicas answered",
    ),
    (
        WriteFailed("app:account", "a-1", "write", "fenced", "app:account/a-1 moved to another owner"),
        logging.WARNING,
        "app:account/a-1 was not written: app:account/a-1 moved to another owner",
    ),
    (HandoffStarted("app:account", "in"), logging.INFO, "keys of app:account started moving to this node"),
    (HandoffEnded("app:account", "out"), logging.INFO, "keys of app:account stopped moving off this node"),
    (
        HandoffEnded("app:account", "out", ("a-1", "a-2")),
        logging.WARNING,
        "keys of app:account stopped moving off this node without handing over 2 of them: a-1, a-2",
    ),
    (ConnectionLost(NODE), logging.INFO, f"lost the connection to {NAMED}"),
    (
        MessageDropped("app:account", "a-1", "the mailbox is full"),
        logging.WARNING,
        "dropped a message to app:account/a-1: the mailbox is full",
    ),
]
"""Every kind of event, the level it is logged at and the line it makes."""
NAP = Nap(timedelta(milliseconds=200))


def heard_by_default(monkeypatch: pytest.MonkeyPatch) -> list[Event]:
    """Every event given to the observer a system builds when it is given none, which still logs them."""
    heard: list[Event] = []

    class Heard(LoggingObserver):
        def __call__(self, event: Event, /) -> None:
            heard.append(event)
            super().__call__(event)

    monkeypatch.setattr(casty, "LoggingObserver", Heard)
    return heard


def activations(events: list[Event]) -> list[type[Event]]:
    return [type(event) for event in events if isinstance(event, ActivationStarted | ActivationEnded)]


def describe_logging_observer() -> None:
    @pytest.mark.parametrize(("event", "level", "line"), LOGGED, ids=[type(event).__name__ for event, _, _ in LOGGED])
    def it_logs_each_event_on_the_casty_logger_at_its_level(
        event: Event, level: int, line: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="casty")

        LoggingObserver()(event)

        assert [(record.name, record.levelno, record.getMessage()) for record in caplog.records] == [
            ("casty", level, line)
        ]

    def it_carries_the_exception_of_a_body_that_failed(caplog: pytest.LogCaptureFixture) -> None:
        LoggingObserver()(ActivationFailed("app:account", "a-1", BROKEN))

        (record,) = caplog.records
        assert record.exc_info is not None
        assert record.exc_info[1] is BROKEN

    def when_a_system_is_built_without_an_observer() -> None:
        async def it_logs_a_dropped_tell_as_it_always_did(caplog: pytest.LogCaptureFixture) -> None:
            async with ActorSystem() as system:
                # One message runs or waits, and a mailbox of one holds the next: the third has nowhere to go.
                for _ in range(3):
                    system.ref(sleepy, "s-1").tell(NAP)

                line = f"dropped a message to {sleepy.name}/s-1: the mailbox is full"

                async def the_drop_was_logged() -> None:
                    logged = [(record.name, record.levelno, record.getMessage()) for record in caplog.records]
                    assert ("casty", logging.WARNING, line) in logged

                await eventually(the_drop_was_logged)

        async def it_builds_no_activation_event_while_the_logger_takes_no_debug_records(
            caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
        ) -> None:
            caplog.set_level(logging.INFO, logger="casty")
            heard = heard_by_default(monkeypatch)

            async with ActorSystem(idle_after=timedelta(milliseconds=100)) as system:
                for _ in range(3):
                    system.ref(sleepy, "s-1").tell(NAP)

                async def the_drop_was_heard_and_the_key_idled_out() -> None:
                    assert any(isinstance(event, MessageDropped) for event in heard)
                    assert system.stats().actors[sleepy.name].active == 0

                await eventually(the_drop_was_heard_and_the_key_idled_out)

            assert activations(heard) == []

        async def it_reports_activations_when_the_logger_takes_debug_records(
            caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
        ) -> None:
            caplog.set_level(logging.DEBUG, logger="casty")
            heard = heard_by_default(monkeypatch)

            async with ActorSystem(idle_after=timedelta(milliseconds=100)) as system:
                system.ref(sleepy, "s-1").tell(NAP)

                async def it_started_and_ended() -> None:
                    assert activations(heard) == [ActivationStarted, ActivationEnded]

                await eventually(it_started_and_ended)

            logged = [(record.name, record.levelno, record.getMessage()) for record in caplog.records]
            assert ("casty", logging.DEBUG, f"activated {sleepy.name}/s-1") in logged

        async def it_logs_a_member_of_its_cluster_that_died(caplog: pytest.LogCaptureFixture) -> None:
            async with Harness.start(3) as harness:
                crashed = harness.nodes[2].system.node
                named = f"{crashed.address} ({crashed.incarnation.hex[:8]})"
                await harness.crash(harness.nodes[2])

                async def the_death_was_logged() -> None:
                    lines = [
                        record.getMessage()
                        for record in caplog.records
                        if record.name == "casty" and record.levelno == logging.WARNING
                    ]
                    assert any(
                        line.startswith(f"member {named} went from ") and line.endswith(" to dead") for line in lines
                    ), lines

                await eventually(the_death_was_logged, timedelta(seconds=10))

    def when_a_system_has_an_observer_of_its_own() -> None:
        async def it_reports_to_it_and_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:
            caplog.set_level(logging.DEBUG, logger="casty")
            events: list[Event] = []

            async with ActorSystem(observer=events.append) as system:
                for _ in range(3):
                    system.ref(sleepy, "s-1").tell(NAP)

                async def the_drop_was_reported() -> None:
                    assert any(isinstance(event, MessageDropped) for event in events)

                await eventually(the_drop_was_reported)

            assert [record for record in caplog.records if record.name == "casty"] == []
