from __future__ import annotations

import asyncio
import time

import pytest

from casty.supervision import Directive, OneForOneStrategy, SupervisionStrategy


async def test_directive_values() -> None:
    assert Directive.restart.name == "restart"
    assert Directive.stop.name == "stop"
    assert Directive.escalate.name == "escalate"


async def test_one_for_one_default_restarts() -> None:
    strategy = OneForOneStrategy()
    assert strategy.decide(RuntimeError("boom")) == Directive.restart


async def test_one_for_one_custom_decider() -> None:
    strategy = OneForOneStrategy(
        decider=lambda exc: (
            Directive.restart if isinstance(exc, ValueError) else Directive.stop
        ),
    )
    assert strategy.decide(ValueError("bad")) == Directive.restart
    assert strategy.decide(RuntimeError("fatal")) == Directive.stop


async def test_one_for_one_max_restarts_exceeded() -> None:
    strategy = OneForOneStrategy(max_restarts=2, within=60.0)
    child_id = "child-1"

    assert strategy.decide(RuntimeError(), child_id=child_id) == Directive.restart
    assert strategy.decide(RuntimeError(), child_id=child_id) == Directive.restart
    assert strategy.decide(RuntimeError(), child_id=child_id) == Directive.stop


async def test_one_for_one_restarts_reset_after_window() -> None:
    strategy = OneForOneStrategy(max_restarts=1, within=0.1)
    child_id = "child-1"

    assert strategy.decide(RuntimeError(), child_id=child_id) == Directive.restart
    assert strategy.decide(RuntimeError(), child_id=child_id) == Directive.stop

    # Wait for window to pass
    await asyncio.sleep(0.15)
    assert strategy.decide(RuntimeError(), child_id=child_id) == Directive.restart


async def test_one_for_one_tracks_children_independently() -> None:
    strategy = OneForOneStrategy(max_restarts=1, within=60.0)

    assert strategy.decide(RuntimeError(), child_id="a") == Directive.restart
    assert strategy.decide(RuntimeError(), child_id="b") == Directive.restart
    assert strategy.decide(RuntimeError(), child_id="a") == Directive.stop
    assert strategy.decide(RuntimeError(), child_id="b") == Directive.stop


async def test_supervision_strategy_protocol() -> None:
    class CustomStrategy:
        def decide(self, exception: Exception, **kwargs: object) -> Directive:
            return Directive.escalate

    s: SupervisionStrategy = CustomStrategy()
    assert s.decide(RuntimeError()) == Directive.escalate
