# tests/test_supervision.py
import pytest
from dataclasses import dataclass


def test_decision_types():
    from casty.supervision import Decision

    assert Decision.restart() is not None
    assert Decision.stop() is not None
    assert Decision.resume() is not None
    assert Decision.escalate() is not None


def test_decision_equality():
    from casty.supervision import Decision

    assert Decision.restart() == Decision.restart()
    assert Decision.stop() != Decision.restart()


def test_restart_strategy():
    from casty.supervision import Restart, Decision

    strategy = Restart(max_retries=3, within=60.0)

    # First failure -> restart
    decision = strategy.decide(ValueError("test"), retries=0)
    assert decision == Decision.restart()

    # Under limit -> restart
    decision = strategy.decide(ValueError("test"), retries=2)
    assert decision == Decision.restart()

    # At limit -> stop
    decision = strategy.decide(ValueError("test"), retries=3)
    assert decision == Decision.stop()


def test_one_for_one():
    from casty.supervision import OneForOne

    scope = OneForOne()
    assert scope.affects_siblings is False


def test_all_for_one():
    from casty.supervision import AllForOne

    scope = AllForOne()
    assert scope.affects_siblings is True
