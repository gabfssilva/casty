"""Shared fixtures and test actors for Casty tests."""

import itertools
import pytest
from dataclasses import dataclass

from casty import Actor, Context

# Global port counter to ensure unique ports across all tests
_port_counter = itertools.count(20000)


# --- Message types ---


@dataclass
class Increment:
    amount: int = 1


@dataclass
class Decrement:
    amount: int = 1


@dataclass
class GetValue:
    pass


@dataclass
class Reset:
    pass


# --- Test actors ---


class Counter(Actor[Increment | Decrement | GetValue | Reset]):
    """Simple counter actor for testing."""

    def __init__(self, initial: int = 0) -> None:
        self.value = initial

    async def receive(self, msg: Increment | Decrement | GetValue | Reset, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.value += amount
            case Decrement(amount):
                self.value -= amount
            case GetValue():
                ctx.reply(self.value)
            case Reset():
                self.value = 0
                ctx.reply(self.value)


# --- Port allocation ---


@pytest.fixture
def get_port():
    """Fixture that returns a function to get unique ports for each test."""
    def _get_port() -> int:
        return next(_port_counter)
    return _get_port


# --- Pytest configuration ---


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow")
