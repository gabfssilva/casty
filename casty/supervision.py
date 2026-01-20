# casty/supervision.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol


class DecisionType(Enum):
    RESTART = auto()
    STOP = auto()
    RESUME = auto()
    ESCALATE = auto()


@dataclass(frozen=True)
class Decision:
    type: DecisionType

    @classmethod
    def restart(cls) -> Decision:
        return cls(DecisionType.RESTART)

    @classmethod
    def stop(cls) -> Decision:
        return cls(DecisionType.STOP)

    @classmethod
    def resume(cls) -> Decision:
        return cls(DecisionType.RESUME)

    @classmethod
    def escalate(cls) -> Decision:
        return cls(DecisionType.ESCALATE)


class SupervisionStrategy(Protocol):
    def decide(self, error: Exception, retries: int) -> Decision: ...


@dataclass
class Restart:
    max_retries: int = 3
    within: float = 60.0

    def decide(self, error: Exception, retries: int) -> Decision:
        if retries >= self.max_retries:
            return Decision.stop()
        return Decision.restart()


@dataclass
class Stop:
    def decide(self, error: Exception, retries: int) -> Decision:
        return Decision.stop()


@dataclass
class Escalate:
    def decide(self, error: Exception, retries: int) -> Decision:
        return Decision.escalate()


@dataclass
class OneForOne:
    affects_siblings: bool = False


@dataclass
class AllForOne:
    affects_siblings: bool = True


@dataclass
class SupervisionConfig:
    strategy: SupervisionStrategy
    scope: OneForOne | AllForOne


def supervised(
    strategy: SupervisionStrategy,
    scope: OneForOne | AllForOne = OneForOne(),
):
    config = SupervisionConfig(strategy=strategy, scope=scope)

    def decorator(actor_func):
        from functools import wraps

        @wraps(actor_func)
        def wrapper(*args, **kwargs):
            behavior = actor_func(*args, **kwargs)
            behavior.supervision = config
            return behavior

        return wrapper

    return decorator
