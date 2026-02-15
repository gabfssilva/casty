"""Minimal composable behavior primitive.

A single frozen dataclass with three modes: receive a message, run setup,
or signal a transition. Everything else (lifecycle, supervision, spy,
discovery) is built on top of this by higher layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from collections.abc import Awaitable, Callable
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.core.context import ActorContext


class Signal(Enum):
    same = auto()
    stopped = auto()
    restart = auto()
    unhandled = auto()


@dataclass(frozen=True, kw_only=True)
class Behavior[M]:
    """Minimal actor behavior primitive.

    Exactly one of ``on_receive``, ``on_setup``, or ``signal`` is set.
    """

    on_receive: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]] | None = None
    on_setup: Callable[[ActorContext[M]], Awaitable[Behavior[M]]] | None = None
    signal: Signal | None = None

    @staticmethod
    def receive[T](
        handler: Callable[[ActorContext[T], T], Awaitable[Behavior[T]]],
    ) -> Behavior[T]:
        return Behavior(on_receive=handler)

    @staticmethod
    def setup[T](
        factory: Callable[[ActorContext[T]], Awaitable[Behavior[T]]],
    ) -> Behavior[T]:
        return Behavior(on_setup=factory)

    @staticmethod
    def same() -> Behavior[Any]:
        return Behavior(signal=Signal.same)

    @staticmethod
    def stopped() -> Behavior[Any]:
        return Behavior(signal=Signal.stopped)

    @staticmethod
    def unhandled() -> Behavior[Any]:
        return Behavior(signal=Signal.unhandled)

    @staticmethod
    def restart() -> Behavior[Any]:
        return Behavior(signal=Signal.restart)
