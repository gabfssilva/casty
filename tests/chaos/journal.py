"""What the traffic of a chaos run did, what it must keep true, and what each check of that came to.

A workload is a kind of actor the traffic writes to (`Workload`). Each call it makes is an `Operation` in the
`Journal`: what it asked of which key, when it began and ended on the clock of the run, and whether it was confirmed or
ended ambiguous — `Unavailable` and `TimeoutError` say the call may or may not have been applied, never that it failed.
The invariants are checked against those records, so a check is only ever as strong as what a caller can know.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import math
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from itertools import count
from pathlib import Path
from typing import Literal, Protocol, TypeGuard

from casty import System
from tests.support import eventually
from tests.traffic import AMBIGUOUS

type Outcome = Literal["pending", "confirmed", "ambiguous"]


@dataclass(slots=True)
class Operation:
    """One call of the traffic.

    `argument` is the value the call is about, when it has one: what it wrote, removed or asked for. `result` is what
    it answered, or the error it ended with, in short.
    """

    id: int
    workload: str
    key: str
    action: str
    argument: int | None
    started: float
    ended: float | None = None
    outcome: Outcome = "pending"
    result: str = ""

    @property
    def confirmed(self) -> bool:
        return self.outcome == "confirmed"

    @property
    def finished(self) -> float:
        """When the call ended, or infinity while it has not."""
        return math.inf if self.ended is None else self.ended

    @property
    def applied_by(self) -> float:
        """When the call had surely taken effect: its end if it was confirmed. One that was not may land at any moment
        after it began, however it ended."""
        return self.finished if self.confirmed else math.inf

    def encoded(self) -> dict[str, object]:
        return {
            "id": self.id,
            "workload": self.workload,
            "key": self.key,
            "action": self.action,
            "argument": self.argument,
            "started": round(self.started, 4),
            "ended": None if self.ended is None else round(self.ended, 4),
            "outcome": self.outcome,
            "result": self.result,
        }

    def __str__(self) -> str:
        what = self.action if self.argument is None else f"{self.action} {self.argument}"
        until = "…" if self.ended is None else f"{self.ended:.3f}"
        return f"#{self.id} {what} on {self.key} [{self.started:.3f}, {until}] {self.outcome}"


class Journal:
    """Every operation of a run, in the order it began, on a clock that starts with the run."""

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._origin = self._loop.time()
        self.operations: list[Operation] = []

    def now(self) -> float:
        return self._loop.time() - self._origin

    def begin(self, workload: str, key: str, action: str, argument: int | None = None, /) -> Operation:
        operation = Operation(len(self.operations) + 1, workload, key, action, argument, self.now())
        self.operations.append(operation)
        return operation

    def confirm(self, operation: Operation, result: object = None, /) -> None:
        operation.ended = self.now()
        operation.outcome = "confirmed"
        operation.result = _brief(repr(result))

    def fail(self, operation: Operation, error: BaseException, /) -> None:
        operation.ended = self.now()
        operation.outcome = "ambiguous"
        operation.result = _brief(f"{type(error).__name__}: {error}")

    def dump(self, path: Path, /) -> None:
        """Write every operation to `path`, one JSON object per line, compressed."""
        with gzip.open(path, "wt", encoding="utf-8") as sink:
            for operation in self.operations:
                sink.write(json.dumps(operation.encoded()) + "\n")


@dataclass(frozen=True)
class Verdict:
    """What one invariant came to at one audit: how many things it checked, and each one that broke it."""

    invariant: str
    checked: int
    violations: tuple[str, ...] = ()


@dataclass(frozen=True)
class Stage:
    """What a workload is built with: the journal it records into, the systems its calls go out from, and the
    advertised addresses of the machines up at each moment, for the types placed by address instead of by the ring."""

    journal: Journal
    senders: tuple[System, ...]
    up: Callable[[], tuple[str, ...]]


class Workload(Protocol):
    """A kind of actor the traffic writes to, and what it must keep true through the faults.

    `operate` makes one call from `stage.senders[sender]` and records it. `audit` checks what holds with the traffic
    still running, retrying each check for `within` since reads are not linearizable; `rest` checks, once every call
    has ended, what only holds then, and may consume what the workload wrote. `outage` says every machine was killed at
    once, the last of them dead at `dead` on the clock of the run: what lived only in their memory is gone, and a call
    that began before `dead` may have been applied by the cluster that went, and gone with it.
    """

    @property
    def name(self) -> str: ...

    async def operate(self, sender: int, rng: random.Random, /) -> None: ...

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]: ...

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]: ...

    def outage(self, dead: float, /) -> None: ...


def ambiguous(error: BaseException, /) -> bool:
    """Whether `error` only says the call may or may not have been applied: alone, or as each error of a fan-out."""
    if _grouped(error):
        return all(ambiguous(inner) for inner in error.exceptions)
    return isinstance(error, AMBIGUOUS)


def _grouped(error: BaseException, /) -> TypeGuard[BaseExceptionGroup[BaseException]]:
    return isinstance(error, BaseExceptionGroup)


def _described(error: BaseException, /) -> str:
    """The errors a fan-out raised, not the group that carries them."""
    if _grouped(error):
        return "; ".join(_described(inner) for inner in error.exceptions)
    return f"{type(error).__name__}: {error}"


async def settles[K](keys: Sequence[K], check: Callable[[K], Awaitable[None]], within: timedelta, /) -> tuple[str, ...]:
    """Run `check` on every key at once until it passes, each for `within`, and give back what still failed.

    A check fails by raising `AssertionError`; a key its reads never reach within `within` fails as unreadable.
    """
    failures: list[str] = []

    async def settled(key: K) -> None:
        attempts = count()

        async def paced() -> None:
            # A check that failed is not asked again at once: every attempt is a call to a cluster under traffic.
            if next(attempts):
                await asyncio.sleep(_RETRY)
            await check(key)

        try:
            await eventually(paced, within)
        except AssertionError as broken:
            failures.append(str(broken))
        except Exception as error:
            if not ambiguous(error):
                raise
            failures.append(f"{key} unreadable for {within.total_seconds():.0f}s: {_described(error)}")

    async with asyncio.TaskGroup() as checks:
        for key in keys:
            checks.create_task(settled(key))
    return tuple(failures)


def _brief(text: str) -> str:
    return text if len(text) <= _BRIEF else f"{text[: _BRIEF - 1]}…"


_BRIEF = 200
_RETRY = 0.2
"""Seconds between two attempts at a check that failed."""
