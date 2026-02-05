# Casty Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a functional actor model library for Python with typed behaviors, hierarchical supervision, and asyncio runtime.

**Architecture:** Akka Typed-inspired functional API where `Behavior[M]` is a tagged union of frozen dataclasses. Actors are spawned via `ActorContext`, communicate via fire-and-forget `tell`, and external code bridges in via `ActorSystem.ask`. An internal `_ActorCell` runs each actor's message loop as an asyncio task.

**Tech Stack:** Python 3.13+, asyncio, uv, pytest + pytest-asyncio, pyright strict

---

## Phase 1: Foundation (Sequential)

These tasks MUST run sequentially — everything else depends on them.

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/casty/__init__.py`
- Create: `tests/__init__.py`

**Step 1: Create pyproject.toml**

```toml
[project]
name = "casty"
version = "0.1.0"
description = "Actor model library for Python"
requires-python = ">=3.13"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.backends"

[tool.hatch.build.targets.wheel]
packages = ["src/casty"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.25",
    "pyright>=1.1",
]

[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "strict"
```

**Step 2: Create empty package files**

- `src/casty/__init__.py` — empty for now
- `tests/__init__.py` — empty

**Step 3: Install dependencies**

Run: `uv sync`
Expected: Dependencies install successfully.

**Step 4: Verify setup**

Run: `uv run pytest --co`
Expected: "no tests ran" (no test files yet), exit 0.

---

### Task 2: Behavior Types + Behaviors Factory (`actor.py`)

**Files:**
- Create: `src/casty/actor.py`
- Create: `tests/test_behavior.py`

**Context:** `Behavior[M]` is a PEP 695 type alias — a union of frozen dataclasses representing behavior variants. `Behaviors` is a static factory class. Sentinel behaviors (`SameBehavior`, `StoppedBehavior`, `UnhandledBehavior`, `RestartBehavior`) are NOT generic — they carry no message type info.

**Step 1: Write tests**

```python
# tests/test_behavior.py
from __future__ import annotations

import pytest

from casty.actor import (
    Behaviors,
    LifecycleBehavior,
    ReceiveBehavior,
    RestartBehavior,
    SameBehavior,
    SetupBehavior,
    StoppedBehavior,
    SupervisedBehavior,
    UnhandledBehavior,
)


async def test_receive_creates_receive_behavior() -> None:
    async def handler(ctx: object, msg: str) -> SameBehavior:
        return Behaviors.same()

    b = Behaviors.receive(handler)
    assert isinstance(b, ReceiveBehavior)
    assert b.handler is handler


async def test_setup_creates_setup_behavior() -> None:
    async def factory(ctx: object) -> SameBehavior:
        return Behaviors.same()

    b = Behaviors.setup(factory)
    assert isinstance(b, SetupBehavior)
    assert b.factory is factory


async def test_same_returns_singleton_type() -> None:
    b = Behaviors.same()
    assert isinstance(b, SameBehavior)


async def test_stopped_returns_singleton_type() -> None:
    b = Behaviors.stopped()
    assert isinstance(b, StoppedBehavior)


async def test_unhandled_returns_singleton_type() -> None:
    b = Behaviors.unhandled()
    assert isinstance(b, UnhandledBehavior)


async def test_restart_returns_singleton_type() -> None:
    b = Behaviors.restart()
    assert isinstance(b, RestartBehavior)


async def test_with_lifecycle_wraps_behavior() -> None:
    inner = Behaviors.receive(lambda ctx, msg: Behaviors.same())
    pre = lambda ctx: None
    post = lambda ctx: None

    b = Behaviors.with_lifecycle(inner, pre_start=pre, post_stop=post)
    assert isinstance(b, LifecycleBehavior)
    assert b.behavior is inner
    assert b.pre_start is pre
    assert b.post_stop is post
    assert b.pre_restart is None
    assert b.post_restart is None


async def test_supervise_wraps_behavior_with_strategy() -> None:
    inner = Behaviors.receive(lambda ctx, msg: Behaviors.same())

    class FakeStrategy:
        def decide(self, exc: Exception) -> None:
            pass

    strategy = FakeStrategy()
    b = Behaviors.supervise(inner, strategy)  # type: ignore[arg-type]
    assert isinstance(b, SupervisedBehavior)
    assert b.behavior is inner
    assert b.strategy is strategy
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_behavior.py -v`
Expected: ImportError — `casty.actor` doesn't exist yet.

**Step 3: Implement `actor.py`**

```python
# src/casty/actor.py
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.supervision import SupervisionStrategy


@dataclass(frozen=True)
class ReceiveBehavior[M]:
    handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]]


@dataclass(frozen=True)
class SetupBehavior[M]:
    factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]]


@dataclass(frozen=True)
class SameBehavior:
    pass


@dataclass(frozen=True)
class StoppedBehavior:
    pass


@dataclass(frozen=True)
class UnhandledBehavior:
    pass


@dataclass(frozen=True)
class RestartBehavior:
    pass


@dataclass(frozen=True)
class LifecycleBehavior[M]:
    behavior: Behavior[M]
    pre_start: Callable[[ActorContext[M]], Awaitable[None]] | None = None
    post_stop: Callable[[ActorContext[M]], Awaitable[None]] | None = None
    pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]] | None = None
    post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None


@dataclass(frozen=True)
class SupervisedBehavior[M]:
    behavior: Behavior[M]
    strategy: SupervisionStrategy


type Behavior[M] = (
    ReceiveBehavior[M]
    | SetupBehavior[M]
    | SameBehavior
    | StoppedBehavior
    | UnhandledBehavior
    | RestartBehavior
    | LifecycleBehavior[M]
    | SupervisedBehavior[M]
)


class Behaviors:
    @staticmethod
    def receive[M](
        handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]],
    ) -> ReceiveBehavior[M]:
        return ReceiveBehavior(handler)

    @staticmethod
    def setup[M](
        factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]],
    ) -> SetupBehavior[M]:
        return SetupBehavior(factory)

    @staticmethod
    def same() -> SameBehavior:
        return SameBehavior()

    @staticmethod
    def stopped() -> StoppedBehavior:
        return StoppedBehavior()

    @staticmethod
    def unhandled() -> UnhandledBehavior:
        return UnhandledBehavior()

    @staticmethod
    def restart() -> RestartBehavior:
        return RestartBehavior()

    @staticmethod
    def with_lifecycle[M](
        behavior: Behavior[M],
        *,
        pre_start: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
        post_stop: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
        pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]] | None = None,
        post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
    ) -> LifecycleBehavior[M]:
        return LifecycleBehavior(behavior, pre_start, post_stop, pre_restart, post_restart)

    @staticmethod
    def supervise[M](
        behavior: Behavior[M],
        strategy: SupervisionStrategy,
    ) -> SupervisedBehavior[M]:
        return SupervisedBehavior(behavior, strategy)
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_behavior.py -v`
Expected: All tests PASS.

---

### Task 3: ActorRef + System Messages (`ref.py`, `messages.py`)

**Files:**
- Create: `src/casty/ref.py`
- Create: `src/casty/messages.py`
- Create: `tests/test_ref.py`
- Create: `tests/test_messages.py`

**Context:** `ActorRef[M]` is a frozen dataclass wrapping an internal send callable. Only exposes `tell(msg: M)`. `Terminated` is a frozen dataclass that actors receive when a watched actor dies.

**Step 1: Write tests**

```python
# tests/test_ref.py
from __future__ import annotations

from casty.ref import ActorRef


async def test_tell_calls_internal_send() -> None:
    sent: list[str] = []
    ref: ActorRef[str] = ActorRef(_send=sent.append)
    ref.tell("hello")
    assert sent == ["hello"]


async def test_tell_preserves_message_order() -> None:
    sent: list[int] = []
    ref: ActorRef[int] = ActorRef(_send=sent.append)
    ref.tell(1)
    ref.tell(2)
    ref.tell(3)
    assert sent == [1, 2, 3]


async def test_ref_is_frozen() -> None:
    import pytest
    ref: ActorRef[str] = ActorRef(_send=lambda m: None)
    with pytest.raises(AttributeError):
        ref._send = lambda m: None  # type: ignore[misc]
```

```python
# tests/test_messages.py
from __future__ import annotations

from casty.messages import Terminated
from casty.ref import ActorRef


async def test_terminated_holds_ref() -> None:
    ref: ActorRef[str] = ActorRef(_send=lambda m: None)
    t = Terminated(ref=ref)
    assert t.ref is ref


async def test_terminated_is_frozen() -> None:
    import pytest
    ref: ActorRef[str] = ActorRef(_send=lambda m: None)
    t = Terminated(ref=ref)
    with pytest.raises(AttributeError):
        t.ref = ref  # type: ignore[misc]
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ref.py tests/test_messages.py -v`
Expected: ImportError.

**Step 3: Implement**

```python
# src/casty/ref.py
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ActorRef[M]:
    _send: Callable[[M], None]

    def tell(self, msg: M) -> None:
        self._send(msg)
```

```python
# src/casty/messages.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.ref import ActorRef


@dataclass(frozen=True)
class Terminated:
    ref: ActorRef[Any]
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ref.py tests/test_messages.py -v`
Expected: All PASS.

---

### Task 4: ActorContext Protocol (`context.py`)

**Files:**
- Create: `src/casty/context.py`

**Context:** `ActorContext[M]` is a Protocol — the public interface that actors see. The actual implementation lives in `_cell.py` (Phase 3). We define it now so other modules can reference it for type annotations.

**Step 1: Implement protocol**

```python
# src/casty/context.py
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from casty.mailbox import Mailbox
    from casty.ref import ActorRef

    type Behavior[M] = Any  # avoid circular import; actual type in actor.py


class ActorContext[M](Protocol):
    @property
    def self(self) -> ActorRef[M]: ...

    @property
    def log(self) -> logging.Logger: ...

    def spawn[C](
        self,
        behavior: Any,  # Behavior[C] — can't reference without circular import
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]: ...

    def stop(self, ref: ActorRef[Any]) -> None: ...

    def watch(self, ref: ActorRef[Any]) -> None: ...

    def unwatch(self, ref: ActorRef[Any]) -> None: ...
```

No tests needed — it's a Protocol (structural typing, no runtime behavior).

**Step 2: Verify imports work**

Run: `uv run python -c "from casty.context import ActorContext; print('OK')"`
Expected: `OK`

---

## Phase 2: Independent Modules (Parallel)

These three tasks have NO dependencies on each other. They depend only on Phase 1 types. **Run with parallel subagents.**

### Task 5: Mailbox Module (`mailbox.py`) — Agent A

**Files:**
- Create: `src/casty/mailbox.py`
- Create: `tests/test_mailbox.py`

**Context:** Mailbox wraps `asyncio.Queue`. Default unbounded. Bounded with configurable overflow: `drop_oldest`, `drop_new`, `backpressure`. Processing is serial — one message at a time.

**Step 1: Write tests**

```python
# tests/test_mailbox.py
from __future__ import annotations

import asyncio

import pytest

from casty.mailbox import Mailbox, MailboxOverflowStrategy


async def test_unbounded_mailbox_put_and_get() -> None:
    mb: Mailbox[str] = Mailbox()
    mb.put("hello")
    mb.put("world")
    assert await mb.get() == "hello"
    assert await mb.get() == "world"


async def test_unbounded_mailbox_no_limit() -> None:
    mb: Mailbox[int] = Mailbox()
    for i in range(10_000):
        mb.put(i)
    assert mb.size() == 10_000


async def test_bounded_drop_new_discards_incoming() -> None:
    mb: Mailbox[int] = Mailbox(capacity=2, overflow=MailboxOverflowStrategy.drop_new)
    mb.put(1)
    mb.put(2)
    mb.put(3)  # dropped
    assert mb.size() == 2
    assert await mb.get() == 1
    assert await mb.get() == 2


async def test_bounded_drop_oldest_discards_oldest() -> None:
    mb: Mailbox[int] = Mailbox(capacity=2, overflow=MailboxOverflowStrategy.drop_oldest)
    mb.put(1)
    mb.put(2)
    mb.put(3)  # 1 is dropped
    assert mb.size() == 2
    assert await mb.get() == 2
    assert await mb.get() == 3


async def test_bounded_backpressure_blocks_until_space() -> None:
    mb: Mailbox[int] = Mailbox(capacity=1, overflow=MailboxOverflowStrategy.backpressure)
    mb.put(1)

    put_done = False

    async def delayed_put() -> None:
        nonlocal put_done
        await mb.put_async(2)
        put_done = True

    task = asyncio.create_task(delayed_put())
    await asyncio.sleep(0.05)
    assert not put_done  # blocked

    await mb.get()  # free space
    await asyncio.sleep(0.05)
    assert put_done
    assert await mb.get() == 2
    task.cancel()


async def test_get_blocks_until_message() -> None:
    mb: Mailbox[str] = Mailbox()
    result: list[str] = []

    async def consumer() -> None:
        result.append(await mb.get())

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)
    assert result == []

    mb.put("hello")
    await asyncio.sleep(0.05)
    assert result == ["hello"]
    task.cancel()


async def test_empty_returns_true_when_empty() -> None:
    mb: Mailbox[str] = Mailbox()
    assert mb.empty()
    mb.put("x")
    assert not mb.empty()
```

**Step 2: Run tests — expect failure**

Run: `uv run pytest tests/test_mailbox.py -v`

**Step 3: Implement**

```python
# src/casty/mailbox.py
from __future__ import annotations

import asyncio
from enum import Enum, auto


class MailboxOverflowStrategy(Enum):
    drop_new = auto()
    drop_oldest = auto()
    backpressure = auto()


class Mailbox[M]:
    def __init__(
        self,
        capacity: int | None = None,
        overflow: MailboxOverflowStrategy = MailboxOverflowStrategy.drop_new,
    ) -> None:
        self._overflow = overflow
        if capacity is None:
            self._queue: asyncio.Queue[M] = asyncio.Queue()
        else:
            self._queue = asyncio.Queue(maxsize=capacity)
        self._capacity = capacity

    def put(self, msg: M) -> None:
        if self._capacity is None:
            self._queue.put_nowait(msg)
            return

        match self._overflow:
            case MailboxOverflowStrategy.drop_new:
                if not self._queue.full():
                    self._queue.put_nowait(msg)
            case MailboxOverflowStrategy.drop_oldest:
                if self._queue.full():
                    try:
                        self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                self._queue.put_nowait(msg)
            case MailboxOverflowStrategy.backpressure:
                if not self._queue.full():
                    self._queue.put_nowait(msg)
                else:
                    raise asyncio.QueueFull()

    async def put_async(self, msg: M) -> None:
        await self._queue.put(msg)

    async def get(self) -> M:
        return await self._queue.get()

    def size(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()
```

**Step 4: Run tests — expect pass**

Run: `uv run pytest tests/test_mailbox.py -v`

---

### Task 6: EventStream + Events (`events.py`) — Agent B

**Files:**
- Create: `src/casty/events.py`
- Create: `tests/test_events.py`

**Context:** Typed pub/sub. Events are frozen dataclasses. `EventStream` allows subscribing by event type and publishing events. Handlers are async callables.

**Step 1: Write tests**

```python
# tests/test_events.py
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
```

**Step 2: Run tests — expect failure**

Run: `uv run pytest tests/test_events.py -v`

**Step 3: Implement**

```python
# src/casty/events.py
from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from casty.ref import ActorRef


# --- Event types ---

@dataclass(frozen=True)
class ActorStarted:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class ActorStopped:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class ActorRestarted:
    ref: ActorRef[Any]
    exception: Exception


@dataclass(frozen=True)
class DeadLetter:
    message: Any
    intended_ref: ActorRef[Any]


@dataclass(frozen=True)
class UnhandledMessage:
    message: Any
    ref: ActorRef[Any]


# --- EventStream ---

type EventHandler[E] = Callable[[E], Awaitable[None] | None]


class EventStream:
    def __init__(self) -> None:
        self._subscribers: dict[type, list[EventHandler[Any]]] = defaultdict(list)

    def subscribe[E](self, event_type: type[E], handler: EventHandler[E]) -> None:
        self._subscribers[event_type].append(handler)

    def unsubscribe[E](self, event_type: type[E], handler: EventHandler[E]) -> None:
        handlers = self._subscribers.get(event_type)
        if handlers:
            handlers.remove(handler)

    async def publish[E](self, event: E) -> None:
        handlers = self._subscribers.get(type(event), [])
        for handler in handlers:
            result = handler(event)
            if asyncio.iscoroutine(result):
                await result
```

**Step 4: Run tests — expect pass**

Run: `uv run pytest tests/test_events.py -v`

---

### Task 7: Supervision Module (`supervision.py`) — Agent C

**Files:**
- Create: `src/casty/supervision.py`
- Create: `tests/test_supervision.py`

**Context:** `Directive` is an enum (restart, stop, escalate). `SupervisionStrategy` is a Protocol with `decide(exception) -> Directive`. `OneForOneStrategy` tracks restarts within a time window.

**Step 1: Write tests**

```python
# tests/test_supervision.py
from __future__ import annotations

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


import asyncio
```

**Step 2: Run tests — expect failure**

Run: `uv run pytest tests/test_supervision.py -v`

**Step 3: Implement**

```python
# src/casty/supervision.py
from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable
from enum import Enum, auto
from typing import Protocol


class Directive(Enum):
    restart = auto()
    stop = auto()
    escalate = auto()


class SupervisionStrategy(Protocol):
    def decide(self, exception: Exception, **kwargs: object) -> Directive: ...


class OneForOneStrategy:
    def __init__(
        self,
        max_restarts: int = 3,
        within: float = 60.0,
        decider: Callable[[Exception], Directive] | None = None,
    ) -> None:
        self._max_restarts = max_restarts
        self._within = within
        self._decider = decider
        self._restart_timestamps: dict[str, list[float]] = defaultdict(list)

    def decide(
        self, exception: Exception, *, child_id: str = "__default__", **kwargs: object
    ) -> Directive:
        if self._decider:
            directive = self._decider(exception)
            if directive != Directive.restart:
                return directive

        now = time.monotonic()
        timestamps = self._restart_timestamps[child_id]

        # Prune timestamps outside the window
        cutoff = now - self._within
        timestamps[:] = [t for t in timestamps if t > cutoff]

        if len(timestamps) >= self._max_restarts:
            return Directive.stop

        timestamps.append(now)
        return Directive.restart
```

**Step 4: Run tests — expect pass**

Run: `uv run pytest tests/test_supervision.py -v`

---

## Phase 3: Runtime Assembly (Sequential)

These tasks integrate everything. They MUST run sequentially.

### Task 8: ActorCell — Internal Runtime (`_cell.py`)

**Files:**
- Create: `src/casty/_cell.py`
- Create: `tests/test_cell.py`

**Context:** `_ActorCell[M]` is the internal engine. It owns the mailbox, runs an asyncio task for the message loop, unwraps behavior layers (Setup/Lifecycle/Supervise), manages children, handles failures via supervision, and fires lifecycle hooks. The cell also provides the concrete `ActorContext` implementation.

This is the most complex module. Key behaviors:
- Unwrap initial behavior: Supervise → extract strategy. Lifecycle → extract hooks. Setup → run factory.
- Message loop: pull from mailbox → call handler → match next behavior.
- On exception: parent's strategy decides (restart/stop/escalate).
- On stop: call post_stop, stop all children, notify watchers with `Terminated`.
- On restart: call pre_restart, re-init from initial behavior, call post_restart.

**Step 1: Write tests**

```python
# tests/test_cell.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty.actor import Behaviors, ReceiveBehavior, Behavior
from casty.events import EventStream, ActorStarted, ActorStopped, DeadLetter
from casty.mailbox import Mailbox
from casty.messages import Terminated
from casty.ref import ActorRef
from casty.supervision import Directive, OneForOneStrategy
from casty._cell import ActorCell


@dataclass(frozen=True)
class Ping:
    text: str


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[str]


async def test_cell_processes_messages() -> None:
    received: list[str] = []

    async def handler(ctx: Any, msg: Ping) -> Any:
        received.append(msg.text)
        return Behaviors.same()

    behavior = Behaviors.receive(handler)
    event_stream = EventStream()
    cell: ActorCell[Ping] = ActorCell(
        behavior=behavior, name="test", parent=None, event_stream=event_stream,
    )
    await cell.start()

    cell.ref.tell(Ping("hello"))
    cell.ref.tell(Ping("world"))
    await asyncio.sleep(0.1)

    assert received == ["hello", "world"]
    await cell.stop()


async def test_cell_behavior_state_transition() -> None:
    """Behavior factory returns new behavior with updated state via closure."""
    results: list[int] = []

    def counter(count: int = 0) -> ReceiveBehavior[Ping]:
        async def handler(ctx: Any, msg: Ping) -> Any:
            new_count = count + 1
            results.append(new_count)
            return counter(new_count)
        return Behaviors.receive(handler)

    event_stream = EventStream()
    cell: ActorCell[Ping] = ActorCell(
        behavior=counter(), name="counter", parent=None, event_stream=event_stream,
    )
    await cell.start()

    cell.ref.tell(Ping("a"))
    cell.ref.tell(Ping("b"))
    cell.ref.tell(Ping("c"))
    await asyncio.sleep(0.1)

    assert results == [1, 2, 3]
    await cell.stop()


async def test_cell_setup_behavior() -> None:
    started = False

    async def setup(ctx: Any) -> Any:
        nonlocal started
        started = True
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    behavior = Behaviors.setup(setup)
    event_stream = EventStream()
    cell: ActorCell[str] = ActorCell(
        behavior=behavior, name="setup-test", parent=None, event_stream=event_stream,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    assert started
    await cell.stop()


async def test_cell_stopped_behavior_stops_actor() -> None:
    async def handler(ctx: Any, msg: str) -> Any:
        return Behaviors.stopped()

    event_stream = EventStream()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(handler),
        name="stopper",
        parent=None,
        event_stream=event_stream,
    )
    await cell.start()
    cell.ref.tell("stop")
    await asyncio.sleep(0.1)

    assert cell.is_stopped


async def test_cell_lifecycle_hooks() -> None:
    hooks_called: list[str] = []

    async def pre_start(ctx: Any) -> None:
        hooks_called.append("pre_start")

    async def post_stop(ctx: Any) -> None:
        hooks_called.append("post_stop")

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        pre_start=pre_start,
        post_stop=post_stop,
    )
    event_stream = EventStream()
    cell: ActorCell[str] = ActorCell(
        behavior=behavior, name="lifecycle", parent=None, event_stream=event_stream,
    )
    await cell.start()
    await asyncio.sleep(0.05)
    assert "pre_start" in hooks_called

    await cell.stop()
    await asyncio.sleep(0.05)
    assert "post_stop" in hooks_called


async def test_cell_spawns_children() -> None:
    child_received: list[str] = []

    async def child_handler(ctx: Any, msg: str) -> Any:
        child_received.append(msg)
        return Behaviors.same()

    child_ref: ActorRef[str] | None = None

    async def parent_setup(ctx: Any) -> Any:
        nonlocal child_ref
        child_ref = ctx.spawn(Behaviors.receive(child_handler), "child")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    event_stream = EventStream()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.setup(parent_setup),
        name="parent",
        parent=None,
        event_stream=event_stream,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    assert child_ref is not None
    child_ref.tell("hi from parent")
    await asyncio.sleep(0.05)
    assert child_received == ["hi from parent"]

    await cell.stop()


async def test_cell_parent_stop_stops_children() -> None:
    async def child_setup(ctx: Any) -> Any:
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    child_cell: ActorCell[str] | None = None

    async def parent_setup(ctx: Any) -> Any:
        ctx.spawn(Behaviors.receive(lambda ctx, msg: Behaviors.same()), "child")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    event_stream = EventStream()
    parent = ActorCell(
        behavior=Behaviors.setup(parent_setup),
        name="parent",
        parent=None,
        event_stream=event_stream,
    )
    await parent.start()
    await asyncio.sleep(0.05)

    assert len(parent._children) == 1

    await parent.stop()
    await asyncio.sleep(0.05)

    for child in parent._children.values():
        assert child.is_stopped


async def test_cell_watch_receives_terminated() -> None:
    terminated_received: list[Terminated] = []

    async def watcher_handler(ctx: Any, msg: Any) -> Any:
        if isinstance(msg, Terminated):
            terminated_received.append(msg)
        return Behaviors.same()

    async def watched_handler(ctx: Any, msg: str) -> Any:
        return Behaviors.stopped()

    event_stream = EventStream()
    watcher = ActorCell(
        behavior=Behaviors.receive(watcher_handler),
        name="watcher",
        parent=None,
        event_stream=event_stream,
    )
    watched = ActorCell(
        behavior=Behaviors.receive(watched_handler),
        name="watched",
        parent=None,
        event_stream=event_stream,
    )
    await watcher.start()
    await watched.start()

    watcher.watch(watched)
    watched.ref.tell("die")
    await asyncio.sleep(0.1)

    assert len(terminated_received) == 1
    assert terminated_received[0].ref is watched.ref


async def test_cell_supervision_restarts_on_failure() -> None:
    call_count = 0

    async def failing_handler(ctx: Any, msg: str) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count <= 1:
            raise RuntimeError("boom")
        return Behaviors.same()

    strategy = OneForOneStrategy(max_restarts=3, within=60.0)
    behavior = Behaviors.supervise(Behaviors.receive(failing_handler), strategy)

    event_stream = EventStream()
    cell: ActorCell[str] = ActorCell(
        behavior=behavior, name="restartable", parent=None, event_stream=event_stream,
    )
    await cell.start()

    cell.ref.tell("trigger")
    await asyncio.sleep(0.2)

    # Actor should have been restarted (not stopped)
    assert not cell.is_stopped
    await cell.stop()


async def test_cell_publishes_events_to_stream() -> None:
    events: list[Any] = []
    event_stream = EventStream()
    event_stream.subscribe(ActorStarted, lambda e: events.append(e))  # type: ignore[arg-type,return-value]
    event_stream.subscribe(ActorStopped, lambda e: events.append(e))  # type: ignore[arg-type,return-value]

    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        name="observable",
        parent=None,
        event_stream=event_stream,
    )
    await cell.start()
    await asyncio.sleep(0.05)
    await cell.stop()
    await asyncio.sleep(0.05)

    types = [type(e) for e in events]
    assert ActorStarted in types
    assert ActorStopped in types


async def test_cell_dead_letter_on_tell_after_stop() -> None:
    dead: list[DeadLetter] = []
    event_stream = EventStream()
    event_stream.subscribe(DeadLetter, lambda e: dead.append(e))  # type: ignore[arg-type,return-value]

    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        name="dead",
        parent=None,
        event_stream=event_stream,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    await cell.stop()
    await asyncio.sleep(0.05)

    cell.ref.tell("after-death")
    await asyncio.sleep(0.05)

    assert len(dead) == 1
    assert dead[0].message == "after-death"
```

**Step 2: Run tests — expect failure**

Run: `uv run pytest tests/test_cell.py -v`

**Step 3: Implement `_cell.py`**

This is the most complex file. Key implementation notes:

- `ActorCell.__init__` stores the initial behavior, creates the mailbox, creates the `ActorRef` (whose `_send` puts into mailbox or publishes DeadLetter if stopped).
- `ActorCell.start()` unwraps the behavior layers, runs lifecycle hooks, starts the message loop task.
- `_unwrap_behavior()` recursively processes: `SupervisedBehavior` → extract strategy, `LifecycleBehavior` → extract hooks, `SetupBehavior` → await factory.
- `_run_loop()` is the asyncio task: `while not stopped: msg = await mailbox.get(); next = await handler(ctx, msg); match next`.
- `_handle_failure()` consults the strategy. Restart → call pre_restart, re-unwrap initial behavior, call post_restart. Stop → stop. Escalate → propagate to parent.
- `stop()` stops message loop, calls post_stop, stops all children, notifies watchers.
- The cell provides a concrete `_CellContext` class implementing `ActorContext` protocol (spawn, self, stop, watch, unwatch, log).

```python
# src/casty/_cell.py
from __future__ import annotations

import asyncio
import logging
from typing import Any

from casty.actor import (
    Behavior,
    Behaviors,
    LifecycleBehavior,
    ReceiveBehavior,
    RestartBehavior,
    SameBehavior,
    SetupBehavior,
    StoppedBehavior,
    SupervisedBehavior,
    UnhandledBehavior,
)
from casty.events import (
    ActorRestarted,
    ActorStarted,
    ActorStopped,
    DeadLetter,
    EventStream,
    UnhandledMessage,
)
from casty.mailbox import Mailbox
from casty.messages import Terminated
from casty.ref import ActorRef
from casty.supervision import Directive, OneForOneStrategy, SupervisionStrategy


class _CellContext[M]:
    """Concrete ActorContext implementation backed by an ActorCell."""

    def __init__(self, cell: ActorCell[M]) -> None:
        self._cell = cell

    @property
    def self(self) -> ActorRef[M]:
        return self._cell.ref

    @property
    def log(self) -> logging.Logger:
        return self._cell._log

    def spawn[C](
        self,
        behavior: Behavior[C],
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]:
        return self._cell.spawn_child(behavior, name, mailbox=mailbox)

    def stop(self, ref: ActorRef[Any]) -> None:
        self._cell.stop_child(ref)

    def watch(self, ref: ActorRef[Any]) -> None:
        self._cell.watch_ref(ref)

    def unwatch(self, ref: ActorRef[Any]) -> None:
        self._cell.unwatch_ref(ref)


class ActorCell[M]:
    def __init__(
        self,
        behavior: Behavior[M],
        name: str,
        parent: ActorCell[Any] | None,
        event_stream: EventStream,
        mailbox: Mailbox[M] | None = None,
    ) -> None:
        self._initial_behavior = behavior
        self._name = name
        self._parent = parent
        self._event_stream = event_stream
        self._mailbox: Mailbox[M] = mailbox or Mailbox()
        self._is_stopped = False
        self._current_handler: ReceiveBehavior[M] | None = None
        self._strategy: SupervisionStrategy = OneForOneStrategy()
        self._pre_start: Any = None
        self._post_stop: Any = None
        self._pre_restart: Any = None
        self._post_restart: Any = None
        self._children: dict[str, ActorCell[Any]] = {}
        self._watchers: set[ActorCell[Any]] = set()
        self._watching: set[ActorCell[Any]] = set()
        self._task: asyncio.Task[None] | None = None
        self._ctx = _CellContext(self)
        self._log = logging.getLogger(f"casty.actor.{name}")

        # ActorRef: sends to mailbox if alive, dead letters if stopped
        self._ref: ActorRef[M] = ActorRef(_send=self._deliver)

    def _deliver(self, msg: M) -> None:
        if self._is_stopped:
            asyncio.get_event_loop().create_task(
                self._event_stream.publish(DeadLetter(message=msg, intended_ref=self._ref))
            )
            return
        self._mailbox.put(msg)

    @property
    def ref(self) -> ActorRef[M]:
        return self._ref

    @property
    def is_stopped(self) -> bool:
        return self._is_stopped

    async def start(self) -> None:
        await self._initialize_behavior(self._initial_behavior)
        self._task = asyncio.create_task(self._run_loop())
        await self._event_stream.publish(ActorStarted(ref=self._ref))

    async def _initialize_behavior(self, behavior: Behavior[M]) -> None:
        """Unwrap behavior layers and extract hooks/strategy/handler."""
        match behavior:
            case SupervisedBehavior(inner, strategy):
                self._strategy = strategy
                await self._initialize_behavior(inner)
            case LifecycleBehavior(inner, pre_start, post_stop, pre_restart, post_restart):
                self._pre_start = pre_start
                self._post_stop = post_stop
                self._pre_restart = pre_restart
                self._post_restart = post_restart
                await self._initialize_behavior(inner)
            case SetupBehavior(factory):
                result = await factory(self._ctx)
                await self._initialize_behavior(result)
            case ReceiveBehavior() as handler:
                self._current_handler = handler
                if self._pre_start:
                    await self._pre_start(self._ctx)
            case _:
                raise TypeError(f"Cannot initialize with behavior: {type(behavior)}")

    async def _run_loop(self) -> None:
        while not self._is_stopped:
            try:
                msg = await self._mailbox.get()
            except asyncio.CancelledError:
                return

            if self._is_stopped:
                return

            try:
                assert self._current_handler is not None
                next_behavior = await self._current_handler.handler(self._ctx, msg)
                await self._apply_next(next_behavior)
            except Exception as exc:
                await self._handle_failure(exc)

    async def _apply_next(self, behavior: Behavior[M]) -> None:
        match behavior:
            case SameBehavior():
                pass
            case StoppedBehavior():
                await self.stop()
            case UnhandledBehavior():
                pass  # could publish UnhandledMessage
            case RestartBehavior():
                await self._restart(RuntimeError("Self-requested restart"))
            case ReceiveBehavior() as handler:
                self._current_handler = handler
            case SetupBehavior(factory):
                result = await factory(self._ctx)
                await self._apply_next(result)
            case _:
                await self._initialize_behavior(behavior)

    async def _handle_failure(self, exc: Exception) -> None:
        directive = self._strategy.decide(exc, child_id=self._name)
        match directive:
            case Directive.restart:
                await self._restart(exc)
            case Directive.stop:
                await self.stop()
            case Directive.escalate:
                if self._parent:
                    await self._parent._handle_child_failure(self, exc)
                else:
                    await self.stop()

    async def _handle_child_failure(self, child: ActorCell[Any], exc: Exception) -> None:
        directive = self._strategy.decide(exc, child_id=child._name)
        match directive:
            case Directive.restart:
                await child._restart(exc)
            case Directive.stop:
                await child.stop()
            case Directive.escalate:
                if self._parent:
                    await self._parent._handle_child_failure(child, exc)

    async def _restart(self, exc: Exception) -> None:
        if self._pre_restart:
            await self._pre_restart(self._ctx, exc)

        # Stop children
        for child in list(self._children.values()):
            await child.stop()
        self._children.clear()

        # Re-initialize from initial behavior
        await self._initialize_behavior(self._initial_behavior)

        if self._post_restart:
            await self._post_restart(self._ctx)

        await self._event_stream.publish(ActorRestarted(ref=self._ref, exception=exc))

    async def stop(self) -> None:
        if self._is_stopped:
            return
        self._is_stopped = True

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Stop children
        for child in list(self._children.values()):
            await child.stop()

        if self._post_stop:
            await self._post_stop(self._ctx)

        # Notify watchers
        for watcher in self._watchers:
            if not watcher.is_stopped:
                watcher.ref.tell(Terminated(ref=self._ref))  # type: ignore[arg-type]

        await self._event_stream.publish(ActorStopped(ref=self._ref))

    def spawn_child[C](
        self,
        behavior: Behavior[C],
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]:
        if name in self._children:
            raise ValueError(f"Child '{name}' already exists")

        child: ActorCell[C] = ActorCell(
            behavior=behavior,
            name=f"{self._name}/{name}",
            parent=self,
            event_stream=self._event_stream,
            mailbox=mailbox,
        )
        self._children[name] = child
        asyncio.get_event_loop().create_task(child.start())
        return child.ref

    def stop_child(self, ref: ActorRef[Any]) -> None:
        for name, child in self._children.items():
            if child.ref is ref:
                asyncio.get_event_loop().create_task(child.stop())
                return

    def watch(self, other: ActorCell[Any]) -> None:
        other._watchers.add(self)
        self._watching.add(other)

    def watch_ref(self, ref: ActorRef[Any]) -> None:
        # For now, watch works with cells we know about (children or sibling cells)
        # Full implementation would need a registry lookup
        for child in self._children.values():
            if child.ref is ref:
                self.watch(child)
                return

    def unwatch_ref(self, ref: ActorRef[Any]) -> None:
        for child in self._children.values():
            if child.ref is ref:
                child._watchers.discard(self)
                self._watching.discard(child)
                return
```

**Note for implementer:** The `watch` test creates two root cells and calls `watcher.watch(watched)` directly on the cell (not via context). This works for testing. The full `watch_ref` via context needs a system-level registry — that gets wired in Task 10 (ActorSystem).

**Step 4: Run tests — expect pass**

Run: `uv run pytest tests/test_cell.py -v`

---

### Task 9: ActorSystem (`system.py`)

**Files:**
- Create: `src/casty/system.py`
- Create: `tests/test_system.py`

**Context:** `ActorSystem` is the entry point. Async context manager. Spawns root actors, provides `ask` (ephemeral actor bridge), `lookup` by path, event_stream access, graceful shutdown.

**Step 1: Write tests**

```python
# tests/test_system.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty.actor import Behaviors, Behavior
from casty.events import ActorStarted, DeadLetter
from casty.ref import ActorRef
from casty.system import ActorSystem


@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class GetCount:
    reply_to: ActorRef[int]


type GreeterMsg = Greet | GetCount


def greeter(count: int = 0) -> Behavior[GreeterMsg]:
    async def receive(ctx: Any, msg: GreeterMsg) -> Any:
        match msg:
            case Greet(name):
                return greeter(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


async def test_system_context_manager() -> None:
    async with ActorSystem() as system:
        assert system is not None


async def test_system_spawn_and_tell() -> None:
    received: list[str] = []

    async def handler(ctx: Any, msg: Greet) -> Any:
        received.append(msg.name)
        return Behaviors.same()

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(handler), "greeter")
        ref.tell(Greet("Gabriel"))
        await asyncio.sleep(0.1)

    assert received == ["Gabriel"]


async def test_system_ask() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(greeter(), "greeter")
        ref.tell(Greet("a"))
        ref.tell(Greet("b"))
        await asyncio.sleep(0.1)

        count = await system.ask(ref, lambda r: GetCount(reply_to=r), timeout=5.0)
        assert count == 2


async def test_system_ask_timeout() -> None:
    async def black_hole(ctx: Any, msg: Any) -> Any:
        return Behaviors.same()  # never replies

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(black_hole), "hole")
        with pytest.raises(TimeoutError):
            await system.ask(ref, lambda r: "ignored", timeout=0.1)


async def test_system_lookup() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()), "my-actor"
        )
        found = system.lookup("/my-actor")
        assert found is ref


async def test_system_lookup_not_found() -> None:
    async with ActorSystem() as system:
        assert system.lookup("/nonexistent") is None


async def test_system_event_stream() -> None:
    events: list[ActorStarted] = []

    async with ActorSystem() as system:
        system.event_stream.subscribe(ActorStarted, lambda e: events.append(e))  # type: ignore[arg-type,return-value]
        system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()), "actor"
        )
        await asyncio.sleep(0.1)

    assert len(events) >= 1


async def test_system_shutdown_stops_all_actors() -> None:
    stopped = False

    async def post_stop(ctx: Any) -> None:
        nonlocal stopped
        stopped = True

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        post_stop=post_stop,
    )

    async with ActorSystem() as system:
        system.spawn(behavior, "actor")
        await asyncio.sleep(0.1)

    # After exiting context manager, shutdown was called
    assert stopped


async def test_system_named() -> None:
    async with ActorSystem(name="test-system") as system:
        assert system.name == "test-system"
```

**Step 2: Run tests — expect failure**

Run: `uv run pytest tests/test_system.py -v`

**Step 3: Implement**

```python
# src/casty/system.py
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from casty.actor import Behavior, Behaviors
from casty._cell import ActorCell
from casty.events import EventStream
from casty.mailbox import Mailbox
from casty.ref import ActorRef


class ActorSystem:
    def __init__(self, name: str = "casty-system") -> None:
        self._name = name
        self._event_stream = EventStream()
        self._root_cells: dict[str, ActorCell[Any]] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def event_stream(self) -> EventStream:
        return self._event_stream

    async def __aenter__(self) -> ActorSystem:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]:
        if name in self._root_cells:
            raise ValueError(f"Root actor '{name}' already exists")

        cell: ActorCell[M] = ActorCell(
            behavior=behavior,
            name=name,
            parent=None,
            event_stream=self._event_stream,
            mailbox=mailbox,
        )
        self._root_cells[name] = cell
        asyncio.get_event_loop().create_task(cell.start())
        return cell.ref

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R:
        future: asyncio.Future[R] = asyncio.get_event_loop().create_future()

        def on_reply(msg: R) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = ActorRef(_send=on_reply)
        message = msg_factory(temp_ref)
        ref.tell(message)

        return await asyncio.wait_for(future, timeout=timeout)

    def lookup(self, path: str) -> ActorRef[Any] | None:
        # Strip leading slash
        parts = path.strip("/").split("/")
        if not parts:
            return None

        root_name = parts[0]
        cell = self._root_cells.get(root_name)
        if cell is None:
            return None

        # Navigate children for nested paths
        for part in parts[1:]:
            child = cell._children.get(part)
            if child is None:
                return None
            cell = child

        return cell.ref

    async def shutdown(self) -> None:
        for cell in list(self._root_cells.values()):
            await cell.stop()
        self._root_cells.clear()
```

**Step 4: Run tests — expect pass**

Run: `uv run pytest tests/test_system.py -v`

---

### Task 10: Public API Exports + Integration Tests

**Files:**
- Modify: `src/casty/__init__.py`
- Create: `tests/test_integration.py`

**Context:** Wire up all re-exports. Then write end-to-end tests that exercise the full actor lifecycle: hierarchical actors, supervision with restarts, ask pattern, event stream, dead letters.

**Step 1: Write `__init__.py` exports**

```python
# src/casty/__init__.py
from casty.actor import (
    Behavior,
    Behaviors,
    LifecycleBehavior,
    ReceiveBehavior,
    RestartBehavior,
    SameBehavior,
    SetupBehavior,
    StoppedBehavior,
    SupervisedBehavior,
    UnhandledBehavior,
)
from casty.context import ActorContext
from casty.events import (
    ActorRestarted,
    ActorStarted,
    ActorStopped,
    DeadLetter,
    EventStream,
    UnhandledMessage,
)
from casty.mailbox import Mailbox, MailboxOverflowStrategy
from casty.messages import Terminated
from casty.ref import ActorRef
from casty.supervision import Directive, OneForOneStrategy, SupervisionStrategy
from casty.system import ActorSystem

__all__ = [
    # Core
    "Behavior",
    "Behaviors",
    "ActorContext",
    "ActorRef",
    # Behavior types
    "ReceiveBehavior",
    "SetupBehavior",
    "SameBehavior",
    "StoppedBehavior",
    "UnhandledBehavior",
    "RestartBehavior",
    "LifecycleBehavior",
    "SupervisedBehavior",
    # System
    "ActorSystem",
    # Supervision
    "SupervisionStrategy",
    "OneForOneStrategy",
    "Directive",
    # Mailbox
    "Mailbox",
    "MailboxOverflowStrategy",
    # Events
    "EventStream",
    "ActorStarted",
    "ActorStopped",
    "ActorRestarted",
    "DeadLetter",
    "UnhandledMessage",
    # Messages
    "Terminated",
]
```

**Step 2: Write integration tests**

```python
# tests/test_integration.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    Directive,
    Mailbox,
    MailboxOverflowStrategy,
    OneForOneStrategy,
    Terminated,
)


# --- Messages ---

@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class GetCount:
    reply_to: ActorRef[int]


type GreeterMsg = Greet | GetCount


@dataclass(frozen=True)
class WorkItem:
    data: str


@dataclass(frozen=True)
class GetResults:
    reply_to: ActorRef[list[str]]


type WorkerMsg = WorkItem | GetResults


# --- Behaviors ---

def greeter(count: int = 0) -> Behavior[GreeterMsg]:
    async def receive(ctx: Any, msg: GreeterMsg) -> Any:
        match msg:
            case Greet(name):
                return greeter(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


# --- Integration Tests ---

async def test_full_lifecycle_greeter() -> None:
    """Spawn a greeter, send messages, ask for count, shutdown."""
    async with ActorSystem(name="test") as system:
        ref = system.spawn(greeter(), "greeter")

        ref.tell(Greet("Alice"))
        ref.tell(Greet("Bob"))
        ref.tell(Greet("Charlie"))
        await asyncio.sleep(0.1)

        count = await system.ask(ref, lambda r: GetCount(reply_to=r), timeout=5.0)
        assert count == 3


async def test_parent_child_hierarchy() -> None:
    """Parent spawns children, children process messages independently."""
    child_results: list[str] = []

    def worker() -> Behavior[WorkItem]:
        async def receive(ctx: Any, msg: WorkItem) -> Any:
            child_results.append(msg.data)
            return Behaviors.same()
        return Behaviors.receive(receive)

    @dataclass(frozen=True)
    class SpawnWorker:
        name: str

    @dataclass(frozen=True)
    class DispatchWork:
        worker_name: str
        data: str

    type BossMsg = SpawnWorker | DispatchWork

    def boss() -> Behavior[BossMsg]:
        workers: dict[str, ActorRef[WorkItem]] = {}

        async def setup(ctx: Any) -> Any:
            async def receive(ctx: Any, msg: BossMsg) -> Any:
                match msg:
                    case SpawnWorker(name):
                        workers[name] = ctx.spawn(worker(), name)
                        return Behaviors.same()
                    case DispatchWork(worker_name, data):
                        if worker_name in workers:
                            workers[worker_name].tell(WorkItem(data))
                        return Behaviors.same()
                    case _:
                        return Behaviors.unhandled()
            return Behaviors.receive(receive)
        return Behaviors.setup(setup)

    async with ActorSystem() as system:
        ref = system.spawn(boss(), "boss")
        ref.tell(SpawnWorker("w1"))
        ref.tell(SpawnWorker("w2"))
        await asyncio.sleep(0.1)

        ref.tell(DispatchWork("w1", "task-a"))
        ref.tell(DispatchWork("w2", "task-b"))
        await asyncio.sleep(0.1)

    assert sorted(child_results) == ["task-a", "task-b"]


async def test_supervision_restart_on_failure() -> None:
    """Actor fails, gets restarted by supervision, continues processing."""
    processed: list[str] = []
    fail_once = True

    def fragile_worker() -> Behavior[str]:
        async def receive(ctx: Any, msg: str) -> Any:
            nonlocal fail_once
            if msg == "fail" and fail_once:
                fail_once = False
                raise RuntimeError("boom")
            processed.append(msg)
            return Behaviors.same()
        return Behaviors.receive(receive)

    strategy = OneForOneStrategy(
        max_restarts=3,
        within=60.0,
        decider=lambda exc: Directive.restart,
    )

    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.supervise(fragile_worker(), strategy),
            "worker",
        )
        ref.tell("fail")
        await asyncio.sleep(0.2)
        ref.tell("after-restart")
        await asyncio.sleep(0.1)

    assert "after-restart" in processed


async def test_lifecycle_hooks_called() -> None:
    """Lifecycle hooks fire in correct order."""
    hooks: list[str] = []

    async def pre_start(ctx: Any) -> None:
        hooks.append("pre_start")

    async def post_stop(ctx: Any) -> None:
        hooks.append("post_stop")

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        pre_start=pre_start,
        post_stop=post_stop,
    )

    async with ActorSystem() as system:
        system.spawn(behavior, "hooked")
        await asyncio.sleep(0.1)

    assert hooks == ["pre_start", "post_stop"]


async def test_dead_letters() -> None:
    """Messages to stopped actors go to dead letters."""
    dead: list[DeadLetter] = []

    async with ActorSystem() as system:
        system.event_stream.subscribe(DeadLetter, lambda e: dead.append(e))  # type: ignore[arg-type,return-value]

        behavior = Behaviors.receive(lambda ctx, msg: Behaviors.stopped())
        ref = system.spawn(behavior, "short-lived")
        ref.tell("trigger-stop")
        await asyncio.sleep(0.1)

        ref.tell("after-death")
        await asyncio.sleep(0.1)

    assert len(dead) >= 1
    assert any(d.message == "after-death" for d in dead)


async def test_ask_timeout_raises() -> None:
    """Ask with no reply raises TimeoutError."""
    async def silent(ctx: Any, msg: Any) -> Any:
        return Behaviors.same()

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(silent), "silent")
        with pytest.raises(TimeoutError):
            await system.ask(ref, lambda r: "hello", timeout=0.1)


async def test_bounded_mailbox_integration() -> None:
    """Bounded mailbox drops messages per strategy."""
    received: list[int] = []

    async def collector(ctx: Any, msg: int) -> Any:
        await asyncio.sleep(0.05)  # slow consumer
        received.append(msg)
        return Behaviors.same()

    mailbox: Mailbox[int] = Mailbox(
        capacity=2, overflow=MailboxOverflowStrategy.drop_new,
    )

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(collector), "slow", mailbox=mailbox)
        for i in range(10):
            ref.tell(i)
        await asyncio.sleep(1.0)

    # Not all 10 should arrive due to bounded mailbox
    assert len(received) < 10


async def test_lookup_nested_child() -> None:
    """Lookup finds nested children by path."""

    async def leaf_setup(ctx: Any) -> Any:
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    async def mid_setup(ctx: Any) -> Any:
        ctx.spawn(Behaviors.setup(leaf_setup), "leaf")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    async def root_setup(ctx: Any) -> Any:
        ctx.spawn(Behaviors.setup(mid_setup), "mid")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    async with ActorSystem() as system:
        system.spawn(Behaviors.setup(root_setup), "root")
        await asyncio.sleep(0.2)

        assert system.lookup("/root") is not None
        assert system.lookup("/root/mid") is not None
        assert system.lookup("/root/mid/leaf") is not None
        assert system.lookup("/root/nonexistent") is None
```

**Step 3: Run all tests**

Run: `uv run pytest -v`
Expected: All tests PASS.

**Step 4: Run pyright**

Run: `uv run pyright src/casty/`
Expected: Likely some type errors to fix. Iterate until clean or near-clean. Note: some `Any` usage in tests is acceptable; the library code should be strict.

---

## Parallel Execution Map

```
Phase 1 (Sequential):
  Task 1 → Task 2 → Task 3 → Task 4

Phase 2 (Parallel — 3 agents):
  Task 5 (Mailbox)       ─┐
  Task 6 (EventStream)   ─┼─→ all complete before Phase 3
  Task 7 (Supervision)   ─┘

Phase 3 (Sequential):
  Task 8 → Task 9 → Task 10
```
