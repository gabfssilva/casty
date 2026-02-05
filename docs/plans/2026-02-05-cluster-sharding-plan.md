# Casty Cluster Sharding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Distribute actors across multiple nodes with automatic sharding, phi accrual failure detection, gossip-based cluster membership, and location-transparent messaging.

**Architecture:** Four incremental layers — (1) addressable ActorRef, (2) pluggable remoting with TCP default, (3) gossip-based cluster membership with phi accrual failure detection, (4) cluster sharding with coordinator, regions, and passivation. Everything is an actor. No external runtime dependencies (stdlib only, pluggable interfaces for alternatives).

**Tech Stack:** Python 3.13+, asyncio, stdlib only (json, asyncio.streams, math, statistics, collections, dataclasses)

---

## Implementation Streams (parallelism guide)

```
Stream A (ActorRef) ──→ Stream B (Serialization) ──┐
                                                     ├──→ Stream E (Membership) ──→ Stream F (Sharding)
Stream A (ActorRef) ──→ Stream C (Transport)      ──┘
Stream D (Phi Accrual) ──────────────────────────────┘
```

- **A** must be completed first (foundation)
- **B, C, D** can run in parallel after A
- **E** depends on B + C + D
- **F** depends on E

---

## Task 1: ActorAddress dataclass

**Files:**
- Create: `src/casty/address.py`
- Test: `tests/test_address.py`

**Step 1: Write the failing test**

```python
# tests/test_address.py
from __future__ import annotations

import pytest

from casty.address import ActorAddress


def test_local_address_creation() -> None:
    addr = ActorAddress(system="my-system", path="/greeter")
    assert addr.system == "my-system"
    assert addr.host is None
    assert addr.port is None
    assert addr.path == "/greeter"
    assert addr.is_local


def test_remote_address_creation() -> None:
    addr = ActorAddress(
        system="my-system", host="192.168.1.10", port=25520, path="/greeter"
    )
    assert not addr.is_local
    assert addr.host == "192.168.1.10"
    assert addr.port == 25520


def test_local_address_to_uri() -> None:
    addr = ActorAddress(system="my-system", path="/greeter/worker-1")
    assert addr.to_uri() == "casty://my-system/greeter/worker-1"


def test_remote_address_to_uri() -> None:
    addr = ActorAddress(
        system="my-system",
        host="192.168.1.10",
        port=25520,
        path="/greeter/worker-1",
    )
    assert addr.to_uri() == "casty://my-system@192.168.1.10:25520/greeter/worker-1"


def test_local_address_from_uri() -> None:
    addr = ActorAddress.from_uri("casty://my-system/greeter/worker-1")
    assert addr.system == "my-system"
    assert addr.host is None
    assert addr.port is None
    assert addr.path == "/greeter/worker-1"


def test_remote_address_from_uri() -> None:
    addr = ActorAddress.from_uri(
        "casty://my-system@192.168.1.10:25520/greeter/worker-1"
    )
    assert addr.system == "my-system"
    assert addr.host == "192.168.1.10"
    assert addr.port == 25520
    assert addr.path == "/greeter/worker-1"


def test_address_roundtrip() -> None:
    original = ActorAddress(
        system="sys", host="10.0.0.1", port=9000, path="/a/b/c"
    )
    restored = ActorAddress.from_uri(original.to_uri())
    assert restored == original


def test_local_address_roundtrip() -> None:
    original = ActorAddress(system="sys", path="/a/b")
    restored = ActorAddress.from_uri(original.to_uri())
    assert restored == original


def test_invalid_uri_raises() -> None:
    with pytest.raises(ValueError, match="Invalid"):
        ActorAddress.from_uri("http://not-casty/foo")


def test_address_is_frozen() -> None:
    addr = ActorAddress(system="sys", path="/a")
    with pytest.raises(AttributeError):
        addr.system = "other"  # type: ignore[misc]
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_address.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'casty.address'`

**Step 3: Write minimal implementation**

```python
# src/casty/address.py
from __future__ import annotations

import re
from dataclasses import dataclass

_URI_PATTERN = re.compile(
    r"^casty://(?P<system>[^/@]+)(?:@(?P<host>[^:]+):(?P<port>\d+))?/(?P<path>.+)$"
)


@dataclass(frozen=True)
class ActorAddress:
    system: str
    path: str
    host: str | None = None
    port: int | None = None

    @property
    def is_local(self) -> bool:
        return self.host is None

    def to_uri(self) -> str:
        path_part = self.path.lstrip("/")
        if self.host is not None and self.port is not None:
            return f"casty://{self.system}@{self.host}:{self.port}/{path_part}"
        return f"casty://{self.system}/{path_part}"

    @staticmethod
    def from_uri(uri: str) -> ActorAddress:
        match = _URI_PATTERN.match(uri)
        if match is None:
            msg = f"Invalid Casty URI: {uri}"
            raise ValueError(msg)
        groups = match.groupdict()
        return ActorAddress(
            system=groups["system"],
            host=groups.get("host"),
            port=int(groups["port"]) if groups.get("port") else None,
            path=f"/{groups['path']}",
        )
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_address.py -v`
Expected: All PASS

**Step 5: Run pyright and ruff**

Run: `uv run pyright src/casty/address.py && uv run ruff check src/casty/address.py`
Expected: No errors

**Step 6: Commit**

```bash
git add src/casty/address.py tests/test_address.py
git commit -m "feat: add ActorAddress with URI parsing"
```

---

## Task 2: MessageTransport Protocol and LocalTransport

**Files:**
- Create: `src/casty/transport.py`
- Modify: `src/casty/ref.py`
- Modify: `src/casty/_cell.py:137-138` (ref creation)
- Modify: `src/casty/system.py:64-70` (ask temp_ref)
- Modify: `tests/test_ref.py` (update for new ActorRef signature)
- Test: `tests/test_transport.py`

**Step 1: Write the failing tests**

```python
# tests/test_transport.py
from __future__ import annotations

import asyncio
from typing import Any

from casty.address import ActorAddress
from casty.transport import LocalTransport, MessageTransport


def test_local_transport_implements_protocol() -> None:
    transport = LocalTransport()
    assert isinstance(transport, MessageTransport)


async def test_local_transport_delivers_to_registered_cell() -> None:
    received: list[str] = []
    transport = LocalTransport()

    transport.register("/greeter", received.append)

    addr = ActorAddress(system="test", path="/greeter")
    transport.deliver(addr, "hello")
    assert received == ["hello"]


async def test_local_transport_unregistered_path_is_noop() -> None:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/nonexistent")
    # Should not raise
    transport.deliver(addr, "lost")


async def test_local_transport_unregister() -> None:
    received: list[str] = []
    transport = LocalTransport()
    transport.register("/greeter", received.append)
    transport.unregister("/greeter")

    addr = ActorAddress(system="test", path="/greeter")
    transport.deliver(addr, "lost")
    assert received == []
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_transport.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'casty.transport'`

**Step 3: Write implementation**

```python
# src/casty/transport.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from casty.address import ActorAddress


@runtime_checkable
class MessageTransport(Protocol):
    def deliver(self, address: ActorAddress, msg: Any) -> None: ...


class LocalTransport:
    def __init__(self) -> None:
        self._handlers: dict[str, Callable[[Any], None]] = {}

    def register(self, path: str, handler: Callable[[Any], None]) -> None:
        self._handlers[path] = handler

    def unregister(self, path: str) -> None:
        self._handlers.pop(path, None)

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        handler = self._handlers.get(address.path)
        if handler is not None:
            handler(msg)
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_transport.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/transport.py tests/test_transport.py
git commit -m "feat: add MessageTransport protocol and LocalTransport"
```

---

## Task 3: Refactor ActorRef to use address + transport

This is the critical migration task. ActorRef changes from `Callable` to `ActorAddress + MessageTransport`.

**Files:**
- Modify: `src/casty/ref.py`
- Modify: `src/casty/_cell.py:106-138` (constructor, ref creation, _deliver)
- Modify: `src/casty/system.py:15-19,45-55,64-70` (LocalTransport integration, ask temp ref)
- Modify: `tests/test_ref.py` (new API)
- Modify: All tests that create `ActorRef(_send=...)` directly

**Step 1: Update ActorRef**

Change `src/casty/ref.py` to:

```python
# src/casty/ref.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.address import ActorAddress
from casty.transport import MessageTransport


@dataclass(frozen=True)
class ActorRef[M]:
    address: ActorAddress
    _transport: MessageTransport

    def tell(self, msg: M) -> None:
        self._transport.deliver(self.address, msg)
```

**Step 2: Update ActorCell**

In `src/casty/_cell.py`, change the constructor to accept and use a `LocalTransport`:

- Constructor receives `local_transport: LocalTransport` parameter
- Ref creation (line 138): `self._ref = ActorRef(address=..., _transport=local_transport)`
- The `_deliver` method stays the same but is registered in the transport
- Change: `ActorCell.__init__` builds an `ActorAddress(system=system_name, path=f"/{name}")` and creates the ref with it
- Call `local_transport.register(f"/{name}", self._deliver)`

Key changes in `_cell.py`:

```python
# In __init__, add system_name and local_transport params:
def __init__(
    self,
    behavior: Behavior[M],
    name: str,
    parent: ActorCell[Any] | None,
    event_stream: EventStream,
    system_name: str,
    local_transport: LocalTransport,
) -> None:
    # ... existing init code ...
    self._system_name = system_name
    self._local_transport = local_transport

    addr = ActorAddress(system=system_name, path=f"/{name}")
    self._ref: ActorRef[M] = ActorRef(address=addr, _transport=local_transport)
    local_transport.register(f"/{name}", self._deliver)
```

And in `_CellContext.spawn`:

```python
def spawn[C](self, behavior: Any, name: str, *, mailbox: Mailbox[C] | None = None) -> ActorRef[C]:
    child: ActorCell[C] = ActorCell(
        behavior=behavior,
        name=f"{self._cell._name}/{name}",
        parent=self._cell,
        event_stream=self._cell._event_stream,
        system_name=self._cell._system_name,
        local_transport=self._cell._local_transport,
    )
    # ... rest unchanged
```

**Step 3: Update ActorSystem**

In `src/casty/system.py`:

```python
class ActorSystem:
    def __init__(self, name: str = "casty-system") -> None:
        self._name = name
        self._event_stream = EventStream()
        self._root_cells: dict[str, ActorCell[Any]] = {}
        self._local_transport = LocalTransport()

    # In spawn():
    cell: ActorCell[M] = ActorCell(
        behavior=behavior,
        name=name,
        parent=None,
        event_stream=self._event_stream,
        system_name=self._name,
        local_transport=self._local_transport,
    )

    # In ask(): create temp ref with _CallbackTransport (a tiny helper)
    async def ask[M, R](self, ref: ActorRef[M], msg_factory: Callable[[ActorRef[R]], M], *, timeout: float) -> R:
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()

        def on_reply(msg: R) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = ActorRef(
            address=ActorAddress(system=self._name, path=f"/_temp/{id(future)}"),
            _transport=_CallbackTransport(on_reply),
        )
        message = msg_factory(temp_ref)
        ref.tell(message)
        return await asyncio.wait_for(future, timeout=timeout)
```

Add `_CallbackTransport` to `system.py`:

```python
class _CallbackTransport:
    """Lightweight transport for temp ask refs — delivers via callback."""
    def __init__(self, callback: Callable[[Any], None]) -> None:
        self._callback = callback

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        self._callback(msg)
```

**Step 4: Update test_ref.py**

```python
# tests/test_ref.py
from __future__ import annotations

import pytest

from casty.address import ActorAddress
from casty.transport import LocalTransport
from casty.ref import ActorRef


async def test_tell_delivers_via_transport() -> None:
    received: list[str] = []
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    transport.register("/actor", received.append)

    ref: ActorRef[str] = ActorRef(address=addr, _transport=transport)
    ref.tell("hello")
    assert received == ["hello"]


async def test_tell_preserves_message_order() -> None:
    received: list[int] = []
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    transport.register("/actor", received.append)

    ref: ActorRef[int] = ActorRef(address=addr, _transport=transport)
    ref.tell(1)
    ref.tell(2)
    ref.tell(3)
    assert received == [1, 2, 3]


async def test_ref_is_frozen() -> None:
    transport = LocalTransport()
    addr = ActorAddress(system="test", path="/actor")
    ref: ActorRef[str] = ActorRef(address=addr, _transport=transport)
    with pytest.raises(AttributeError):
        ref.address = addr  # type: ignore[misc]
```

**Step 5: Run ALL existing tests**

Run: `uv run pytest -v`
Expected: All PASS (existing tests updated, no regressions)

**Step 6: Run pyright**

Run: `uv run pyright src/casty/`
Expected: No errors

**Step 7: Commit**

```bash
git add -A
git commit -m "refactor: ActorRef uses address + transport instead of Callable"
```

---

## Task 4: Add resolve() to ActorSystem

**Files:**
- Modify: `src/casty/system.py`
- Test: `tests/test_system.py` (add resolve test)

**Step 1: Write the failing test**

Add to `tests/test_system.py`:

```python
async def test_system_resolve_local_address() -> None:
    async with ActorSystem(name="test-sys") as system:
        ref = system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()), "my-actor"
        )
        addr = ActorAddress(system="test-sys", path="/my-actor")
        resolved = system.resolve(addr)
        assert resolved is not None
        assert resolved.address == ref.address


async def test_system_resolve_unknown_returns_none() -> None:
    async with ActorSystem(name="test-sys") as system:
        addr = ActorAddress(system="test-sys", path="/nonexistent")
        assert system.resolve(addr) is None
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_system.py::test_system_resolve_local_address -v`
Expected: FAIL — `AttributeError: 'ActorSystem' object has no attribute 'resolve'`

**Step 3: Implement resolve()**

Add to `ActorSystem`:

```python
def resolve(self, address: ActorAddress) -> ActorRef[Any] | None:
    """Resolve an ActorAddress to a local ActorRef, or None if not found."""
    if address.is_local or address.host is None:
        return self.lookup(address.path)
    return None  # Remote resolution handled by remoting layer later
```

**Step 4: Run test**

Run: `uv run pytest tests/test_system.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/system.py tests/test_system.py
git commit -m "feat: add ActorSystem.resolve() for address-based lookup"
```

---

## Task 5: Update __init__.py exports

**Files:**
- Modify: `src/casty/__init__.py`

**Step 1: Add new exports**

Add `ActorAddress`, `MessageTransport`, `LocalTransport` to `__init__.py` imports and `__all__`.

**Step 2: Run all tests + pyright**

Run: `uv run pytest -v && uv run pyright src/casty/`
Expected: All PASS

**Step 3: Commit**

```bash
git add src/casty/__init__.py
git commit -m "feat: export ActorAddress and transport types"
```

---

## Task 6: Phi Accrual Failure Detector (independent — can run parallel with Tasks 7-9)

**Files:**
- Create: `src/casty/failure_detector.py`
- Test: `tests/test_failure_detector.py`

**Step 1: Write the failing tests**

```python
# tests/test_failure_detector.py
from __future__ import annotations

import time
from unittest.mock import patch

from casty.failure_detector import PhiAccrualFailureDetector


def test_no_heartbeat_returns_zero_phi() -> None:
    detector = PhiAccrualFailureDetector()
    assert detector.phi("node-1") == 0.0


def test_recent_heartbeat_is_available() -> None:
    detector = PhiAccrualFailureDetector(threshold=8.0)
    detector.heartbeat("node-1")
    assert detector.is_available("node-1")


def test_stale_heartbeat_becomes_unavailable() -> None:
    detector = PhiAccrualFailureDetector(
        threshold=3.0, first_heartbeat_estimate_ms=100.0
    )
    detector.heartbeat("node-1")
    # Simulate time passing: 10 seconds with no heartbeat
    with patch("casty.failure_detector.time") as mock_time:
        mock_time.monotonic.return_value = time.monotonic() + 10.0
        assert not detector.is_available("node-1")


def test_phi_increases_with_elapsed_time() -> None:
    detector = PhiAccrualFailureDetector(first_heartbeat_estimate_ms=1000.0)
    detector.heartbeat("node-1")

    base = time.monotonic()
    with patch("casty.failure_detector.time") as mock_time:
        mock_time.monotonic.return_value = base + 1.0
        phi_1s = detector.phi("node-1")

        mock_time.monotonic.return_value = base + 5.0
        phi_5s = detector.phi("node-1")

    assert phi_5s > phi_1s


def test_regular_heartbeats_stay_available() -> None:
    detector = PhiAccrualFailureDetector(threshold=8.0)

    base = time.monotonic()
    for i in range(10):
        with patch("casty.failure_detector.time") as mock_time:
            mock_time.monotonic.return_value = base + i * 1.0
            detector.heartbeat("node-1")

    with patch("casty.failure_detector.time") as mock_time:
        mock_time.monotonic.return_value = base + 10.5
        assert detector.is_available("node-1")


def test_max_sample_size_respected() -> None:
    detector = PhiAccrualFailureDetector(max_sample_size=5)

    base = time.monotonic()
    for i in range(20):
        with patch("casty.failure_detector.time") as mock_time:
            mock_time.monotonic.return_value = base + i * 1.0
            detector.heartbeat("node-1")

    assert len(detector._history["node-1"]) <= 5


def test_multiple_nodes_independent() -> None:
    detector = PhiAccrualFailureDetector(threshold=3.0)
    detector.heartbeat("node-1")
    detector.heartbeat("node-2")

    assert detector.is_available("node-1")
    assert detector.is_available("node-2")
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_failure_detector.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/casty/failure_detector.py
from __future__ import annotations

import math
import statistics
import time
from collections import deque


class PhiAccrualFailureDetector:
    def __init__(
        self,
        *,
        threshold: float = 8.0,
        max_sample_size: int = 200,
        min_std_deviation_ms: float = 100.0,
        acceptable_heartbeat_pause_ms: float = 0.0,
        first_heartbeat_estimate_ms: float = 1000.0,
    ) -> None:
        self._threshold = threshold
        self._max_sample_size = max_sample_size
        self._min_std_deviation_ms = min_std_deviation_ms
        self._acceptable_heartbeat_pause_ms = acceptable_heartbeat_pause_ms
        self._first_heartbeat_estimate_ms = first_heartbeat_estimate_ms
        self._last_heartbeat: dict[str, float] = {}
        self._history: dict[str, deque[float]] = {}

    def heartbeat(self, node: str) -> None:
        now = time.monotonic()
        if node in self._last_heartbeat:
            interval_ms = (now - self._last_heartbeat[node]) * 1000.0
            if node not in self._history:
                self._history[node] = deque(maxlen=self._max_sample_size)
            self._history[node].append(interval_ms)
        self._last_heartbeat[node] = now

    def phi(self, node: str) -> float:
        if node not in self._last_heartbeat:
            return 0.0

        elapsed_ms = (time.monotonic() - self._last_heartbeat[node]) * 1000.0
        history = self._history.get(node)

        if not history or len(history) < 2:
            mean = self._first_heartbeat_estimate_ms
            std = mean / 4.0
        else:
            mean = statistics.mean(history)
            std = max(statistics.stdev(history), self._min_std_deviation_ms)

        mean += self._acceptable_heartbeat_pause_ms

        y = (elapsed_ms - mean) / std
        p = 0.5 * (1.0 + math.erf(y / math.sqrt(2.0)))

        if p >= 1.0:
            return float("inf")
        if p <= 0.0:
            return 0.0

        return -math.log10(1.0 - p)

    def is_available(self, node: str) -> bool:
        return self.phi(node) < self._threshold
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_failure_detector.py -v`
Expected: All PASS

**Step 5: Run pyright**

Run: `uv run pyright src/casty/failure_detector.py`
Expected: No errors

**Step 6: Commit**

```bash
git add src/casty/failure_detector.py tests/test_failure_detector.py
git commit -m "feat: add PhiAccrualFailureDetector"
```

---

## Task 7: Serialization — TypeRegistry and Serializer Protocol (can run parallel with Task 6, 8)

**Files:**
- Create: `src/casty/serialization.py`
- Test: `tests/test_serialization.py`

**Step 1: Write the failing tests**

```python
# tests/test_serialization.py
from __future__ import annotations

import pytest
from dataclasses import dataclass

from casty.serialization import TypeRegistry, JsonSerializer, Serializer
from casty.address import ActorAddress


@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class Count:
    value: int


def test_type_registry_register_and_resolve() -> None:
    registry = TypeRegistry()
    registry.register(Greet)
    resolved = registry.resolve(registry.type_name(Greet))
    assert resolved is Greet


def test_type_registry_unknown_raises() -> None:
    registry = TypeRegistry()
    with pytest.raises(KeyError):
        registry.resolve("nonexistent.Type")


def test_json_serializer_roundtrip_simple() -> None:
    registry = TypeRegistry()
    registry.register(Greet)
    serializer = JsonSerializer(registry)

    original = Greet(name="Alice")
    data = serializer.serialize(original)
    restored = serializer.deserialize(data)
    assert restored == original


def test_json_serializer_roundtrip_int_field() -> None:
    registry = TypeRegistry()
    registry.register(Count)
    serializer = JsonSerializer(registry)

    original = Count(value=42)
    data = serializer.serialize(original)
    restored = serializer.deserialize(data)
    assert restored == original


def test_json_serializer_implements_protocol() -> None:
    registry = TypeRegistry()
    serializer = JsonSerializer(registry)
    assert isinstance(serializer, Serializer)


def test_json_serializer_address_roundtrip() -> None:
    """ActorAddress should serialize/deserialize correctly."""
    registry = TypeRegistry()
    registry.register(ActorAddress)
    serializer = JsonSerializer(registry)

    addr = ActorAddress(
        system="sys", host="10.0.0.1", port=9000, path="/a/b"
    )
    data = serializer.serialize(addr)
    restored = serializer.deserialize(data)
    assert restored == addr
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_serialization.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/casty/serialization.py
from __future__ import annotations

import dataclasses
import json
from typing import Any, Protocol, runtime_checkable


class TypeRegistry:
    def __init__(self) -> None:
        self._name_to_type: dict[str, type] = {}
        self._type_to_name: dict[type, str] = {}

    def register(self, cls: type) -> None:
        name = f"{cls.__module__}.{cls.__qualname__}"
        self._name_to_type[name] = cls
        self._type_to_name[cls] = name

    def resolve(self, name: str) -> type:
        if name not in self._name_to_type:
            msg = f"Unknown type: {name}"
            raise KeyError(msg)
        return self._name_to_type[name]

    def type_name(self, cls: type) -> str:
        return self._type_to_name[cls]


@runtime_checkable
class Serializer(Protocol):
    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes) -> Any: ...


class JsonSerializer:
    def __init__(self, registry: TypeRegistry) -> None:
        self._registry = registry

    def serialize(self, obj: Any) -> bytes:
        if not dataclasses.is_dataclass(obj) or isinstance(obj, type):
            msg = f"Can only serialize dataclass instances, got {type(obj)}"
            raise TypeError(msg)
        type_name = self._registry.type_name(type(obj))
        payload = dataclasses.asdict(obj)
        envelope = {"_type": type_name, **payload}
        return json.dumps(envelope).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        envelope = json.loads(data.decode("utf-8"))
        type_name = envelope.pop("_type")
        cls = self._registry.resolve(type_name)
        return cls(**envelope)
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_serialization.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/serialization.py tests/test_serialization.py
git commit -m "feat: add TypeRegistry and JsonSerializer"
```

---

## Task 8: TCP Transport (can run parallel with Tasks 6, 7)

**Files:**
- Create: `src/casty/remote_transport.py`
- Test: `tests/test_remote_transport.py`

**Step 1: Write the failing tests**

```python
# tests/test_remote_transport.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty.remote_transport import (
    MessageEnvelope,
    TcpTransport,
    InboundHandler,
)


@dataclass(frozen=True)
class _TestHandler:
    received: list[bytes]

    async def on_message(self, data: bytes) -> None:
        self.received.append(data)


async def test_envelope_roundtrip() -> None:
    envelope = MessageEnvelope(
        target="casty://sys@localhost:25520/actor",
        sender="casty://sys@localhost:25521/sender",
        payload=b'{"_type":"Greet","name":"Alice"}',
        type_hint="test.Greet",
    )
    data = envelope.to_bytes()
    restored = MessageEnvelope.from_bytes(data)
    assert restored == envelope


async def test_tcp_transport_send_receive() -> None:
    """Two transports on localhost can exchange messages."""
    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    handler_a = Handler()
    handler_b = Handler()

    await transport_a.start(handler_a)
    await transport_b.start(handler_b)

    try:
        # Send from A to B
        envelope = MessageEnvelope(
            target=f"casty://sys@127.0.0.1:{transport_b.port}/actor",
            sender=f"casty://sys@127.0.0.1:{transport_a.port}/sender",
            payload=b"hello",
            type_hint="str",
        )
        await transport_a.send("127.0.0.1", transport_b.port, envelope.to_bytes())
        await asyncio.sleep(0.2)

        assert len(received) == 1
        restored = MessageEnvelope.from_bytes(received[0])
        assert restored.payload == b"hello"
    finally:
        await transport_a.stop()
        await transport_b.stop()


async def test_tcp_transport_connection_reuse() -> None:
    """Multiple sends reuse the same connection."""
    received: list[bytes] = []

    class Handler:
        async def on_message(self, data: bytes) -> None:
            received.append(data)

    transport_a = TcpTransport(host="127.0.0.1", port=0)
    transport_b = TcpTransport(host="127.0.0.1", port=0)

    await transport_a.start(Handler())
    await transport_b.start(Handler())

    try:
        for i in range(5):
            envelope = MessageEnvelope(
                target=f"casty://sys@127.0.0.1:{transport_b.port}/actor",
                sender=f"casty://sys@127.0.0.1:{transport_a.port}/sender",
                payload=f"msg-{i}".encode(),
                type_hint="str",
            )
            await transport_a.send("127.0.0.1", transport_b.port, envelope.to_bytes())

        await asyncio.sleep(0.3)
        assert len(received) == 5
    finally:
        await transport_a.stop()
        await transport_b.stop()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_remote_transport.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/casty/remote_transport.py
from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass
from typing import Protocol


class InboundHandler(Protocol):
    async def on_message(self, data: bytes) -> None: ...


@dataclass(frozen=True)
class MessageEnvelope:
    target: str
    sender: str
    payload: bytes
    type_hint: str

    def to_bytes(self) -> bytes:
        header = json.dumps({
            "target": self.target,
            "sender": self.sender,
            "type_hint": self.type_hint,
        }).encode("utf-8")
        # Format: [header_len:4][header][payload]
        return struct.pack("!I", len(header)) + header + self.payload

    @staticmethod
    def from_bytes(data: bytes) -> MessageEnvelope:
        header_len = struct.unpack("!I", data[:4])[0]
        header_bytes = data[4 : 4 + header_len]
        payload = data[4 + header_len :]
        header = json.loads(header_bytes.decode("utf-8"))
        return MessageEnvelope(
            target=header["target"],
            sender=header["sender"],
            payload=payload,
            type_hint=header["type_hint"],
        )


class TcpTransport:
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None
        self._handler: InboundHandler | None = None
        self._connections: dict[tuple[str, int], tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}

    @property
    def port(self) -> int:
        """Actual port (useful when started with port=0)."""
        if self._server is not None:
            sockets = self._server.sockets
            if sockets:
                return sockets[0].getsockname()[1]
        return self._port

    async def start(self, handler: InboundHandler) -> None:
        self._handler = handler
        self._server = await asyncio.start_server(
            self._handle_connection, self._host, self._port
        )

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                # Read message length (4 bytes)
                length_bytes = await reader.readexactly(4)
                msg_len = struct.unpack("!I", length_bytes)[0]
                data = await reader.readexactly(msg_len)
                if self._handler is not None:
                    await self._handler.on_message(data)
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            pass
        finally:
            writer.close()

    async def send(self, host: str, port: int, data: bytes) -> None:
        key = (host, port)
        if key not in self._connections:
            reader, writer = await asyncio.open_connection(host, port)
            self._connections[key] = (reader, writer)

        _, writer = self._connections[key]
        # Send: [msg_len:4][data]
        writer.write(struct.pack("!I", len(data)) + data)
        await writer.drain()

    async def stop(self) -> None:
        for _, writer in self._connections.values():
            writer.close()
        self._connections.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_remote_transport.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/remote_transport.py tests/test_remote_transport.py
git commit -m "feat: add TcpTransport and MessageEnvelope for remoting"
```

---

## Task 9: VectorClock and ClusterState data structures

**Files:**
- Create: `src/casty/cluster_state.py`
- Test: `tests/test_cluster_state.py`

**Step 1: Write the failing tests**

```python
# tests/test_cluster_state.py
from __future__ import annotations

from casty.cluster_state import (
    VectorClock,
    NodeAddress,
    Member,
    MemberStatus,
    ClusterState,
)


def test_vector_clock_increment() -> None:
    vc = VectorClock()
    node = NodeAddress(host="10.0.0.1", port=25520)
    vc2 = vc.increment(node)
    assert vc2.version_of(node) == 1
    vc3 = vc2.increment(node)
    assert vc3.version_of(node) == 2


def test_vector_clock_merge() -> None:
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    vc_a = VectorClock().increment(node_a).increment(node_a)  # A=2
    vc_b = VectorClock().increment(node_b)  # B=1

    merged = vc_a.merge(vc_b)
    assert merged.version_of(node_a) == 2
    assert merged.version_of(node_b) == 1


def test_vector_clock_ordering() -> None:
    node = NodeAddress(host="10.0.0.1", port=25520)
    vc1 = VectorClock().increment(node)
    vc2 = vc1.increment(node)

    assert vc1.is_before(vc2)
    assert not vc2.is_before(vc1)
    assert not vc1.is_concurrent_with(vc2)


def test_vector_clock_concurrent() -> None:
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    vc_a = VectorClock().increment(node_a)
    vc_b = VectorClock().increment(node_b)

    assert vc_a.is_concurrent_with(vc_b)


def test_member_creation() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.up, roles=frozenset())
    assert member.status == MemberStatus.up


def test_cluster_state_add_member() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.joining, roles=frozenset())
    state = ClusterState()
    new_state = state.add_member(member)
    assert member in new_state.members


def test_cluster_state_update_member_status() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.joining, roles=frozenset())
    state = ClusterState().add_member(member)
    new_state = state.update_status(addr, MemberStatus.up)
    updated = next(m for m in new_state.members if m.address == addr)
    assert updated.status == MemberStatus.up


def test_cluster_state_mark_unreachable() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.up, roles=frozenset())
    state = ClusterState().add_member(member)
    new_state = state.mark_unreachable(addr)
    assert addr in new_state.unreachable


def test_cluster_state_leader_is_lowest_up_address() -> None:
    addr_a = NodeAddress(host="10.0.0.1", port=25520)
    addr_b = NodeAddress(host="10.0.0.2", port=25520)
    state = (
        ClusterState()
        .add_member(Member(address=addr_a, status=MemberStatus.up, roles=frozenset()))
        .add_member(Member(address=addr_b, status=MemberStatus.up, roles=frozenset()))
    )
    assert state.leader == addr_a


def test_node_address_ordering() -> None:
    a = NodeAddress(host="10.0.0.1", port=25520)
    b = NodeAddress(host="10.0.0.2", port=25520)
    assert a < b
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cluster_state.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/casty/cluster_state.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from functools import total_ordering


class MemberStatus(Enum):
    joining = auto()
    up = auto()
    leaving = auto()
    down = auto()
    removed = auto()


@total_ordering
@dataclass(frozen=True)
class NodeAddress:
    host: str
    port: int

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, NodeAddress):
            return NotImplemented
        return (self.host, self.port) < (other.host, other.port)


@dataclass(frozen=True)
class Member:
    address: NodeAddress
    status: MemberStatus
    roles: frozenset[str]


@dataclass(frozen=True)
class VectorClock:
    _versions: dict[NodeAddress, int] = field(default_factory=dict)

    def version_of(self, node: NodeAddress) -> int:
        return self._versions.get(node, 0)

    def increment(self, node: NodeAddress) -> VectorClock:
        new_versions = dict(self._versions)
        new_versions[node] = new_versions.get(node, 0) + 1
        return VectorClock(_versions=new_versions)

    def merge(self, other: VectorClock) -> VectorClock:
        all_nodes = set(self._versions) | set(other._versions)
        merged = {
            node: max(self.version_of(node), other.version_of(node))
            for node in all_nodes
        }
        return VectorClock(_versions=merged)

    def is_before(self, other: VectorClock) -> bool:
        all_nodes = set(self._versions) | set(other._versions)
        at_least_one_less = False
        for node in all_nodes:
            mine = self.version_of(node)
            theirs = other.version_of(node)
            if mine > theirs:
                return False
            if mine < theirs:
                at_least_one_less = True
        return at_least_one_less

    def is_concurrent_with(self, other: VectorClock) -> bool:
        return not self.is_before(other) and not other.is_before(self) and self._versions != other._versions


@dataclass(frozen=True)
class ClusterState:
    members: frozenset[Member] = field(default_factory=frozenset)
    unreachable: frozenset[NodeAddress] = field(default_factory=frozenset)
    version: VectorClock = field(default_factory=VectorClock)

    def add_member(self, member: Member) -> ClusterState:
        return ClusterState(
            members=self.members | {member},
            unreachable=self.unreachable,
            version=self.version,
        )

    def update_status(self, address: NodeAddress, status: MemberStatus) -> ClusterState:
        new_members = frozenset(
            Member(address=m.address, status=status, roles=m.roles)
            if m.address == address
            else m
            for m in self.members
        )
        return ClusterState(
            members=new_members,
            unreachable=self.unreachable,
            version=self.version,
        )

    def mark_unreachable(self, address: NodeAddress) -> ClusterState:
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable | {address},
            version=self.version,
        )

    def mark_reachable(self, address: NodeAddress) -> ClusterState:
        return ClusterState(
            members=self.members,
            unreachable=self.unreachable - {address},
            version=self.version,
        )

    @property
    def leader(self) -> NodeAddress | None:
        up_members = sorted(
            (m.address for m in self.members if m.status == MemberStatus.up)
        )
        return up_members[0] if up_members else None
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cluster_state.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/cluster_state.py tests/test_cluster_state.py
git commit -m "feat: add VectorClock, NodeAddress, Member, and ClusterState"
```

---

## Task 10: Cluster Events

**Files:**
- Modify: `src/casty/events.py` (add cluster event types)
- Test: `tests/test_events.py` (add cluster event tests)

**Step 1: Add cluster event types to `src/casty/events.py`**

```python
# Add after existing event types in events.py:

@dataclass(frozen=True)
class MemberUp:
    member: Member

@dataclass(frozen=True)
class MemberLeft:
    member: Member

@dataclass(frozen=True)
class UnreachableMember:
    member: Member

@dataclass(frozen=True)
class ReachableMember:
    member: Member
```

Note: Import `Member` from `casty.cluster_state` (use `TYPE_CHECKING` if needed to avoid circular imports).

**Step 2: Run tests**

Run: `uv run pytest tests/test_events.py -v && uv run pyright src/casty/events.py`
Expected: All PASS

**Step 3: Commit**

```bash
git add src/casty/events.py tests/test_events.py
git commit -m "feat: add cluster membership events"
```

---

## Task 11: HeartbeatActor

**Files:**
- Create: `src/casty/_heartbeat_actor.py`
- Test: `tests/test_heartbeat_actor.py`

This actor:
- Periodically sends heartbeat messages to all known members
- Receives heartbeat responses and feeds them to the failure detector
- Reports unreachable nodes

**Step 1: Write failing tests**

```python
# tests/test_heartbeat_actor.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef, Behavior
from casty.cluster_state import NodeAddress
from casty.failure_detector import PhiAccrualFailureDetector
from casty._heartbeat_actor import (
    heartbeat_actor,
    HeartbeatTick,
    HeartbeatRequest,
    HeartbeatResponse,
    CheckAvailability,
    NodeUnreachable,
)


async def test_heartbeat_responds_to_request() -> None:
    """HeartbeatActor responds to HeartbeatRequest with HeartbeatResponse."""
    responses: list[HeartbeatResponse] = []

    async def collector_handler(ctx: Any, msg: HeartbeatResponse) -> Any:
        responses.append(msg)
        return Behaviors.same()

    async with ActorSystem(name="test") as system:
        hb_ref = system.spawn(
            heartbeat_actor(
                self_node=NodeAddress(host="127.0.0.1", port=25520),
                detector=PhiAccrualFailureDetector(),
            ),
            "heartbeat",
        )
        collector_ref = system.spawn(Behaviors.receive(collector_handler), "collector")
        await asyncio.sleep(0.1)

        hb_ref.tell(HeartbeatRequest(
            from_node=NodeAddress(host="127.0.0.2", port=25520),
            reply_to=collector_ref,
        ))
        await asyncio.sleep(0.1)

    assert len(responses) == 1
    assert responses[0].from_node == NodeAddress(host="127.0.0.1", port=25520)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_heartbeat_actor.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Implement**

```python
# src/casty/_heartbeat_actor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef


@dataclass(frozen=True)
class HeartbeatTick:
    """Periodic trigger to send heartbeats."""
    members: frozenset[NodeAddress]


@dataclass(frozen=True)
class HeartbeatRequest:
    from_node: NodeAddress
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class HeartbeatResponse:
    from_node: NodeAddress


@dataclass(frozen=True)
class CheckAvailability:
    """Request to check which nodes are unreachable."""
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class NodeUnreachable:
    node: NodeAddress


type HeartbeatMsg = HeartbeatTick | HeartbeatRequest | HeartbeatResponse | CheckAvailability


def heartbeat_actor(
    *,
    self_node: NodeAddress,
    detector: PhiAccrualFailureDetector,
) -> Behavior[HeartbeatMsg]:
    async def receive(ctx: Any, msg: HeartbeatMsg) -> Any:
        match msg:
            case HeartbeatRequest(from_node, reply_to):
                reply_to.tell(HeartbeatResponse(from_node=self_node))
                return Behaviors.same()

            case HeartbeatResponse(from_node):
                detector.heartbeat(str(from_node))
                return Behaviors.same()

            case CheckAvailability(reply_to):
                for node_key in list(detector._last_heartbeat.keys()):
                    if not detector.is_available(node_key):
                        # Parse back to NodeAddress
                        reply_to.tell(NodeUnreachable(
                            node=NodeAddress(
                                host=node_key.split(":")[0].split("(")[1].strip("'"),
                                port=int(node_key.split("port=")[1].rstrip(")")),
                            )
                        ))
                return Behaviors.same()

            case HeartbeatTick():
                # Tick handling is done by the cluster layer
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)
```

Note: The `CheckAvailability` node parsing is ugly — we'll clean this up when integrating with the gossip actor. The key point is that the detector uses string keys internally but we bridge to `NodeAddress`. A simpler approach: use `f"{node.host}:{node.port}"` as the detector key.

**Step 4: Run tests**

Run: `uv run pytest tests/test_heartbeat_actor.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/_heartbeat_actor.py tests/test_heartbeat_actor.py
git commit -m "feat: add HeartbeatActor with phi accrual integration"
```

---

## Task 12: GossipActor

**Files:**
- Create: `src/casty/_gossip_actor.py`
- Test: `tests/test_gossip_actor.py`

**Step 1: Write failing tests**

```python
# tests/test_gossip_actor.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    VectorClock,
)
from casty._gossip_actor import (
    gossip_actor,
    GossipMessage,
    GetClusterState,
    JoinRequest,
    JoinAccepted,
)


async def test_gossip_actor_join_adds_member() -> None:
    """JoinRequest adds a new member in Joining state."""
    states: list[ClusterState] = []

    self_node = NodeAddress(host="127.0.0.1", port=25520)
    initial_state = ClusterState().add_member(
        Member(address=self_node, status=MemberStatus.up, roles=frozenset())
    )

    async with ActorSystem(name="test") as system:
        gossip_ref = system.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        new_node = NodeAddress(host="127.0.0.2", port=25520)
        gossip_ref.tell(JoinRequest(node=new_node, roles=frozenset()))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=2.0
        )
        states.append(state)

    assert len(states) == 1
    addresses = {m.address for m in states[0].members}
    assert new_node in addresses


async def test_gossip_merge_updates_state() -> None:
    """Receiving a GossipMessage with newer state updates local state."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    other_node = NodeAddress(host="127.0.0.2", port=25520)

    initial_state = ClusterState().add_member(
        Member(address=self_node, status=MemberStatus.up, roles=frozenset())
    )

    remote_state = (
        ClusterState()
        .add_member(Member(address=self_node, status=MemberStatus.up, roles=frozenset()))
        .add_member(Member(address=other_node, status=MemberStatus.up, roles=frozenset()))
    )
    remote_state = ClusterState(
        members=remote_state.members,
        unreachable=remote_state.unreachable,
        version=VectorClock().increment(other_node),
    )

    async with ActorSystem(name="test") as system:
        gossip_ref = system.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        gossip_ref.tell(GossipMessage(state=remote_state, from_node=other_node))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=2.0
        )

    addresses = {m.address for m in state.members}
    assert other_node in addresses
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_gossip_actor.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Implement**

```python
# src/casty/_gossip_actor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
)
from casty.ref import ActorRef


@dataclass(frozen=True)
class GossipMessage:
    state: ClusterState
    from_node: NodeAddress


@dataclass(frozen=True)
class GetClusterState:
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class JoinRequest:
    node: NodeAddress
    roles: frozenset[str]


@dataclass(frozen=True)
class JoinAccepted:
    state: ClusterState


type GossipMsg = GossipMessage | GetClusterState | JoinRequest


def gossip_actor(
    *,
    self_node: NodeAddress,
    initial_state: ClusterState,
) -> Behavior[GossipMsg]:
    def active(state: ClusterState) -> Behavior[GossipMsg]:
        async def receive(ctx: Any, msg: GossipMsg) -> Any:
            match msg:
                case GossipMessage(remote_state, from_node):
                    merged_version = state.version.merge(remote_state.version)
                    # Merge members: union of both, prefer higher status
                    all_addresses = {m.address for m in state.members} | {
                        m.address for m in remote_state.members
                    }
                    merged_members: set[Member] = set()
                    for addr in all_addresses:
                        local_m = next(
                            (m for m in state.members if m.address == addr), None
                        )
                        remote_m = next(
                            (m for m in remote_state.members if m.address == addr),
                            None,
                        )
                        if local_m and remote_m:
                            # Keep the one from the newer clock
                            if state.version.is_before(remote_state.version):
                                merged_members.add(remote_m)
                            else:
                                merged_members.add(local_m)
                        elif local_m:
                            merged_members.add(local_m)
                        elif remote_m:
                            merged_members.add(remote_m)

                    new_state = ClusterState(
                        members=frozenset(merged_members),
                        unreachable=state.unreachable | remote_state.unreachable,
                        version=merged_version.increment(self_node),
                    )
                    return active(new_state)

                case GetClusterState(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()

                case JoinRequest(node, roles):
                    new_member = Member(
                        address=node,
                        status=MemberStatus.joining,
                        roles=roles,
                    )
                    new_state = ClusterState(
                        members=state.members | {new_member},
                        unreachable=state.unreachable,
                        version=state.version.increment(self_node),
                    )
                    return active(new_state)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(initial_state)
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_gossip_actor.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/_gossip_actor.py tests/test_gossip_actor.py
git commit -m "feat: add GossipActor with state merge and join protocol"
```

---

## Task 13: Cluster entry point

**Files:**
- Create: `src/casty/cluster.py`
- Test: `tests/test_cluster.py`

The `Cluster` class ties membership, gossip, heartbeat, and transport together. It's the user-facing API for cluster features.

**Step 1: Write failing tests**

```python
# tests/test_cluster.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef
from casty.cluster import Cluster, ClusterConfig
from casty.cluster_state import MemberStatus


async def test_cluster_single_node_becomes_leader() -> None:
    """A single-node cluster should mark itself as Up and become leader."""
    config = ClusterConfig(host="127.0.0.1", port=0, seed_nodes=[])

    async with ActorSystem(name="test") as system:
        cluster = Cluster(system=system, config=config)
        await cluster.start()
        await asyncio.sleep(0.3)

        state = await cluster.get_state(timeout=2.0)
        assert len(state.members) == 1
        member = next(iter(state.members))
        assert member.status == MemberStatus.up
        assert state.leader is not None

        await cluster.shutdown()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cluster.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Implement**

```python
# src/casty/cluster.py
from __future__ import annotations

from dataclasses import dataclass, field

from casty.system import ActorSystem
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
)
from casty._gossip_actor import gossip_actor, GetClusterState, GossipMsg
from casty.ref import ActorRef


@dataclass(frozen=True)
class ClusterConfig:
    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    roles: frozenset[str] = field(default_factory=frozenset)


class Cluster:
    def __init__(self, system: ActorSystem, config: ClusterConfig) -> None:
        self._system = system
        self._config = config
        self._gossip_ref: ActorRef[GossipMsg] | None = None
        self._self_node = NodeAddress(host=config.host, port=config.port)

    async def start(self) -> None:
        initial_member = Member(
            address=self._self_node,
            status=MemberStatus.up,
            roles=self._config.roles,
        )
        initial_state = ClusterState().add_member(initial_member)

        self._gossip_ref = self._system.spawn(
            gossip_actor(
                self_node=self._self_node,
                initial_state=initial_state,
            ),
            "_cluster_gossip",
        )

    async def get_state(self, *, timeout: float = 5.0) -> ClusterState:
        if self._gossip_ref is None:
            msg = "Cluster not started"
            raise RuntimeError(msg)
        return await self._system.ask(
            self._gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=timeout,
        )

    async def shutdown(self) -> None:
        pass  # Cleanup will be added as features grow
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_cluster.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/cluster.py tests/test_cluster.py
git commit -m "feat: add Cluster entry point with single-node bootstrap"
```

---

## Task 14: ShardAllocationStrategy and ShardCoordinatorActor

**Files:**
- Create: `src/casty/_shard_coordinator_actor.py`
- Test: `tests/test_shard_coordinator.py`

**Step 1: Write failing tests**

```python
# tests/test_shard_coordinator.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef
from casty.cluster_state import NodeAddress
from casty._shard_coordinator_actor import (
    shard_coordinator_actor,
    GetShardLocation,
    ShardLocation,
    UpdateTopology,
    LeastShardStrategy,
)


async def test_coordinator_allocates_shard_to_node() -> None:
    """Requesting a shard location allocates it to the available node."""
    node_a = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        await asyncio.sleep(0.1)

        location: ShardLocation = await system.ask(
            coord_ref,
            lambda r: GetShardLocation(shard_id=42, reply_to=r),
            timeout=2.0,
        )
        assert location.shard_id == 42
        assert location.node == node_a


async def test_coordinator_distributes_evenly() -> None:
    """LeastShardStrategy distributes shards evenly across nodes."""
    node_a = NodeAddress(host="127.0.0.1", port=25520)
    node_b = NodeAddress(host="127.0.0.2", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
            ),
            "coordinator",
        )
        await asyncio.sleep(0.1)

        locations: list[ShardLocation] = []
        for shard_id in range(10):
            loc = await system.ask(
                coord_ref,
                lambda r, sid=shard_id: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )
            locations.append(loc)

        nodes_used = {loc.node for loc in locations}
        assert len(nodes_used) == 2  # Both nodes got shards

        counts = {node_a: 0, node_b: 0}
        for loc in locations:
            counts[loc.node] += 1
        assert abs(counts[node_a] - counts[node_b]) <= 1  # Balanced


async def test_coordinator_topology_update() -> None:
    """Adding a node via UpdateTopology makes it available for allocation."""
    node_a = NodeAddress(host="127.0.0.1", port=25520)
    node_b = NodeAddress(host="127.0.0.2", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        await asyncio.sleep(0.1)

        # Allocate some shards to node_a
        for shard_id in range(5):
            await system.ask(
                coord_ref,
                lambda r, sid=shard_id: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )

        # Add node_b
        coord_ref.tell(UpdateTopology(available_nodes=frozenset({node_a, node_b})))
        await asyncio.sleep(0.1)

        # New shard should go to node_b (fewer shards)
        loc = await system.ask(
            coord_ref,
            lambda r: GetShardLocation(shard_id=99, reply_to=r),
            timeout=2.0,
        )
        assert loc.node == node_b
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_shard_coordinator.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Implement**

```python
# src/casty/_shard_coordinator_actor.py
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Protocol

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef


@dataclass(frozen=True)
class GetShardLocation:
    shard_id: int
    reply_to: ActorRef[ShardLocation]


@dataclass(frozen=True)
class ShardLocation:
    shard_id: int
    node: NodeAddress


@dataclass(frozen=True)
class UpdateTopology:
    available_nodes: frozenset[NodeAddress]


type CoordinatorMsg = GetShardLocation | UpdateTopology


class ShardAllocationStrategy(Protocol):
    def allocate(
        self,
        shard_id: int,
        current_allocations: dict[int, NodeAddress],
        available_nodes: frozenset[NodeAddress],
    ) -> NodeAddress: ...


class LeastShardStrategy:
    def allocate(
        self,
        shard_id: int,
        current_allocations: dict[int, NodeAddress],
        available_nodes: frozenset[NodeAddress],
    ) -> NodeAddress:
        counts = Counter(current_allocations.values())
        # Include nodes with 0 shards
        for node in available_nodes:
            if node not in counts:
                counts[node] = 0
        return min(available_nodes, key=lambda n: counts.get(n, 0))


def shard_coordinator_actor(
    *,
    strategy: ShardAllocationStrategy,
    available_nodes: frozenset[NodeAddress],
) -> Behavior[CoordinatorMsg]:
    def active(
        allocations: dict[int, NodeAddress],
        nodes: frozenset[NodeAddress],
    ) -> Behavior[CoordinatorMsg]:
        async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
            match msg:
                case GetShardLocation(shard_id, reply_to):
                    if shard_id in allocations:
                        reply_to.tell(ShardLocation(
                            shard_id=shard_id, node=allocations[shard_id]
                        ))
                        return Behaviors.same()

                    node = strategy.allocate(shard_id, allocations, nodes)
                    new_allocations = {**allocations, shard_id: node}
                    reply_to.tell(ShardLocation(shard_id=shard_id, node=node))
                    return active(new_allocations, nodes)

                case UpdateTopology(new_nodes):
                    return active(allocations, new_nodes)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({}, available_nodes)
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_shard_coordinator.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/_shard_coordinator_actor.py tests/test_shard_coordinator.py
git commit -m "feat: add ShardCoordinatorActor with LeastShardStrategy"
```

---

## Task 15: ShardRegionActor

**Files:**
- Create: `src/casty/sharding.py` (user-facing API: `ShardEnvelope`, `init_sharding`)
- Create: `src/casty/_shard_region_actor.py`
- Test: `tests/test_shard_region.py`

**Step 1: Write failing tests**

```python
# tests/test_shard_region.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef, Behavior
from casty.sharding import ShardEnvelope
from casty._shard_region_actor import shard_region_actor
from casty._shard_coordinator_actor import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty.cluster_state import NodeAddress


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[str]


type EntityMsg = str | GetValue


def echo_entity(entity_id: str) -> Behavior[EntityMsg]:
    received: list[str] = []

    async def receive(ctx: Any, msg: EntityMsg) -> Any:
        match msg:
            case GetValue(reply_to):
                reply_to.tell(f"{entity_id}:{len(received)}")
                return Behaviors.same()
            case str() as text:
                received.append(text)
                return Behaviors.same()
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


async def test_shard_region_routes_to_entity() -> None:
    """ShardRegion creates entity and delivers messages."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=echo_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        region_ref.tell(ShardEnvelope(entity_id="user-1", message="hello"))
        region_ref.tell(ShardEnvelope(entity_id="user-1", message="world"))
        await asyncio.sleep(0.3)

        result = await system.ask(
            region_ref,
            lambda r: ShardEnvelope(entity_id="user-1", message=GetValue(reply_to=r)),
            timeout=2.0,
        )
        assert result == "user-1:2"


async def test_shard_region_multiple_entities() -> None:
    """Different entity IDs create separate actors."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=echo_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        region_ref.tell(ShardEnvelope(entity_id="a", message="msg1"))
        region_ref.tell(ShardEnvelope(entity_id="b", message="msg2"))
        region_ref.tell(ShardEnvelope(entity_id="b", message="msg3"))
        await asyncio.sleep(0.3)

        result_a = await system.ask(
            region_ref,
            lambda r: ShardEnvelope(entity_id="a", message=GetValue(reply_to=r)),
            timeout=2.0,
        )
        result_b = await system.ask(
            region_ref,
            lambda r: ShardEnvelope(entity_id="b", message=GetValue(reply_to=r)),
            timeout=2.0,
        )
        assert result_a == "a:1"
        assert result_b == "b:2"
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_shard_region.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Implement**

```python
# src/casty/sharding.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShardEnvelope[M]:
    entity_id: str
    message: M
```

```python
# src/casty/_shard_region_actor.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef
from casty.sharding import ShardEnvelope
from casty._shard_coordinator_actor import GetShardLocation, ShardLocation, CoordinatorMsg


def shard_region_actor(
    *,
    self_node: NodeAddress,
    coordinator: ActorRef[CoordinatorMsg],
    entity_factory: Callable[[str], Behavior[Any]],
    num_shards: int,
) -> Behavior[Any]:
    async def setup(ctx: Any) -> Any:
        # Local entities managed by this region
        entities: dict[str, ActorRef[Any]] = {}
        # Cache: shard_id → node
        shard_locations: dict[int, NodeAddress] = {}
        # Buffer: messages waiting for shard location
        pending: dict[int, list[ShardEnvelope[Any]]] = {}

        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ShardEnvelope(entity_id, inner_msg):
                    shard_id = hash(entity_id) % num_shards

                    if shard_id in shard_locations:
                        node = shard_locations[shard_id]
                        if node == self_node:
                            _deliver_local(ctx, entities, entity_id, inner_msg, entity_factory)
                        # TODO: forward to remote node
                        return Behaviors.same()

                    # Ask coordinator for shard location
                    if shard_id not in pending:
                        pending[shard_id] = []

                        async def ask_coordinator(sid: int = shard_id) -> None:
                            # Use a callback ref to get the response
                            pass

                        # For now, assume local allocation (single-node)
                        # Full remote resolution comes with cluster integration
                        shard_locations[shard_id] = self_node
                        _deliver_local(ctx, entities, entity_id, inner_msg, entity_factory)
                    else:
                        pending[shard_id].append(ShardEnvelope(entity_id, inner_msg))

                    return Behaviors.same()

                case ShardLocation(shard_id, node):
                    shard_locations[shard_id] = node
                    # Flush pending messages for this shard
                    buffered = pending.pop(shard_id, [])
                    for envelope in buffered:
                        if node == self_node:
                            _deliver_local(
                                ctx, entities, envelope.entity_id,
                                envelope.message, entity_factory,
                            )
                    return Behaviors.same()

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


def _deliver_local(
    ctx: Any,
    entities: dict[str, ActorRef[Any]],
    entity_id: str,
    msg: Any,
    entity_factory: Callable[[str], Behavior[Any]],
) -> None:
    if entity_id not in entities:
        ref = ctx.spawn(entity_factory(entity_id), f"entity-{entity_id}")
        entities[entity_id] = ref
    entities[entity_id].tell(msg)
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_shard_region.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/casty/sharding.py src/casty/_shard_region_actor.py tests/test_shard_region.py
git commit -m "feat: add ShardRegionActor with local entity management"
```

---

## Task 16: Integration test — full local sharding workflow

**Files:**
- Test: `tests/test_sharding_integration.py`

**Step 1: Write the integration test**

```python
# tests/test_sharding_integration.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef, Behavior
from casty.cluster import Cluster, ClusterConfig
from casty.sharding import ShardEnvelope


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountMsg = Deposit | GetBalance


def account_entity(entity_id: str) -> Behavior[AccountMsg]:
    def active(balance: int) -> Behavior[AccountMsg]:
        async def receive(ctx: Any, msg: AccountMsg) -> Any:
            match msg:
                case Deposit(amount):
                    return active(balance + amount)
                case GetBalance(reply_to):
                    reply_to.tell(balance)
                    return Behaviors.same()
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)


async def test_sharding_full_workflow() -> None:
    """Full local sharding: create cluster, init sharding, route messages."""
    async with ActorSystem(name="bank") as system:
        from casty._shard_coordinator_actor import (
            shard_coordinator_actor,
            LeastShardStrategy,
        )
        from casty._shard_region_actor import shard_region_actor
        from casty.cluster_state import NodeAddress

        self_node = NodeAddress(host="127.0.0.1", port=25520)

        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=account_entity,
                num_shards=50,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        # Deposit to multiple accounts
        for i in range(5):
            region_ref.tell(ShardEnvelope(f"account-{i}", Deposit(100 * (i + 1))))

        # Deposit more to account-0
        region_ref.tell(ShardEnvelope("account-0", Deposit(50)))
        await asyncio.sleep(0.3)

        # Check balances
        balance_0 = await system.ask(
            region_ref,
            lambda r: ShardEnvelope("account-0", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance_0 == 150  # 100 + 50

        balance_4 = await system.ask(
            region_ref,
            lambda r: ShardEnvelope("account-4", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance_4 == 500
```

**Step 2: Run test**

Run: `uv run pytest tests/test_sharding_integration.py -v`
Expected: All PASS

**Step 3: Commit**

```bash
git add tests/test_sharding_integration.py
git commit -m "test: add full sharding integration test"
```

---

## Task 17: Final exports and full test suite

**Files:**
- Modify: `src/casty/__init__.py` (add all new exports)
- Run: full test suite + pyright + ruff

**Step 1: Update exports**

Add to `__init__.py`:
- `ActorAddress`
- `MessageTransport`, `LocalTransport`
- `PhiAccrualFailureDetector`
- `TypeRegistry`, `JsonSerializer`, `Serializer`
- `MessageEnvelope`, `TcpTransport`
- `VectorClock`, `NodeAddress`, `Member`, `MemberStatus`, `ClusterState`
- `Cluster`, `ClusterConfig`
- `ShardEnvelope`
- `ShardAllocationStrategy`, `LeastShardStrategy`
- Cluster events: `MemberUp`, `MemberLeft`, `UnreachableMember`, `ReachableMember`

**Step 2: Run full verification**

```bash
uv run pytest -v --tb=short
uv run pyright src/casty/
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

Expected: All PASS, no type errors, no lint issues

**Step 3: Commit**

```bash
git add -A
git commit -m "feat: export all cluster and sharding types"
```

---

## Summary

| Task | Stream | Description | Parallel? |
|------|--------|-------------|-----------|
| 1 | A | ActorAddress dataclass | Foundation |
| 2 | A | MessageTransport + LocalTransport | Foundation |
| 3 | A | Refactor ActorRef (address + transport) | Foundation |
| 4 | A | ActorSystem.resolve() | Foundation |
| 5 | A | Update exports | Foundation |
| 6 | D | Phi Accrual Failure Detector | Yes (with 7, 8) |
| 7 | B | Serialization (TypeRegistry + JsonSerializer) | Yes (with 6, 8) |
| 8 | C | TCP Transport | Yes (with 6, 7) |
| 9 | E | VectorClock + ClusterState | After 6-8 |
| 10 | E | Cluster Events | After 9 |
| 11 | E | HeartbeatActor | After 9 |
| 12 | E | GossipActor | After 9 |
| 13 | E | Cluster entry point | After 11, 12 |
| 14 | F | ShardCoordinatorActor | After 13 |
| 15 | F | ShardRegionActor | After 14 |
| 16 | F | Integration test | After 15 |
| 17 | — | Final exports + verification | After all |
