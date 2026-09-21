"""Named collections over an actor system or a client.

Each collection is a handful of actor types, configured by replicas and write level: a configuration is a type of
its own, so two collections of the same kind with different settings never share a key. The bodies of those types
run in the core; what is here builds their messages and reads their answers.
"""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Hashable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from hashlib import blake2b
from typing import cast
from uuid import UUID, uuid4

from casty import ActorDefinition, Context, DefaultedActor, Ref, System, Unavailable, Write, actor

__all__ = [
    "MISSING",
    "Barrier",
    "Collections",
    "ConfigurationError",
    "Counter",
    "Dict",
    "Lease",
    "Lock",
    "Missing",
    "MultiMap",
    "Queue",
    "Register",
    "Semaphore",
    "Set",
]


@dataclass(frozen=True)
class Missing:
    """An absent entry, distinct from a stored None."""


MISSING = Missing()


class ConfigurationError(ValueError):
    """A collection was used with settings other than the ones its first operation fixed."""


@dataclass(frozen=True)
class RegisterState:
    value: bytes | None = None


@dataclass(frozen=True)
class TableState:
    entries: Mapping[bytes, tuple[bytes, ...]] = field(default_factory=dict[bytes, tuple[bytes, ...]])


@dataclass(frozen=True)
class EntryState:
    value: bytes | None = None
    indexed: bool = False


@dataclass(frozen=True)
class Permit:
    """A held permit: who holds it, the token it fences with, how much it takes and when it runs out."""

    id: UUID
    token: int
    count: int
    expires: float


@dataclass(frozen=True)
class Waiter:
    """A request waiting for permits, which is granted in order or times out where it is."""

    id: UUID
    reply_to: Ref[int | None]
    count: int
    ttl: float
    until: float


@dataclass(frozen=True)
class SemaphoreState:
    capacity: int = 0
    next_token: int = 1
    held: tuple[Permit, ...] = ()
    pending: tuple[Waiter, ...] = ()


@dataclass(frozen=True)
class Party:
    """A party waiting at the barrier, until it is released or its deadline passes."""

    id: UUID
    reply_to: Ref[bool]
    until: float


@dataclass(frozen=True)
class Completed:
    """A generation that was released, remembered so that a party asking again hears it went through."""

    id: UUID
    until: float


@dataclass(frozen=True)
class BarrierState:
    generation: int = 0
    pending: tuple[Party, ...] = ()
    completed: tuple[Completed, ...] = ()


class counter:
    """A number that goes up and down. Striped: an aggregate read is not atomic across the stripes."""

    @dataclass(frozen=True)
    class Add:
        reply_to: Ref[None]
        delta: int

    @dataclass(frozen=True)
    class Get:
        reply_to: Ref[int]

    @dataclass(frozen=True)
    class Reset:
        reply_to: Ref[None]

    type Message = Add | Get | Reset

    @actor(initial=0)
    async def actor(ctx: Context[int, Message]) -> None:
        """The body runs in the core."""


class register:
    """One value, replaced whole, with a compare-and-set that decides in one message."""

    @dataclass(frozen=True)
    class Get:
        reply_to: Ref[bytes | None]

    @dataclass(frozen=True)
    class Put:
        reply_to: Ref[None]
        value: bytes

    @dataclass(frozen=True)
    class CompareAndSet:
        reply_to: Ref[bool]
        expected: bytes | None
        value: bytes

    @dataclass(frozen=True)
    class GetAndSet:
        reply_to: Ref[bytes | None]
        value: bytes

    type Message = Get | Put | CompareAndSet | GetAndSet

    @actor(initial=RegisterState())
    async def actor(ctx: Context[RegisterState, Message]) -> None:
        """The body runs in the core."""


class table:
    """A key with the values listed under it, which is what a set, a dict and a multimap are indexed by."""

    @dataclass(frozen=True)
    class Add:
        reply_to: Ref[bool]
        key: bytes
        value: bytes

    @dataclass(frozen=True)
    class Get:
        reply_to: Ref[tuple[bytes, ...]]
        key: bytes

    @dataclass(frozen=True)
    class Remove:
        reply_to: Ref[int]
        key: bytes
        value: bytes | None

    @dataclass(frozen=True)
    class Size:
        reply_to: Ref[int]

    @dataclass(frozen=True)
    class Clear:
        reply_to: Ref[None]

    @dataclass(frozen=True)
    class Items:
        reply_to: Ref[Mapping[bytes, tuple[bytes, ...]]]

    type Message = Add | Get | Remove | Size | Clear | Items

    @actor(initial=TableState())
    async def actor(ctx: Context[TableState, Message]) -> None:
        """The body runs in the core."""


class entry:
    """One entry of a dict: the value under a key, and the index it is listed in."""

    @dataclass(frozen=True)
    class Put:
        reply_to: Ref[None]
        key: bytes
        value: bytes
        index: Ref[table.Message]

    @dataclass(frozen=True)
    class Get:
        reply_to: Ref[bytes | None]

    @dataclass(frozen=True)
    class Contains:
        reply_to: Ref[bool]

    @dataclass(frozen=True)
    class Remove:
        reply_to: Ref[bool]

    type Message = Put | Get | Contains | Remove

    @actor(initial=EntryState())
    async def actor(ctx: Context[EntryState, Message]) -> None:
        """The body runs in the core."""


class queue:
    """Items taken in the order they were offered. A lost poll or drain can lose them to the caller."""

    @dataclass(frozen=True)
    class Offer:
        reply_to: Ref[None]
        value: bytes

    @dataclass(frozen=True)
    class Poll:
        reply_to: Ref[bytes | None]

    @dataclass(frozen=True)
    class Peek:
        reply_to: Ref[bytes | None]

    @dataclass(frozen=True)
    class Drain:
        reply_to: Ref[tuple[bytes, ...]]
        limit: int

    @dataclass(frozen=True)
    class Size:
        reply_to: Ref[int]

    @dataclass(frozen=True)
    class Clear:
        reply_to: Ref[None]

    type Message = Offer | Poll | Peek | Drain | Size | Clear

    @actor(initial=cast("tuple[bytes, ...]", ()))
    async def actor(ctx: Context[tuple[bytes, ...], Message]) -> None:
        """The body runs in the core."""


class semaphore:
    """Leases over a capacity, granted in order, renewed while they are held, expired when they are not."""

    @dataclass(frozen=True)
    class Request:
        reply_to: Ref[int | None]
        id: UUID
        count: int
        ttl: float
        capacity: int
        until: float | None

    @dataclass(frozen=True)
    class Cancel:
        reply_to: Ref[None]
        id: UUID

    @dataclass(frozen=True)
    class Release:
        reply_to: Ref[bool]
        token: int

    @dataclass(frozen=True)
    class Renew:
        reply_to: Ref[bool]
        token: int
        ttl: float

    @dataclass(frozen=True)
    class Available:
        reply_to: Ref[int]
        capacity: int

    type Message = Request | Cancel | Release | Renew | Available

    @actor(initial=SemaphoreState())
    async def actor(ctx: Context[SemaphoreState, Message]) -> None:
        """The body runs in the core."""


class barrier:
    """Parties that arrive and are all released together, or time out where they wait."""

    @dataclass(frozen=True)
    class Arrive:
        reply_to: Ref[bool]
        id: UUID
        parties: int
        until: float

    @dataclass(frozen=True)
    class Cancel:
        reply_to: Ref[bool]
        id: UUID

    @dataclass(frozen=True)
    class Waiting:
        reply_to: Ref[int]

    type Message = Arrive | Cancel | Waiting

    @actor(initial=BarrierState())
    async def actor(ctx: Context[BarrierState, Message]) -> None:
        """The body runs in the core."""


def configured[S, M](definition: DefaultedActor[S, M], replicas: int, write: Write) -> DefaultedActor[S, M]:
    """The same type under the name its configuration gives it, so that two settings never share a key."""
    kind = definition.name.rsplit(":", 1)[1].split(".", 1)[0]
    return definition.configured(f"casty.collections:{kind}_{replicas}_{write}", replicas, write)


#: The actor of each kind, which is what a configured name resolves to.
_KINDS: dict[str, ActorDefinition] = {
    "counter": counter.actor,
    "register": register.actor,
    "table": table.actor,
    "entry": entry.actor,
    "queue": queue.actor,
    "semaphore": semaphore.actor,
    "barrier": barrier.actor,
}


def __getattr__(name: str) -> ActorDefinition:
    """Resolve `kind_replicas_level`, which is how a node that never used the collection finds its type."""
    parts = name.rsplit("_", 2)
    if len(parts) != 3 or not parts[1].isdigit() or int(parts[1]) < 1:
        raise AttributeError(name)
    kind, replicas, level = parts
    if kind not in _KINDS:
        raise AttributeError(name)
    write: Write
    match level:
        case "one" | "majority" | "all":
            write = level
        case _:
            raise AttributeError(name)
    return _KINDS[kind].configured(f"casty.collections:{kind}_{replicas}_{write}", int(replicas), write)


class Value[T]:
    """A stored value, encoded in the canonical order so that equal values have equal bytes.

    The system is what compiles the annotation and what encodes it: a ref inside a value has to come back bound to
    the node that can reach it, and only the system knows which node that is.
    """

    def __init__(self, annotation: type[T], system: System) -> None:
        self._schema = system._schema(annotation)  # pyright: ignore[reportPrivateUsage]
        self._system = system

    def dump(self, value: T) -> bytes:
        return self._system._encode(self._schema, value)  # pyright: ignore[reportPrivateUsage]

    def load(self, raw: bytes) -> T:
        # The schema was compiled from the annotation of `T`, so what comes back out of it is one.
        return cast("T", self._system._decode(self._schema, raw))  # pyright: ignore[reportPrivateUsage]

    def optional(self, raw: bytes | None) -> T | Missing:
        return MISSING if raw is None else self.load(raw)


class Binding:
    """A named collection: the types its keys belong to, and the settings the first operation fixes."""

    def __init__(
        self,
        system: System,
        kind: str,
        name: str,
        *,
        replicas: int,
        write: Write,
        shards: int = 1,
        signature: tuple[str, ...] = (),
    ) -> None:
        if shards < 1:
            raise ValueError("shards must be positive")
        if replicas < 1:
            raise ValueError("replicas must be positive")
        self.system = system
        self.kind = kind
        self.name = name
        self.replicas = replicas
        self.write: Write = write
        self.shards = shards
        self._metadata = system.ref(register.actor, f"{kind}:{len(name)}:{name}")
        self._settings = Value[tuple[str | int, ...]](tuple[str | int, ...], system).dump(
            (kind, replicas, write, shards, *signature)
        )
        self._ready = False
        self._lock = asyncio.Lock()

    async def ready(self) -> None:
        if self._ready:
            return
        async with self._lock:
            if self._ready:
                return
            if (
                not await self._metadata.ask(register.CompareAndSet, None, self._settings)
                and await self._metadata.ask(register.Get) != self._settings
            ):
                raise ConfigurationError(f"incompatible configuration for collection {self.name!r}")
            self._ready = True

    def ref[S, M](self, definition: DefaultedActor[S, M], shard: int | str = 0) -> Ref[M]:
        key = f"{self.kind}:{len(self.name)}:{self.name}:{shard}"
        return self.system.ref(configured(definition, self.replicas, self.write), key)

    def shard(self, key: bytes) -> int:
        return int.from_bytes(blake2b(key, digest_size=8).digest()) % self.shards

    async def each[T](self, operation: Callable[[int], Awaitable[T]]) -> list[T]:
        await self.ready()
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(_run(operation, shard)) for shard in range(self.shards)]
        return [task.result() for task in tasks]


async def _run[T](operation: Callable[[int], Awaitable[T]], shard: int) -> T:
    return await operation(shard)


def _duration(value: float, name: str, *, zero: bool = False) -> None:
    if not math.isfinite(value) or value < 0 or (value == 0 and not zero):
        raise ValueError(f"{name} must be {'nonnegative' if zero else 'positive'} and finite")


async def _wait[T](request: Callable[[float], Awaitable[T]]) -> T:
    """Ask again while the interest lasts: a lost reply is reattached, not turned into a second arrival."""
    while True:
        until = time.time() + 30.0
        try:
            async with asyncio.timeout(10.0):
                return await request(until)
        except TimeoutError:
            if time.time() >= until:
                raise


class Counter:
    """Striped counter. Aggregate reads and resets are not atomic across stripes."""

    def __init__(self, binding: Binding) -> None:
        self._binding = binding
        self._next = 0

    async def add(self, delta: int = 1) -> None:
        await self._binding.ready()
        shard = self._next % self._binding.shards
        self._next += 1
        await self._binding.ref(counter.actor, shard).ask(counter.Add, delta)

    async def get(self) -> int:
        return sum(await self._binding.each(lambda shard: self._binding.ref(counter.actor, shard).ask(counter.Get)))

    async def reset(self) -> None:
        await self._binding.each(lambda shard: self._binding.ref(counter.actor, shard).ask(counter.Reset))


class Register[T]:
    """One value, replaced whole."""

    def __init__(self, binding: Binding, value: type[T]) -> None:
        self._binding = binding
        self._value = Value(value, binding.system)
        self._ref = binding.ref(register.actor)

    async def get(self) -> T | Missing:
        await self._binding.ready()
        return self._value.optional(await self._ref.ask(register.Get))

    async def set(self, value: T) -> None:
        await self._binding.ready()
        await self._ref.ask(register.Put, self._value.dump(value))

    async def compare_and_set(self, expected: T | Missing, value: T) -> bool:
        """Compare encoded values and commit the replacement in one actor message."""
        await self._binding.ready()
        raw = None if isinstance(expected, Missing) else self._value.dump(expected)
        return await self._ref.ask(register.CompareAndSet, raw, self._value.dump(value))

    async def get_and_set(self, value: T) -> T | Missing:
        await self._binding.ready()
        return self._value.optional(await self._ref.ask(register.GetAndSet, self._value.dump(value)))


class _Table:
    """What a set, a dict and a multimap share: an index sharded by key."""

    def __init__(self, binding: Binding) -> None:
        self._binding = binding

    async def size(self) -> int:
        return sum(await self._binding.each(lambda shard: self._binding.ref(table.actor, shard).ask(table.Size)))

    async def clear(self) -> None:
        await self._binding.each(lambda shard: self._binding.ref(table.actor, shard).ask(table.Clear))


class Dict[K, V]:
    """Entries under their own keys, with an index of the keys sharded across `index_shards`."""

    def __init__(self, binding: Binding, key: type[K], value: type[V]) -> None:
        self._binding = binding
        self._key = Value(key, binding.system)
        self._value = Value(value, binding.system)

    async def put(self, key: K, value: V) -> None:
        await self._binding.ready()
        raw = self._key.dump(key)
        index = self._binding.ref(table.actor, self._binding.shard(raw))
        await self._entry(raw).ask(entry.Put, raw, self._value.dump(value), index)

    async def get(self, key: K) -> V | Missing:
        await self._binding.ready()
        return self._value.optional(await self._entry(self._key.dump(key)).ask(entry.Get))

    async def contains(self, key: K) -> bool:
        await self._binding.ready()
        return await self._entry(self._key.dump(key)).ask(entry.Contains)

    async def remove(self, key: K) -> bool:
        await self._binding.ready()
        return await self._entry(self._key.dump(key)).ask(entry.Remove)

    async def items(self) -> list[tuple[K, V]]:
        return [item for shard in await self._binding.each(self._items) for item in shard]

    async def size(self) -> int:
        return sum(await self._binding.each(self._size))

    async def clear(self) -> None:
        await self._binding.each(self._clear)

    def _entry(self, raw: bytes) -> Ref[entry.Message]:
        return self._binding.ref(entry.actor, f"key:{raw.hex()}")

    async def _keys(self, shard: int) -> tuple[bytes, ...]:
        return tuple(await self._binding.ref(table.actor, shard).ask(table.Items))

    async def _items(self, shard: int) -> list[tuple[K, V]]:
        items: list[tuple[K, V]] = []
        for raw in await self._keys(shard):
            value = await self._entry(raw).ask(entry.Get)
            if value is not None:
                items.append((self._key.load(raw), self._value.load(value)))
        return items

    async def _size(self, shard: int) -> int:
        count = 0
        for raw in await self._keys(shard):
            count += await self._entry(raw).ask(entry.Contains)
        return count

    async def _clear(self, shard: int) -> None:
        for raw in await self._keys(shard):
            await self._entry(raw).ask(entry.Remove)


class Set[T: Hashable](_Table):
    """Unique encoded values, with client-side set algebra over non-atomic snapshots."""

    def __init__(self, binding: Binding, value: type[T]) -> None:
        super().__init__(binding)
        self._value = Value(value, binding.system)

    async def add(self, value: T) -> bool:
        await self._binding.ready()
        raw = self._value.dump(value)
        return await self._binding.ref(table.actor, self._binding.shard(raw)).ask(table.Add, raw, raw)

    async def remove(self, value: T) -> bool:
        await self._binding.ready()
        raw = self._value.dump(value)
        return bool(await self._binding.ref(table.actor, self._binding.shard(raw)).ask(table.Remove, raw, None))

    async def contains(self, value: T) -> bool:
        await self._binding.ready()
        raw = self._value.dump(value)
        return bool(await self._binding.ref(table.actor, self._binding.shard(raw)).ask(table.Get, raw))

    async def items(self) -> list[T]:
        shards = await self._binding.each(lambda shard: self._binding.ref(table.actor, shard).ask(table.Items))
        return [self._value.load(raw) for shard in shards for raw in shard]

    async def union(self, other: "Set[T]") -> set[T]:
        return set(await self.items()) | set(await other.items())

    async def intersection(self, other: "Set[T]") -> set[T]:
        return set(await self.items()) & set(await other.items())

    async def difference(self, other: "Set[T]") -> set[T]:
        return set(await self.items()) - set(await other.items())


class MultiMap[K, V](_Table):
    """Values listed under a key, each one unique for that key."""

    def __init__(self, binding: Binding, key: type[K], value: type[V]) -> None:
        super().__init__(binding)
        self._key = Value(key, binding.system)
        self._value = Value(value, binding.system)

    async def put(self, key: K, value: V) -> bool:
        await self._binding.ready()
        raw = self._key.dump(key)
        return await self._binding.ref(table.actor, self._binding.shard(raw)).ask(
            table.Add, raw, self._value.dump(value)
        )

    async def get(self, key: K) -> list[V]:
        await self._binding.ready()
        raw = self._key.dump(key)
        values = await self._binding.ref(table.actor, self._binding.shard(raw)).ask(table.Get, raw)
        return [self._value.load(value) for value in values]

    async def contains(self, key: K, value: V) -> bool:
        await self._binding.ready()
        raw = self._key.dump(key)
        values = await self._binding.ref(table.actor, self._binding.shard(raw)).ask(table.Get, raw)
        return self._value.dump(value) in values

    async def remove(self, key: K, value: V) -> bool:
        await self._binding.ready()
        raw = self._key.dump(key)
        return bool(
            await self._binding.ref(table.actor, self._binding.shard(raw)).ask(
                table.Remove,
                raw,
                self._value.dump(value),
            )
        )

    async def remove_key(self, key: K) -> int:
        await self._binding.ready()
        raw = self._key.dump(key)
        return await self._binding.ref(table.actor, self._binding.shard(raw)).ask(table.Remove, raw, None)


class Queue[T]:
    """Single-owner FIFO. A lost poll/drain response can lose removed items to the caller."""

    def __init__(self, binding: Binding, value: type[T]) -> None:
        self._binding = binding
        self._value = Value(value, binding.system)
        self._ref = binding.ref(queue.actor)

    async def offer(self, value: T) -> None:
        await self._binding.ready()
        await self._ref.ask(queue.Offer, self._value.dump(value))

    async def poll(self) -> T | Missing:
        await self._binding.ready()
        return self._value.optional(await self._ref.ask(queue.Poll))

    async def peek(self) -> T | Missing:
        await self._binding.ready()
        return self._value.optional(await self._ref.ask(queue.Peek))

    async def size(self) -> int:
        await self._binding.ready()
        return await self._ref.ask(queue.Size)

    async def drain(self, max_items: int) -> list[T]:
        if max_items < 0:
            raise ValueError("max_items must be nonnegative")
        await self._binding.ready()
        return [self._value.load(raw) for raw in await self._ref.ask(queue.Drain, max_items)]

    async def clear(self) -> None:
        await self._binding.ready()
        await self._ref.ask(queue.Clear)


@dataclass(frozen=True)
class Lease:
    """Time-limited permits. The protected resource must reject older fencing tokens."""

    token: int
    _semaphore: "Semaphore"

    async def renew(self, ttl: float = 30.0) -> bool:
        _duration(ttl, "ttl")
        return await self._semaphore.ref.ask(semaphore.Renew, self.token, ttl)

    async def release(self) -> bool:
        return await self._semaphore.ref.ask(semaphore.Release, self.token)

    async def __aenter__(self) -> "Lease":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class Semaphore:
    """FIFO waiters and renewable leases. TTLs use wall clocks, which must be synchronized across nodes.

    Times are seconds. A transport failure aborts a wait; abandoned requests expire within 30 seconds.
    Existing leases retain their expiry on failover. No automatic renewal is performed.
    """

    def __init__(self, binding: Binding, capacity: int) -> None:
        self._binding = binding
        self._capacity = capacity
        self.ref = binding.ref(semaphore.actor)

    def _validate(self, n: int, ttl: float) -> None:
        if not 1 <= n <= self._capacity:
            raise ValueError("n must be between one and the semaphore capacity")
        _duration(ttl, "ttl")

    async def try_acquire(self, n: int = 1, *, ttl: float = 30.0) -> Lease | None:
        self._validate(n, ttl)
        await self._binding.ready()
        token = await self.ref.ask(semaphore.Request, uuid4(), n, ttl, self._capacity, None)
        return None if token is None else Lease(token, self)

    async def acquire(self, n: int = 1, *, ttl: float = 30.0) -> Lease:
        """Wait for permits; use asyncio.timeout to bound the wait."""
        self._validate(n, ttl)
        await self._binding.ready()
        id = uuid4()
        try:
            token = await _wait(lambda until: self.ref.ask(semaphore.Request, id, n, ttl, self._capacity, until))
            if token is None:
                raise TimeoutError("semaphore acquisition timed out")
            return Lease(token, self)
        except BaseException:
            await self._cancel(id)
            raise

    async def _cancel(self, id: UUID) -> None:
        try:
            async with asyncio.timeout(1.0):
                await self.ref.ask(semaphore.Cancel, id)
        except (TimeoutError, Unavailable):
            # A replicated expiry bounds an abandoned request when cancellation cannot reach the owner.
            pass

    async def available(self) -> int:
        await self._binding.ready()
        return await self.ref.ask(semaphore.Available, self._capacity)


class Lock:
    """One holder at a time, with a lease that expires if the holder goes away."""

    def __init__(self, permits: Semaphore, ttl: float, timeout: float | None) -> None:
        _duration(ttl, "ttl")
        if timeout is not None:
            _duration(timeout, "timeout", zero=True)
        self._semaphore = permits
        self._ttl = ttl
        self._timeout = timeout
        self._held: ContextVar[tuple[asyncio.Task[object], Lease] | None] = ContextVar("collection_lock", default=None)

    async def try_lock(self, *, ttl: float | None = None) -> Lease | None:
        return await self._semaphore.try_acquire(ttl=self._ttl if ttl is None else ttl)

    async def acquire(self, *, ttl: float | None = None) -> Lease:
        async with asyncio.timeout(self._timeout):
            return await self._semaphore.acquire(ttl=self._ttl if ttl is None else ttl)

    async def locked(self) -> bool:
        return await self._semaphore.available() == 0

    async def __aenter__(self) -> Lease:
        task = asyncio.current_task()
        assert task is not None
        held = self._held.get()
        if held is not None and held[0] is task:
            raise RuntimeError("lock context is not reentrant")
        lease = await self.acquire()
        self._held.set((task, lease))
        return lease

    async def __aexit__(self, *exc: object) -> None:
        held = self._held.get()
        if held is None or held[0] is not asyncio.current_task():
            raise RuntimeError("lock context was not entered")
        self._held.set(None)
        await held[1].release()


class Barrier:
    """Cyclic barrier. Dead participants expire within 30 seconds; failed transports abort their local wait."""

    def __init__(self, binding: Binding, parties: int) -> None:
        self._binding = binding
        self._parties = parties
        self._ref = binding.ref(barrier.actor)

    @property
    def parties(self) -> int:
        return self._parties

    async def wait(self) -> None:
        """Join this generation; asyncio.timeout bounds the wait and withdraws this arrival."""
        await self._binding.ready()
        id = uuid4()
        try:
            released = await _wait(lambda until: self._ref.ask(barrier.Arrive, id, self.parties, until))
            if not released:
                raise TimeoutError("barrier wait timed out")
        except TimeoutError:
            if not await self._cancel(id):
                raise
        except BaseException:
            await self._cancel(id)
            raise

    async def _cancel(self, id: UUID) -> bool:
        try:
            async with asyncio.timeout(1.0):
                return await self._ref.ask(barrier.Cancel, id)
        except (TimeoutError, Unavailable):
            return False

    async def waiting(self) -> int:
        await self._binding.ready()
        return await self._ref.ask(barrier.Waiting)


class Collections:
    """Named collections over the supplied actor system or client.

    Configuration is fixed by the first operation. Mutating calls acknowledge saved state;
    a failed or timed-out call may have committed and is not automatically repeated.
    """

    def __init__(self, system: System) -> None:
        self._system = system

    def counter(
        self,
        name: str,
        *,
        stripes: int = 1,
        replicas: int = 3,
        write: Write = "majority",
    ) -> Counter:
        return Counter(Binding(self._system, "counter", name, replicas=replicas, write=write, shards=stripes))

    def register[T](
        self,
        name: str,
        *,
        value: type[T],
        replicas: int = 3,
        write: Write = "majority",
    ) -> Register[T]:
        binding = Binding(
            self._system,
            "register",
            name,
            replicas=replicas,
            write=write,
            signature=(repr(value),),
        )
        return Register(binding, value)

    def dict[K, V](
        self,
        name: str,
        *,
        key: type[K],
        value: type[V],
        index_shards: int = 16,
        replicas: int = 3,
        write: Write = "majority",
    ) -> Dict[K, V]:
        binding = self._table("dict", name, index_shards, replicas, write, ("entry-v1", repr(key), repr(value)))
        return Dict(binding, key, value)

    def set[T: Hashable](
        self,
        name: str,
        *,
        value: type[T],
        shards: int = 16,
        replicas: int = 3,
        write: Write = "majority",
    ) -> Set[T]:
        return Set(self._table("set", name, shards, replicas, write, (repr(value),)), value)

    def multimap[K, V](
        self,
        name: str,
        *,
        key: type[K],
        value: type[V],
        shards: int = 16,
        replicas: int = 3,
        write: Write = "majority",
    ) -> MultiMap[K, V]:
        return MultiMap(self._table("multimap", name, shards, replicas, write, (repr(key), repr(value))), key, value)

    def queue[T](
        self,
        name: str,
        *,
        value: type[T],
        replicas: int = 3,
        write: Write = "majority",
    ) -> Queue[T]:
        return Queue(self._table("queue", name, 1, replicas, write, (repr(value),)), value)

    def semaphore(self, name: str, *, capacity: int, replicas: int = 3) -> Semaphore:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        binding = self._table("semaphore", name, 1, replicas, "majority", (str(capacity),))
        return Semaphore(binding, capacity)

    def lock(
        self,
        name: str,
        *,
        ttl: float = 30.0,
        timeout: float | None = None,
        replicas: int = 3,
    ) -> Lock:
        binding = self._table("lock", name, 1, replicas, "majority", ("1",))
        return Lock(Semaphore(binding, 1), ttl, timeout)

    def barrier(self, name: str, *, parties: int, replicas: int = 3) -> Barrier:
        if parties < 1:
            raise ValueError("parties must be positive")
        binding = self._table("barrier", name, 1, replicas, "majority", (str(parties),))
        return Barrier(binding, parties)

    def _table(
        self,
        kind: str,
        name: str,
        shards: int,
        replicas: int,
        write: Write,
        signature: tuple[str, ...],
    ) -> Binding:
        return Binding(
            self._system,
            kind,
            name,
            replicas=replicas,
            write=write,
            shards=shards,
            signature=signature,
        )
