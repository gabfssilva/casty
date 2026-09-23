"""Named collections over an actor system or a client.

Each collection is a handful of actor types, configured by replicas and write level: a configuration is a type of
its own, so two collections of the same kind with different settings never share a key. The bodies of those types
run in the core; what is here builds their messages and reads their answers.
"""

import asyncio
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Hashable, Iterable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
from hashlib import blake2b
from inspect import Signature
from typing import Concatenate, cast
from uuid import UUID, uuid4
from weakref import WeakKeyDictionary, WeakValueDictionary

from casty import ActorDefinition, ActorFailed, Context, DefaultedActor, Ref, System, Unavailable, Write, actor

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
    """A collection was asked for or used with settings other than the ones its first operation fixed."""


@dataclass(frozen=True)
class RegisterState:
    value: bytes | None = None


@dataclass(frozen=True)
class TableState:
    segments: int = 1


@dataclass(frozen=True)
class TableSegmentState:
    entries: Mapping[bytes, tuple[bytes, ...]] = field(default_factory=dict[bytes, tuple[bytes, ...]])
    modulus: int = 1
    id: int = 0
    version: int = 0


@dataclass(frozen=True)
class EntryState:
    value: bytes | None = None
    generation: int = 0


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


@dataclass(frozen=True)
class QueueState:
    head: int = 0
    tail: int = 0


@dataclass(frozen=True)
class QueueSegmentState:
    items: tuple[bytes, ...] = ()
    taken: int = 0
    sealed: bool = False


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


class table_segment:
    """Keys of a shard of an index, with the values listed under each: those whose hash leaves `id` over `modulus`.

    A message about a key the segment does not hold answers None and changes nothing, which is how a caller that has
    not seen a split learns of it. `Add` and `List` answer whether they changed the listing and whether the segment
    lists more than 512 keys, which is the cue to ask the directory for a split. `Size` and `Clear` answer the modulus
    first, which tells a walk whether the segment still held the keys a split moved. `Scan` answers the modulus and
    the keys, and is refused like a key at a `position` whose hash the segment does not hold. The split itself runs in
    the core, from the directory to the segment and from the segment to the new one.
    """

    @dataclass(frozen=True)
    class Add:
        reply_to: Ref[tuple[bool, bool] | None]
        key: bytes
        value: bytes

    @dataclass(frozen=True)
    class Get:
        reply_to: Ref[tuple[bytes, ...] | None]
        key: bytes

    @dataclass(frozen=True)
    class Remove:
        reply_to: Ref[int | None]
        key: bytes
        value: bytes | None

    @dataclass(frozen=True)
    class List:
        """List a dict key under `generation`, unless it is listed under a newer one."""

        reply_to: Ref[tuple[bool, bool] | None]
        key: bytes
        generation: int

    @dataclass(frozen=True)
    class Unlist:
        """Drop a dict key, unless it is listed under a newer generation than `generation`."""

        reply_to: Ref[bool | None]
        key: bytes
        generation: int

    @dataclass(frozen=True)
    class Size:
        reply_to: Ref[tuple[int, int]]

    @dataclass(frozen=True)
    class Scan:
        reply_to: Ref[tuple[int, Mapping[bytes, tuple[bytes, ...]]] | None]
        position: int

    @dataclass(frozen=True)
    class Clear:
        reply_to: Ref[tuple[int, int]]

    type Message = Add | Get | Remove | List | Unlist | Size | Scan | Clear

    @actor(initial=TableSegmentState())
    async def actor(ctx: Context[TableSegmentState, Message]) -> None:
        """The body runs in the core."""


class table:
    """The directory of a shard of an index: how many segments it has, split in order by linear hashing.

    `Grow` splits the next segment, `split`, into a new one after the last, `into`, when the directory still counts
    the `seen` segments the caller did, and answers how many it counts. A split is counted once it is done.
    """

    @dataclass(frozen=True)
    class Segments:
        reply_to: Ref[int]

    @dataclass(frozen=True)
    class Grow:
        reply_to: Ref[int]
        seen: int
        split: Ref[table_segment.Message]
        into: Ref[table_segment.Message]

    type Message = Segments | Grow

    @actor(initial=TableState())
    async def actor(ctx: Context[TableState, Message]) -> None:
        """The body runs in the core."""


class entry:
    """One entry of a dict: the value under a key, and the generation its index lists the key under.

    The entry never asks the index; the facade does. `Put` of a key with no value answers the generation to list the
    key under, and saves the value when it is sent again with that generation as `listed`; any other put saves and
    answers 0. `Remove` clears the value and answers the generation to unlist the key under. `Retire` answers the same
    for a listing a walk found with no value, raising the entry to `listed` first when a put listed the key and never
    saved its value, and answers 0 when there is a value.
    """

    @dataclass(frozen=True)
    class Put:
        reply_to: Ref[int]
        value: bytes
        listed: int

    @dataclass(frozen=True)
    class Get:
        reply_to: Ref[bytes | None]

    @dataclass(frozen=True)
    class Contains:
        reply_to: Ref[bool]

    @dataclass(frozen=True)
    class Remove:
        reply_to: Ref[tuple[bool, int]]

    @dataclass(frozen=True)
    class Retire:
        reply_to: Ref[int]
        listed: int

    type Message = Put | Get | Contains | Remove | Retire

    @actor(initial=EntryState())
    async def actor(ctx: Context[EntryState, Message]) -> None:
        """The body runs in the core."""


class queue:
    """The index of a queue: the first segment that may hold items, and the segment offers go to.

    Both only move forward, to at least what `Advance` names, which answers where they are. A segment below the tail
    is sealed, and one below the head is sealed and empty.
    """

    @dataclass(frozen=True)
    class Advance:
        reply_to: Ref[tuple[int, int]]
        head: int
        tail: int

    type Message = Advance

    @actor(initial=QueueState())
    async def actor(ctx: Context[QueueState, Message]) -> None:
        """The body runs in the core."""


class queue_segment:
    """Items of a queue in the order they were offered, under a key of their own.

    A segment is sealed once it holds 1024 items or 64 KiB, and takes no offer after that: `Offer` answers whether it
    took the item. `Take` and `Peek` answer the items and whether the segment is sealed, so that one that answers
    fewer than asked for and is sealed is empty for good.

    A segment sealed and drained is deleted when its activation ends. `Offer`, `Take` and `Peek` name the index of the
    queue and the number of the segment, which a segment nothing wrote asks the index about: found below the tail,
    it is one that was deleted, and answers as the sealed and empty segment it was.
    """

    @dataclass(frozen=True)
    class Offer:
        reply_to: Ref[bool]
        value: bytes
        index: Ref[queue.Message]
        at: int

    @dataclass(frozen=True)
    class Take:
        reply_to: Ref[tuple[tuple[bytes, ...], bool]]
        limit: int
        index: Ref[queue.Message]
        at: int

    @dataclass(frozen=True)
    class Peek:
        reply_to: Ref[tuple[tuple[bytes, ...], bool]]
        index: Ref[queue.Message]
        at: int

    @dataclass(frozen=True)
    class Size:
        reply_to: Ref[int]

    @dataclass(frozen=True)
    class Clear:
        reply_to: Ref[None]

    type Message = Offer | Take | Peek | Size | Clear

    @actor(initial=QueueSegmentState())
    async def actor(ctx: Context[QueueSegmentState, Message]) -> None:
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


def configured[D: ActorDefinition](definition: D, replicas: int, write: Write) -> D:
    """The same type under the name its configuration gives it, so that two settings never share a key."""
    kind = definition.name.rsplit(":", 1)[1].split(".", 1)[0]
    return definition.configured(f"casty.collections:{kind}_{replicas}_{write}", replicas, write)


#: The actor of each kind, which is what a configured name resolves to.
_KINDS: dict[str, ActorDefinition] = {
    "counter": counter.actor,
    "register": register.actor,
    "table": table.actor,
    "table_segment": table_segment.actor,
    "entry": entry.actor,
    "queue": queue.actor,
    "queue_segment": queue_segment.actor,
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
    return configured(_KINDS[kind], int(replicas), write)


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
    """A named collection: the types its keys belong to, and the settings the first operation fixes.

    `confirmed` once an operation found the settings to be the collection's, which they then are for good.
    """

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
        self.settings: tuple[str | int, ...] = (kind, replicas, write, shards, *signature)
        self.confirmed = False
        self._metadata = system.ref(register.actor, f"{kind}:{len(name)}:{name}")
        self._encoded = Value(tuple[str | int, ...], system).dump(self.settings)
        self._lock = asyncio.Lock()

    async def ready(self) -> None:
        if self.confirmed:
            return
        async with self._lock:
            if self.confirmed:
                return
            if (
                not await self._metadata.ask(register.CompareAndSet, None, self._encoded)
                and await self._metadata.ask(register.Get) != self._encoded
            ):
                raise ConfigurationError(f"incompatible configuration for collection {self.name!r}")
            self.confirmed = True

    def ref[S, M](self, definition: DefaultedActor[S, M], shard: int | str = 0) -> Ref[M]:
        key = f"{self.kind}:{len(self.name)}:{self.name}:{shard}"
        return self.system.ref(configured(definition, self.replicas, self.write), key)

    def shard(self, key: bytes) -> int:
        return int.from_bytes(blake2b(key, digest_size=8).digest()) % self.shards

    async def each[T](self, operation: Callable[[int], Coroutine[object, object, T]]) -> list[T]:
        await self.ready()
        return await _each(operation, range(self.shards))


async def _each[I, T](operation: Callable[[I], Coroutine[object, object, T]], among: Iterable[I]) -> list[T]:
    """`operation` on each of `among` at once, answered in their order."""
    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(operation(at)) for at in among]
    return [task.result() for task in tasks]


_PROBES = 64
"""How many keys of a segment are asked at once. Enough to overlap the round trips to the nodes that own them, and a
task for each of these workers, not for each key, is what reading a segment costs on the loop."""


async def _pooled[I, T](operation: Callable[[I], Awaitable[T]], among: Sequence[I]) -> list[T]:
    """`operation` on each of `among`, `_PROBES` at a time, answered in their order."""
    found: dict[int, T] = {}
    pending = iter(enumerate(among))

    async def work(_: int) -> None:
        for at, item in pending:
            found[at] = await operation(item)

    await _each(work, range(min(_PROBES, len(among))))
    return [found[at] for at in range(len(among))]


def _duration(value: float, name: str, *, zero: bool = False) -> None:
    if not math.isfinite(value) or value < 0 or (value == 0 and not zero):
        raise ValueError(f"{name} must be {'nonnegative' if zero else 'positive'} and finite")


_INTEREST = 30.0
"""Seconds the owner keeps a wait after its last ask, which is how long an abandoned one lingers."""

_REASK = 10.0
"""Seconds one ask of a wait gets before it is sent again: well within `_INTEREST`, so the owner never drops it."""


async def _wait[T](request: Callable[[float], Awaitable[T]]) -> T:
    """Ask again while the interest lasts: a lost reply is reattached, not turned into a second arrival."""
    while True:
        until = time.time() + _INTEREST
        try:
            async with asyncio.timeout(_REASK):
                return await request(until)
        except TimeoutError:
            if time.time() >= until:
                raise


async def _withdraw[T](cancel: Coroutine[object, object, T], unreached: T) -> T:
    """What `cancel` answers within a second, or `unreached` when it does not reach the owner in time.

    A wait that is never withdrawn expires on its own, `_INTEREST` after its last ask.
    """
    try:
        async with asyncio.timeout(1.0):
            return await cancel
    except (TimeoutError, Unavailable):
        return unreached


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


def _spread(raw: bytes) -> int:
    """The hash that places a key among the segments of its shard: the high half of the digest that picks the shard."""
    return int.from_bytes(blake2b(raw, digest_size=8).digest()) >> 32


def _place(spread: int, segments: int) -> int:
    """The segment holding the hash `spread` in a shard of `segments`, which are split in order by linear hashing."""
    level = 1 << (segments.bit_length() - 1)
    at = spread % (2 * level)
    return at if at < segments else spread % level


#: How many hashes `_spread` tells apart.
_HASHES = 1 << 32


def _reverse(spread: int) -> int:
    """The 32 bits of `spread` in the opposite order."""
    return int(f"{spread:032b}"[::-1], 2)


def _after(position: int, modulus: int) -> int | None:
    """Where a scan goes on after the segment that holds the hash `position` over `modulus`, or None past the last.

    A scan takes the hashes in the order of their bits reversed. In that order the hashes a segment holds are
    consecutive, and a split keeps them so, the segment holding the first half and the new one the second: the scan
    never goes back into the hashes of a segment it read, whatever splits afterwards, and a split ahead of it leaves
    every hash still to come where it was.
    """
    following = _reverse(position | (_HASHES - modulus)) + 1
    return None if following == _HASHES else _reverse(following)


class _Index:
    """The shards of a set, a dict or a multimap: each a directory, and the segments it counts.

    How many segments each shard has is kept from the last answer of its directory, and can only lag behind it: a
    segment that split since refuses a key it no longer holds, and the directory is asked again. The refs are kept
    too, since obtaining one asks the owner to start the key.
    """

    def __init__(self, binding: Binding) -> None:
        self._binding = binding
        self._counts = [1] * binding.shards
        self._directories: dict[int, Ref[table.Message]] = {}
        self._segments: dict[tuple[int, int], Ref[table_segment.Message]] = {}

    async def add(self, raw: bytes, value: bytes) -> bool:
        added, full = await self._keyed(raw, lambda segment: segment.ask(table_segment.Add, raw, value))
        if full:
            await self._grow(self._binding.shard(raw))
        return added

    async def get(self, raw: bytes) -> tuple[bytes, ...]:
        return await self._keyed(raw, lambda segment: segment.ask(table_segment.Get, raw))

    async def remove(self, raw: bytes, value: bytes | None) -> int:
        return await self._keyed(raw, lambda segment: segment.ask(table_segment.Remove, raw, value))

    async def list_under(self, raw: bytes, generation: int) -> bool:
        """List a dict key under `generation`, and whether it is listed under it: not when it is under a newer one."""
        listed, full = await self._keyed(raw, lambda segment: segment.ask(table_segment.List, raw, generation))
        if full:
            await self._grow(self._binding.shard(raw))
        return listed

    async def unlist(self, raw: bytes, generation: int) -> None:
        await self._keyed(raw, lambda segment: segment.ask(table_segment.Unlist, raw, generation))

    async def size(self) -> int:
        """How many values the segments list, with one ask to each of them."""
        return sum(await self.walk(lambda segment: segment.ask(table_segment.Size)))

    async def scan(self) -> AsyncIterator[Mapping[bytes, tuple[bytes, ...]]]:
        """The listings of every segment, as `segments` reads them, shard after shard."""
        for shard in range(self._binding.shards):
            async for listed in self.segments(shard):
                yield listed

    async def segments(self, shard: int) -> AsyncIterator[Mapping[bytes, tuple[bytes, ...]]]:
        """The listings of each segment of `shard`, one segment at a time, each from where `_after` goes on.

        Not a snapshot: a key written meanwhile may be seen or not. A key is seen at most once, since the hashes
        are read in an order no split takes back, and one listed throughout is seen.
        """
        await self._binding.ready()
        position: int | None = 0
        while position is not None:
            modulus, listed = await self._page(shard, position)
            yield listed
            position = _after(position, modulus)

    async def walk[R](
        self, visit: Callable[[Ref[table_segment.Message]], Coroutine[object, object, tuple[int, R]]]
    ) -> list[R]:
        """What `visit` answers at every segment of every shard, the shards at once and the segments of each at once.

        Not a snapshot: a key written meanwhile may be seen or not, and one a split moves meanwhile is seen once.
        """
        await self._binding.ready()
        shards = await _each(lambda shard: self._walk(shard, visit), range(self._binding.shards))
        return [answer for shard in shards for answer in shard]

    async def _walk[R](
        self, shard: int, visit: Callable[[Ref[table_segment.Message]], Coroutine[object, object, tuple[int, R]]]
    ) -> list[R]:
        count = await self._count(shard)
        visited = await _each(lambda at: visit(self._segment(shard, at)), range(count))
        moduli = {at: modulus for at, (modulus, _) in enumerate(visited)}
        found = [answer for _, answer in visited]
        # A segment that split after the count was read answered without the keys it moved, and the segment they
        # moved to is visited as well; one that split after it answered had them still, and it is not.
        for at in range(count, await self._count(shard) + 1):
            if moduli.get(at - (1 << (at.bit_length() - 1)), 0) > at:
                moduli[at], answer = await visit(self._segment(shard, at))
                found.append(answer)
        return found

    async def _page(self, shard: int, position: int) -> tuple[int, Mapping[bytes, tuple[bytes, ...]]]:
        """The modulus and the listings of the segment of `shard` that holds the hash `position`."""
        return await self._placed(shard, position, lambda segment: segment.ask(table_segment.Scan, position))

    async def _keyed[T](self, raw: bytes, ask: Callable[[Ref[table_segment.Message]], Awaitable[T | None]]) -> T:
        """What `ask` answers at the segment that holds `raw`."""
        return await self._placed(self._binding.shard(raw), _spread(raw), ask)

    async def _placed[T](
        self, shard: int, spread: int, ask: Callable[[Ref[table_segment.Message]], Awaitable[T | None]]
    ) -> T:
        """What `ask` answers at the segment of `shard` that holds `spread`, following the splits not seen here yet."""
        at = _place(spread, self._counts[shard])
        while True:
            answer = await ask(self._segment(shard, at))
            if answer is not None:
                return answer
            count = await self._count(shard)
            moved = _place(spread, count)
            # Only the split the directory is making can be ahead of its count, and it moved the key to the new segment.
            at = moved if moved != at else _place(spread, count + 1)

    async def _grow(self, shard: int) -> None:
        """Ask the directory of `shard` to split its next segment, as a write that found its segment full does."""
        seen = self._counts[shard]
        split = seen - (1 << (seen.bit_length() - 1))
        try:
            count = await self._directory(shard).ask(
                table.Grow, seen, self._segment(shard, split), self._segment(shard, seen)
            )
        except (TimeoutError, Unavailable, ActorFailed):
            # The write this follows went through, and the next one that finds a segment full asks again.
            return
        self._counts[shard] = max(self._counts[shard], count)

    async def _count(self, shard: int) -> int:
        count = await self._directory(shard).ask(table.Segments)
        self._counts[shard] = max(self._counts[shard], count)
        return count

    def _directory(self, shard: int) -> Ref[table.Message]:
        directory = self._directories.get(shard)
        if directory is None:
            directory = self._directories[shard] = self._binding.ref(table.actor, shard)
        return directory

    def _segment(self, shard: int, at: int) -> Ref[table_segment.Message]:
        segment = self._segments.get((shard, at))
        if segment is None:
            segment = self._segments[shard, at] = self._binding.ref(table_segment.actor, f"{shard}.{at}")
        return segment


class _Table:
    """What a set and a multimap share: an index sharded by key, each shard in segments."""

    def __init__(self, binding: Binding) -> None:
        self._binding = binding
        self._index = _Index(binding)

    async def size(self) -> int:
        """Count the values listed, with one ask to each segment of the index."""
        return await self._index.size()

    async def clear(self) -> None:
        await self._index.walk(lambda segment: segment.ask(table_segment.Clear))


class Dict[K, V]:
    """Entries under their own keys, with an index of the keys sharded across `index_shards`.

    Replacing the value of a key asks only its entry. A new key is listed before its value is saved, and a removed one
    is unlisted after its value is cleared, so a call that stops halfway may leave a listing without a value but never
    a value without a listing. `scan`, `items` and `clear` read the index a segment at a time, ask the entries of its
    keys a few dozen at a time, and drop each listing they find without a value; `items` and `clear` read the shards of
    the index at once. `size` counts the listings and asks no entry.
    """

    def __init__(self, binding: Binding, key: type[K], value: type[V]) -> None:
        self._binding = binding
        self._index = _Index(binding)
        self._key = Value(key, binding.system)
        self._value = Value(value, binding.system)
        self._generation = Value(int, binding.system)

    async def put(self, key: K, value: V) -> None:
        await self._binding.ready()
        raw = self._key.dump(key)
        data = self._value.dump(value)
        ref = self._entry(raw)
        # A key with no value answers the generation to list it under, and is saved when sent again naming that one.
        listed = 0
        while listed := await ref.ask(entry.Put, data, listed):
            if not await self._index.list_under(raw, listed):
                # A life of the key on a node whose clock ran ahead listed it later: the entry goes past that one.
                await ref.ask(entry.Retire, self._under(await self._index.get(raw)))
                listed = 0

    async def get(self, key: K) -> V | Missing:
        await self._binding.ready()
        return self._value.optional(await self._entry(self._key.dump(key)).ask(entry.Get))

    async def contains(self, key: K) -> bool:
        await self._binding.ready()
        return await self._entry(self._key.dump(key)).ask(entry.Contains)

    async def remove(self, key: K) -> bool:
        await self._binding.ready()
        raw = self._key.dump(key)
        removed, listed = await self._entry(raw).ask(entry.Remove)
        # Unlisted even when nothing was removed: it drops the listing a removal that stopped halfway left behind.
        await self._unlist(raw, listed)
        return removed

    async def scan(self) -> AsyncIterator[tuple[K, V]]:
        """Every entry, reading the index a segment of about 512 keys at a time and the values of each together.

        Not a snapshot: an entry written while the scan runs may be seen or not, and none is seen twice. Only the
        segment being read is held, whatever the size of the dict.
        """
        async for listed in self._index.scan():
            for pair in await self._entries(listed):
                yield pair

    async def items(self) -> list[tuple[K, V]]:
        """Every entry, read as `scan` reads them but the shards of the index at once."""
        shards = await self._binding.each(self._shard_items)
        return [pair for shard in shards for pair in shard]

    async def size(self) -> int:
        """Count the keys the index lists, with one ask to each of its segments and none to the entries.

        A put or a removal that stopped halfway leaves a key listed without a value, which is counted until a `scan`,
        `items` or `clear` drops it, or the key is put or removed again. So the count is never below the entries that
        were there throughout, and above it by those listings.
        """
        return await self._index.size()

    async def clear(self) -> None:
        """Remove every entry, as `items` reads them. Not atomic."""
        await self._binding.each(self._clear_shard)

    def _entry(self, raw: bytes) -> Ref[entry.Message]:
        return self._binding.ref(entry.actor, f"key:{raw.hex()}")

    async def _shard_items(self, shard: int) -> list[tuple[K, V]]:
        return [pair async for listed in self._index.segments(shard) for pair in await self._entries(listed)]

    async def _clear_shard(self, shard: int) -> None:
        async for listed in self._index.segments(shard):
            await self._probed(listed, self._drop)

    async def _entries(self, listed: Mapping[bytes, tuple[bytes, ...]]) -> list[tuple[K, V]]:
        """The entries of the keys a segment lists, dropping each listing found without a value."""
        return [
            (self._key.load(raw), self._value.load(value))
            for raw, value in await self._probed(listed, self._value_of)
            if value is not None
        ]

    async def _probed[T](
        self, listed: Mapping[bytes, tuple[bytes, ...]], probe: Callable[[bytes, tuple[bytes, ...]], Awaitable[T]]
    ) -> list[tuple[bytes, T]]:
        """What `probe` answers for every key of a segment and what the segment lists it under."""
        keys = list(listed)
        found = await _pooled(lambda raw: probe(raw, listed[raw]), keys)
        return list(zip(keys, found, strict=True))

    async def _value_of(self, raw: bytes, listing: tuple[bytes, ...]) -> bytes | None:
        ref = self._entry(raw)
        value = await ref.ask(entry.Get)
        if value is None:
            await self._unlist(raw, await ref.ask(entry.Retire, self._under(listing)))
        return value

    async def _drop(self, raw: bytes, listing: tuple[bytes, ...]) -> None:
        ref = self._entry(raw)
        _, generation = await ref.ask(entry.Remove)
        listed = self._under(listing)
        # A put that listed the key under a newer generation never saved its value: the entry is raised to it first.
        if generation < listed:
            generation = await ref.ask(entry.Retire, listed)
        await self._unlist(raw, generation)

    async def _unlist(self, raw: bytes, generation: int) -> None:
        """Drop the listing of `raw` under the generation its entry answered, which is 0 when there is none to drop."""
        if generation:
            await self._index.unlist(raw, generation)

    def _under(self, listed: tuple[bytes, ...]) -> int:
        """The generation a key is listed under, which is the one value the index keeps for it."""
        return max((self._generation.load(value) for value in listed), default=0)


class Set[T: Hashable](_Table):
    """Unique encoded values.

    `scan` reads them a segment of the index at a time and is not a snapshot; `items`, which reads the shards at once,
    and the set algebra are built on it, on the client. `intersection` and `difference` ask the other set about each
    member of this one.
    """

    def __init__(self, binding: Binding, value: type[T]) -> None:
        super().__init__(binding)
        self._value = Value(value, binding.system)

    async def add(self, value: T) -> bool:
        await self._binding.ready()
        raw = self._value.dump(value)
        return await self._index.add(raw, raw)

    async def remove(self, value: T) -> bool:
        await self._binding.ready()
        return bool(await self._index.remove(self._value.dump(value), None))

    async def contains(self, value: T) -> bool:
        await self._binding.ready()
        return bool(await self._index.get(self._value.dump(value)))

    async def scan(self) -> AsyncIterator[T]:
        """Every member, reading the index a segment of about 512 members at a time.

        Not a snapshot: a member added or removed while the scan runs may be seen or not, and none is seen twice.
        Only the segment being read is held, whatever the size of the set.
        """
        async for listed in self._index.scan():
            for raw in listed:
                yield self._value.load(raw)

    async def items(self) -> list[T]:
        """Every member, read as `scan` reads them but the shards of the index at once."""
        shards = await self._binding.each(self._shard_members)
        return [member for shard in shards for member in shard]

    async def union(self, other: "Set[T]") -> set[T]:
        """Members of either set, scanning one and then the other."""
        union = {member async for member in self.scan()}
        async for member in other.scan():
            union.add(member)
        return union

    async def intersection(self, other: "Set[T]") -> set[T]:
        """Members of this set that `other` holds, holding no more than the answer and a segment of this set."""
        return {member async for member, held in self._held_by(other) if held}

    async def difference(self, other: "Set[T]") -> set[T]:
        """Members of this set that `other` does not hold, holding no more than the answer and a segment of this set."""
        return {member async for member, held in self._held_by(other) if not held}

    async def _shard_members(self, shard: int) -> list[T]:
        return [self._value.load(raw) async for listed in self._index.segments(shard) for raw in listed]

    async def _held_by(self, other: "Set[T]") -> AsyncIterator[tuple[T, bool]]:
        """Every member of this set and whether `other` holds it, asking `other` about a segment's members together."""
        async for listed in self._index.scan():
            members = [self._value.load(raw) for raw in listed]
            for member, held in zip(members, await _pooled(other.contains, members), strict=True):
                yield member, held


class MultiMap[K, V](_Table):
    """Values listed under a key, each one unique for that key."""

    def __init__(self, binding: Binding, key: type[K], value: type[V]) -> None:
        super().__init__(binding)
        self._key = Value(key, binding.system)
        self._value = Value(value, binding.system)

    async def put(self, key: K, value: V) -> bool:
        await self._binding.ready()
        return await self._index.add(self._key.dump(key), self._value.dump(value))

    async def get(self, key: K) -> list[V]:
        await self._binding.ready()
        return [self._value.load(value) for value in await self._index.get(self._key.dump(key))]

    async def contains(self, key: K, value: V) -> bool:
        await self._binding.ready()
        return self._value.dump(value) in await self._index.get(self._key.dump(key))

    async def remove(self, key: K, value: V) -> bool:
        await self._binding.ready()
        return bool(await self._index.remove(self._key.dump(key), self._value.dump(value)))

    async def remove_key(self, key: K) -> int:
        await self._binding.ready()
        return await self._index.remove(self._key.dump(key), None)

    async def scan(self) -> AsyncIterator[tuple[K, V]]:
        """Every value with its key, reading the index a segment at a time, the values of a key one after the other.

        Not a snapshot: a value put or removed while the scan runs may be seen or not, and none is seen twice. Only
        the segment being read is held, whatever the size of the multimap.
        """
        async for listed in self._index.scan():
            for raw, values in listed.items():
                key = self._key.load(raw)
                for value in values:
                    yield key, self._value.load(value)


class Queue[T]:
    """FIFO kept in segments under keys of their own, so that an operation writes one segment and not the queue.

    Offers go to the tail segment until it is sealed, polls take from the head segment until it is sealed and empty,
    and the index of the two is asked only when one of them moves. `size` asks every segment in between and `clear`
    empties each of them: neither is atomic, and an item offered while `clear` runs may survive it. A lost poll or
    drain response can lose removed items to the caller. A segment drained for good is deleted once it idles, so
    only the segments between the head and the tail stay.
    """

    def __init__(self, binding: Binding, value: type[T]) -> None:
        self._binding = binding
        self._value = Value(value, binding.system)
        self._index = binding.ref(queue.actor)
        # Where the head and the tail were last seen. They can only lag behind the index, and a segment they left is
        # sealed, which sends the call to the index to catch up.
        self._head = 0
        self._tail = 0
        self._segments: dict[int, Ref[queue_segment.Message]] = {}

    async def offer(self, value: T) -> None:
        await self._binding.ready()
        raw = self._value.dump(value)
        while True:
            tail = self._tail
            if await self._segment(tail).ask(queue_segment.Offer, raw, self._index, tail):
                return
            await self._advance(self._head, tail + 1)

    async def poll(self) -> T | Missing:
        taken = await self._take(1)
        return self._value.load(taken[0]) if taken else MISSING

    async def peek(self) -> T | Missing:
        await self._binding.ready()
        while True:
            head = self._head
            items, sealed = await self._segment(head).ask(queue_segment.Peek, self._index, head)
            if items or not sealed:
                return self._value.load(items[0]) if items else MISSING
            await self._advance(head + 1, self._tail)

    async def size(self) -> int:
        """Count the items segment by segment, with one ask to each of them."""
        await self._binding.ready()
        head, tail = await self._advance(self._head, self._tail)
        return sum(await _each(lambda at: self._segment(at).ask(queue_segment.Size), range(head, tail + 1)))

    async def drain(self, max_items: int) -> list[T]:
        if max_items < 0:
            raise ValueError("max_items must be nonnegative")
        return [self._value.load(raw) for raw in await self._take(max_items)]

    async def clear(self) -> None:
        await self._binding.ready()
        head, tail = await self._advance(self._head, self._tail)
        await _each(lambda at: self._segment(at).ask(queue_segment.Clear), range(head, tail + 1))
        # The segments below the tail are sealed, and now empty for good.
        await self._advance(tail, tail)

    async def _take(self, limit: int) -> list[bytes]:
        """Up to `limit` items from the head on, moving past every segment that is sealed and was emptied."""
        await self._binding.ready()
        taken: list[bytes] = []
        while len(taken) < limit:
            head = self._head
            items, sealed = await self._segment(head).ask(queue_segment.Take, limit - len(taken), self._index, head)
            taken += items
            if len(taken) < limit:
                # The segment is empty now: a sealed one for good, and an open one is the end of the queue.
                if not sealed:
                    break
                await self._advance(head + 1, self._tail)
        return taken

    async def _advance(self, head: int, tail: int) -> tuple[int, int]:
        """Move the index to at least `head` and `tail`, and learn where it is."""
        head, tail = await self._index.ask(queue.Advance, head, tail)
        self._head = max(self._head, head)
        self._tail = max(self._tail, tail)
        self._segments = {at: segment for at, segment in self._segments.items() if at >= self._head}
        return head, tail

    def _segment(self, at: int) -> Ref[queue_segment.Message]:
        """The segment `at`, whose ref is kept: obtaining one asks the owner to start the key."""
        segment = self._segments.get(at)
        if segment is None:
            segment = self._segments[at] = self._binding.ref(queue_segment.actor, at)
        return segment


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
            await _withdraw(self.ref.ask(semaphore.Cancel, id), None)
            raise

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
            if not await _withdraw(self._ref.ask(barrier.Cancel, id), False):
                raise
        except BaseException:
            await _withdraw(self._ref.ask(barrier.Cancel, id), False)
            raise

    async def waiting(self) -> int:
        await self._binding.ready()
        return await self._ref.ask(barrier.Waiting)


def _kept[**P, F](build: Callable[Concatenate["Collections", P], F]) -> Callable[Concatenate["Collections", P], F]:
    """`build`, answering again what it built for equal arguments, for as long as something holds it.

    Each factory keeps its own facades, which is what lets them keep the type the factory answers. They are kept
    weakly, so a facade nobody holds is dropped, and built again when it is asked for again.
    """
    parameters = Signature.from_callable(build)
    built: WeakKeyDictionary[Collections, WeakValueDictionary[tuple[object, ...], F]] = WeakKeyDictionary()

    @wraps(build)
    def kept(collections: "Collections", /, *args: P.args, **kwargs: P.kwargs) -> F:
        arguments = parameters.bind(collections, *args, **kwargs)
        arguments.apply_defaults()
        key: tuple[object, ...] = tuple(arguments.arguments.values())[1:]
        facades = built.setdefault(collections, WeakValueDictionary())
        facade = facades.get(key)
        if facade is None:
            facade = facades[key] = build(collections, *args, **kwargs)
        return facade

    return kept


class Collections:
    """Named collections over the supplied actor system or client.

    Configuration is fixed by the first operation. Mutating calls acknowledge saved state;
    a failed or timed-out call may have committed and is not automatically repeated.

    Asked again with equal arguments, a factory answers the object it built, whose first operation is the only one
    that checks the configuration with the cluster: a `Collections` is meant to be held, one per system. Once an
    operation fixed the configuration of a name, other settings for it raise `ConfigurationError` here without asking
    the cluster. All of that lasts while something holds the object: one nobody holds is dropped, so a collection per
    entity name costs nothing once it is let go, and the one built in its place checks with the cluster again.
    """

    def __init__(self, system: System) -> None:
        self._system = system
        # Weak: a binding lives as long as a facade that holds it.
        self._bindings: WeakValueDictionary[tuple[str, str], Binding] = WeakValueDictionary()

    @_kept
    def counter(
        self,
        name: str,
        *,
        stripes: int = 1,
        replicas: int = 3,
        write: Write = "majority",
    ) -> Counter:
        return Counter(self._table("counter", name, stripes, replicas, write, ()))

    @_kept
    def register[T](
        self,
        name: str,
        *,
        value: type[T],
        replicas: int = 3,
        write: Write = "majority",
    ) -> Register[T]:
        return Register(self._table("register", name, 1, replicas, write, (repr(value),)), value)

    @_kept
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

    @_kept
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

    @_kept
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

    @_kept
    def queue[T](
        self,
        name: str,
        *,
        value: type[T],
        replicas: int = 3,
        write: Write = "majority",
    ) -> Queue[T]:
        return Queue(self._table("queue", name, 1, replicas, write, (repr(value),)), value)

    @_kept
    def semaphore(self, name: str, *, capacity: int, replicas: int = 3) -> Semaphore:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        binding = self._table("semaphore", name, 1, replicas, "majority", (str(capacity),))
        return Semaphore(binding, capacity)

    @_kept
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

    @_kept
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
        """The binding of `name`, which every facade of it held here shares.

        Settings other than those the cluster confirmed for the binding can never be the collection's, so they raise
        here. A binding not confirmed yet is replaced: the first operation is what decides.
        """
        held = self._bindings.get((kind, name))
        if held is not None and held.settings == (kind, replicas, write, shards, *signature):
            return held
        if held is not None and held.confirmed:
            raise ConfigurationError(f"incompatible configuration for collection {name!r}")
        binding = self._bindings[kind, name] = Binding(
            self._system,
            kind,
            name,
            replicas=replicas,
            write=write,
            shards=shards,
            signature=signature,
        )
        return binding
