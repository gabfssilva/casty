from __future__ import annotations

from collections.abc import Callable
from typing import Any, TYPE_CHECKING

from casty.actor import Behaviors
from casty.distributed._counter import Counter, counter_entity, persistent_counter_entity
from casty.distributed._dict import Dict, map_entity, persistent_map_entity
from casty.distributed._queue import Queue, queue_entity, persistent_queue_entity
from casty.distributed._set import Set, set_entity, persistent_set_entity

if TYPE_CHECKING:
    from casty.journal import EventJournal
    from casty.ref import ActorRef
    from casty.sharding import ClusteredActorSystem


def _get_or_spawn_region(
    system: ClusteredActorSystem,
    regions: dict[str, ActorRef[Any]],
    key: str,
    factory: Callable[[str], Any],
    shards: int,
) -> ActorRef[Any]:
    if key not in regions:
        regions[key] = system.spawn(
            Behaviors.sharded(factory, num_shards=shards), key
        )
    return regions[key]


class _MapAccessor:
    def __init__(
        self,
        system: ClusteredActorSystem,
        regions: dict[str, ActorRef[Any]],
        factory: Callable[[str], Any],
    ) -> None:
        self._system = system
        self._regions = regions
        self._factory = factory

    def __getitem__[K, V](
        self, params: tuple[type[K], type[V]]
    ) -> Callable[..., Dict[K, V]]:
        system = self._system
        regions = self._regions
        factory = self._factory

        def create(name: str, *, shards: int = 100, timeout: float = 5.0) -> Dict[K, V]:
            region = _get_or_spawn_region(system, regions, f"d-map-{name}", factory, shards)
            return Dict(system=system, region_ref=region, name=name, timeout=timeout)

        return create


class _SetAccessor:
    def __init__(
        self,
        system: ClusteredActorSystem,
        regions: dict[str, ActorRef[Any]],
        factory: Callable[[str], Any],
    ) -> None:
        self._system = system
        self._regions = regions
        self._factory = factory

    def __getitem__[V](self, param: type[V]) -> Callable[..., Set[V]]:
        system = self._system
        regions = self._regions
        factory = self._factory

        def create(name: str, *, shards: int = 100, timeout: float = 5.0) -> Set[V]:
            region = _get_or_spawn_region(system, regions, f"d-set-{name}", factory, shards)
            return Set(system=system, region_ref=region, name=name, timeout=timeout)

        return create


class _QueueAccessor:
    def __init__(
        self,
        system: ClusteredActorSystem,
        regions: dict[str, ActorRef[Any]],
        factory: Callable[[str], Any],
    ) -> None:
        self._system = system
        self._regions = regions
        self._factory = factory

    def __getitem__[V](self, param: type[V]) -> Callable[..., Queue[V]]:
        system = self._system
        regions = self._regions
        factory = self._factory

        def create(name: str, *, shards: int = 100, timeout: float = 5.0) -> Queue[V]:
            region = _get_or_spawn_region(system, regions, f"d-queue-{name}", factory, shards)
            return Queue(system=system, region_ref=region, name=name, timeout=timeout)

        return create


class Distributed:
    """Facade for creating distributed data structures.

    Spawns sharded actor regions lazily, per structure name. Usage::

        d = Distributed(system)
        counter = d.counter("hits", shards=50)
        users = d.map[str, User]("users", shards=10)
        tags = d.set[str]("tags")
        jobs = d.queue[Task]("jobs")

    For persistent (event-sourced) mode::

        d = Distributed(system, journal=journal)
    """

    def __init__(
        self,
        system: ClusteredActorSystem,
        *,
        journal: EventJournal | None = None,
    ) -> None:
        self._system = system
        self._journal = journal
        self._regions: dict[str, ActorRef[Any]] = {}

    def counter(self, name: str, *, shards: int = 100, timeout: float = 5.0) -> Counter:
        """Create a distributed counter."""
        factory = persistent_counter_entity(self._journal) if self._journal is not None else counter_entity
        region = _get_or_spawn_region(
            self._system, self._regions, f"d-counter-{name}", factory, shards
        )
        return Counter(system=self._system, region_ref=region, name=name, timeout=timeout)

    @property
    def map(self) -> _MapAccessor:
        """Create a distributed map via ``d.map[K, V]("name", shards=10)``."""
        factory = persistent_map_entity(self._journal) if self._journal is not None else map_entity
        return _MapAccessor(self._system, self._regions, factory)

    @property
    def set(self) -> _SetAccessor:
        """Create a distributed set via ``d.set[V]("name", shards=10)``."""
        factory = persistent_set_entity(self._journal) if self._journal is not None else set_entity
        return _SetAccessor(self._system, self._regions, factory)

    @property
    def queue(self) -> _QueueAccessor:
        """Create a distributed queue via ``d.queue[V]("name", shards=10)``."""
        factory = persistent_queue_entity(self._journal) if self._journal is not None else queue_entity
        return _QueueAccessor(self._system, self._regions, factory)
