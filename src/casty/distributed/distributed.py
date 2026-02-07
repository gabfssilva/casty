from __future__ import annotations

from collections.abc import Callable
from typing import Any, TYPE_CHECKING

from casty.actor import Behaviors
from casty.distributed.barrier import Barrier, barrier_entity
from casty.distributed.counter import Counter, counter_entity, persistent_counter_entity
from casty.distributed.dict import Dict, map_entity, persistent_map_entity
from casty.distributed.lock import Lock, lock_entity
from casty.distributed.queue import Queue, queue_entity, persistent_queue_entity
from casty.distributed.semaphore import Semaphore, semaphore_entity_factory
from casty.distributed.set import Set, set_entity, persistent_set_entity

if TYPE_CHECKING:
    from casty.journal import EventJournal
    from casty.ref import ActorRef
    from casty.sharding import ClusteredActorSystem


def get_or_spawn_region(
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


class MapAccessor:
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
            region = get_or_spawn_region(system, regions, f"d-map-{name}", factory, shards)
            return Dict(system=system, region_ref=region, name=name, timeout=timeout)

        return create


class SetAccessor:
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
            region = get_or_spawn_region(system, regions, f"d-set-{name}", factory, shards)
            return Set(system=system, region_ref=region, name=name, timeout=timeout)

        return create


class QueueAccessor:
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
            region = get_or_spawn_region(system, regions, f"d-queue-{name}", factory, shards)
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
        region = get_or_spawn_region(
            self._system, self._regions, f"d-counter-{name}", factory, shards
        )
        return Counter(system=self._system, region_ref=region, name=name, timeout=timeout)

    @property
    def map(self) -> MapAccessor:
        """Create a distributed map via ``d.map[K, V]("name", shards=10)``."""
        factory = persistent_map_entity(self._journal) if self._journal is not None else map_entity
        return MapAccessor(self._system, self._regions, factory)

    @property
    def set(self) -> SetAccessor:
        """Create a distributed set via ``d.set[V]("name", shards=10)``."""
        factory = persistent_set_entity(self._journal) if self._journal is not None else set_entity
        return SetAccessor(self._system, self._regions, factory)

    @property
    def queue(self) -> QueueAccessor:
        """Create a distributed queue via ``d.queue[V]("name", shards=10)``."""
        factory = persistent_queue_entity(self._journal) if self._journal is not None else queue_entity
        return QueueAccessor(self._system, self._regions, factory)

    def lock(self, name: str, *, shards: int = 100, timeout: float = 5.0) -> Lock:
        """Create a distributed lock."""
        region = get_or_spawn_region(
            self._system, self._regions, f"d-lock-{name}", lock_entity, shards
        )
        return Lock(system=self._system, region_ref=region, name=name, timeout=timeout)

    def semaphore(
        self, name: str, permits: int, *, shards: int = 100, timeout: float = 5.0
    ) -> Semaphore:
        """Create a distributed semaphore with *permits* concurrent holders."""
        factory = semaphore_entity_factory(permits)
        region = get_or_spawn_region(
            self._system, self._regions, f"d-sem-{name}", factory, shards
        )
        return Semaphore(system=self._system, region_ref=region, name=name, timeout=timeout)

    def barrier(
        self, name: str, *, node_id: str | None = None, shards: int = 10, timeout: float = 60.0
    ) -> Barrier:
        """Create a distributed barrier."""
        nid = node_id or f"{self._system.self_node.host}:{self._system.self_node.port}"
        region = get_or_spawn_region(
            self._system, self._regions, f"d-barrier-{name}", barrier_entity, shards
        )
        return Barrier(system=self._system, region_ref=region, name=name, node_id=nid, timeout=timeout)
