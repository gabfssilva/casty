from __future__ import annotations

import abc
import typing
from collections.abc import AsyncIterator
from types import TracebackType

from casty.actors import registry as actor_registry
from casty.actors.proxy import ActorProxy
from casty.actors.registry import ActorInfo, MethodInfo
from casty.collections import counter as collections_counter
from casty.collections import map as collections_map
from casty.collections import multimap as collections_multimap
from casty.collections import queue as collections_queue
from casty.collections import register as collections_register
from casty.collections import semaphore as collections_semaphore
from casty.collections import set as collections_set
from casty.collections.counter import Counter
from casty.collections.map import Map
from casty.collections.multimap import MultiMap
from casty.collections.queue import Queue
from casty.collections.register import Register
from casty.collections.semaphore import Lock, Semaphore
from casty.collections.set import Set


class ActorSystem(abc.ABC):
    """Anything that can route an actor call. Hands out typed proxies and maps
    on top of `call_actor`, and is an async context manager that `close()`s on
    exit. `Node` routes across a cluster; `Local` dispatches in-process."""

    def actor[T](self, cls: type[T], key: str) -> T:
        info = actor_registry.info_of(cls)
        if info is None:
            raise TypeError(f"{cls.__qualname__} is not a @casty.actor")
        return typing.cast(T, ActorProxy(self, info, key))

    def service[T](self, cls: type[T]) -> T:
        """Proxy to a `@casty.service` (spec 08). No key: a service is stateless,
        so any activation serves — a Node dispatches to its own, a Client picks a
        member."""
        info = actor_registry.info_of(cls)
        if info is None or info.kind != "service":
            raise TypeError(f"{cls.__qualname__} is not a @casty.service")
        return typing.cast(T, ActorProxy(self, info, actor_registry.SERVICE_KEY))

    def map[K, V](
        self,
        name: str,
        *,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
        shards: int = 32,
    ) -> Map[K, V]:
        info = collections_map.shard_info(replicas, write, read)
        return Map(self, name, info, shards)

    def set[T](
        self,
        name: str,
        *,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
        shards: int = 32,
    ) -> Set[T]:
        info = collections_set.shard_info(replicas, write, read)
        return Set(self, name, info, shards)

    def multimap[K, V](
        self,
        name: str,
        *,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
        shards: int = 32,
    ) -> MultiMap[K, V]:
        info = collections_multimap.shard_info(replicas, write, read)
        return MultiMap(self, name, info, shards)

    def counter(
        self,
        name: str,
        *,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
        stripes: int = 32,
    ) -> Counter:
        info = collections_counter.shard_info(replicas, write, read)
        return Counter(self, name, info, stripes)

    def register[T](
        self,
        name: str,
        *,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
    ) -> Register[T]:
        info = collections_register.shard_info(replicas, write, read)
        return Register(self, name, info, 1)

    def queue[T](
        self,
        name: str,
        *,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
    ) -> Queue[T]:
        info = collections_queue.shard_info(replicas, write, read)
        return Queue(self, name, info, 1)

    def semaphore(
        self,
        name: str,
        *,
        capacity: int,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
    ) -> Semaphore:
        info = collections_semaphore.shard_info(replicas, write, read)
        return Semaphore(self, name, info, capacity)

    def lock(
        self,
        name: str,
        *,
        ttl: float = 30.0,
        timeout: float | None = None,
        replicas: int = 3,
        write: actor_registry.Consistency | int = actor_registry.Consistency.MAJORITY,
        read: actor_registry.Consistency | int = actor_registry.Consistency.ONE,
    ) -> Lock:
        info = collections_semaphore.shard_info(replicas, write, read)
        return Lock(self, name, info, ttl=ttl, timeout=timeout)

    @abc.abstractmethod
    async def call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object: ...

    def stream_out(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> AsyncIterator[object]:
        """Server/duplex streaming (spec 07): route to the owner and iterate the
        elements it yields. Returned synchronously so `async for` can consume it."""
        raise NotImplementedError("streaming is not supported by this actor system")

    async def stream_in(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        chain: list[str],
    ) -> object:
        """Client-streaming (spec 07): upload the input iterator and await the
        owner's single scalar result."""
        raise NotImplementedError("streaming is not supported by this actor system")

    @abc.abstractmethod
    async def close(self) -> None:
        """Shut this system down and release its resources."""

    async def __aenter__(self) -> typing.Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
