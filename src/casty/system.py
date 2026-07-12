from __future__ import annotations

import abc
import typing
from collections.abc import AsyncIterator
from types import TracebackType

from casty.actors import inspection
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
    """Anything that can route an actor call: hands out typed proxies and
    distributed collections. `Node` and `Client` route across a cluster;
    `local()` dispatches in-process.

    An async context manager that `close()`s on exit. Collection factories
    with the same name and settings address the same distributed object from
    anywhere in the cluster — the (replicas, write, read) triple is part of
    the shard's wire identity, so every caller must pass the same settings
    for the same name.
    """

    def actor[T](self, cls: type[T], key: str) -> T:
        """Typed proxy to the virtual actor `(cls, key)`.

        Creating the proxy costs nothing: activation happens on demand, on
        the node that owns the key's token, at the first call.

        Parameters
        ----------
        cls : type[T]
            A `@casty.actor` class.
        key : str
            The actor's identity within the class.

        Returns
        -------
        T
            A proxy exposing the class's public async methods.

        Raises
        ------
        TypeError
            If `cls` is not a `@casty.actor`.
        """
        info = actor_registry.info_of(cls)
        if info is None:
            raise TypeError(f"{cls.__qualname__} is not a @casty.actor")
        return typing.cast(T, ActorProxy(self, info, key))

    def service[T](self, cls: type[T]) -> T:
        """Typed proxy to a `@casty.service`.

        No key: a service is stateless, so any activation serves — a Node
        dispatches to its own, a Client picks a member.

        Parameters
        ----------
        cls : type[T]
            A `@casty.service` class.

        Returns
        -------
        T
            A proxy exposing the class's public async methods.

        Raises
        ------
        TypeError
            If `cls` is not a `@casty.service`.
        """
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
        """Distributed map. Keys hash across `shards` shard actors.

        Parameters
        ----------
        name : str
            Cluster-wide name of the map.
        replicas : int
            Physical nodes holding each shard's state.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.
        shards : int
            Shard actors the keyspace splits into.

        Returns
        -------
        Map[K, V]
            Typed facade; K and V need no registration, they are encoded
            per operation.
        """
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
        """Distributed set. Items hash across `shards` shard actors.

        Parameters
        ----------
        name : str
            Cluster-wide name of the set.
        replicas : int
            Physical nodes holding each shard's state.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.
        shards : int
            Shard actors the item space splits into.

        Returns
        -------
        Set[T]
            Typed facade; T needs no registration.
        """
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
        """Distributed key -> set-of-values map. All values of a key
        live on one shard, so `get(key)` is a single call.

        Parameters
        ----------
        name : str
            Cluster-wide name of the multimap.
        replicas : int
            Physical nodes holding each shard's state.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.
        shards : int
            Shard actors the keyspace splits into.

        Returns
        -------
        MultiMap[K, V]
            Typed facade; K and V need no registration.
        """
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
        """Striped distributed counter. Writes rotate over `stripes`
        shard actors to avoid a hotspot; reads sum the fan-out.

        Parameters
        ----------
        name : str
            Cluster-wide name of the counter.
        replicas : int
            Physical nodes holding each stripe's state.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.
        stripes : int
            Shard actors the value splits into.

        Returns
        -------
        Counter
            The counter facade.
        """
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
        """Distributed atomic reference. One owner actor serializes
        writes, so compare-and-set is single-writer correct.

        Parameters
        ----------
        name : str
            Cluster-wide name of the register.
        replicas : int
            Physical nodes holding the value.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.

        Returns
        -------
        Register[T]
            Typed facade; T needs no registration.
        """
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
        """Distributed FIFO queue. One owner actor keeps order; a
        single queue does not scale across nodes — partition into many named
        queues instead.

        Parameters
        ----------
        name : str
            Cluster-wide name of the queue.
        replicas : int
            Physical nodes holding the queue's state.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.

        Returns
        -------
        Queue[T]
            Typed facade; T needs no registration.
        """
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
        """Distributed counting semaphore. Permits are leases with a
        TTL and a fencing token; see `Semaphore.acquire`.

        Parameters
        ----------
        name : str
            Cluster-wide name of the semaphore.
        capacity : int
            Total permits. Callers of the same name must agree on it — it is
            client-supplied, not stored.
        replicas : int
            Physical nodes holding the lease table.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.

        Returns
        -------
        Semaphore
            The semaphore facade.
        """
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
        """Distributed mutual-exclusion lock — a semaphore of
        capacity 1. Async context manager: acquires on entry, releases on
        exit.

        Parameters
        ----------
        name : str
            Cluster-wide name of the lock.
        ttl : float
            Default lease seconds; the holder must `renew` before expiry or
            the lock is reclaimed.
        timeout : float | None
            Default seconds `acquire` (and `async with`) waits for the lock.
            None waits forever.
        replicas : int
            Physical nodes holding the lease.
        write : Consistency | int
            Acks required per mutation, resolved against `replicas`.
        read : Consistency | int
            Reserved for read paths; part of the shard identity today.

        Returns
        -------
        Lock
            The lock facade.
        """
        info = collections_semaphore.shard_info(replicas, write, read)
        return Lock(self, name, info, ttl=ttl, timeout=timeout)

    @abc.abstractmethod
    def spy(
        self,
        actor: type | str | None = None,
        *,
        key: str = "*",
        scope: typing.Literal["local", "cluster"] = "local",
        payloads: bool = False,
    ) -> inspection.Subscription:
        """Subscribe to actor lifecycle and handler events (spec 11).

        Observation is passive and best-effort: buffers drop on overflow and
        the loss surfaces in-band as `inspection.Lag`. It never slows an
        observed actor.

        Parameters
        ----------
        actor : type | str | None
            A `@casty.actor` class, a wire name, or a wire-name glob; None
            matches every actor.
        key : str
            `fnmatch` glob on the actor key.
        scope : {"local", "cluster"}
            "local" sees only activations hosted on this node (zero wire);
            "cluster" dials every member and merges. A `Client` accepts only
            "cluster"; `local()` treats both the same.
        payloads : bool
            Ship handler args/results as encoded bytes on the events; decode
            with `inspection.decode`.

        Returns
        -------
        Subscription
            Async context manager and async iterator of `inspection` events.
        """

    @abc.abstractmethod
    async def state_of(self, actor: type, key: str) -> dict[str, object] | None:
        """Point-in-time read of an activation's live state, without the
        mailbox (spec 11 §8). A debugging read, not a consistency primitive —
        it can observe a handler's partial writes across its awaits.

        Parameters
        ----------
        actor : type
            A `@casty.actor` class.
        key : str
            The actor's key.

        Returns
        -------
        dict[str, object] | None
            The serializable state fields (`transient()` and `paged()`
            excluded), or None when `(actor, key)` is not currently activated.
        """

    @abc.abstractmethod
    async def _call_actor(
        self, info: ActorInfo, key: str, method: MethodInfo, args: list[object], chain: list[str]
    ) -> object: ...

    def _stream_out(
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

    async def _stream_in(
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
