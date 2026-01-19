import asyncio
import pytest
from casty import ActorSystem
from casty.cluster.clustered_system import ClusteredSystem
from casty.cluster.config import ClusterConfig
from casty.cluster.cache import (
    CacheEntry,
    DistributedCache,
    Get,
    Set,
    Delete,
    Exists,
)


class TestCacheEntry:
    @pytest.mark.asyncio
    async def test_get_empty_entry_returns_none(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-empty")

            result = await entry.ask(Get())

            assert result is None

    @pytest.mark.asyncio
    async def test_set_then_get_returns_value(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-set-get")

            await entry.send(Set(b"value1"))
            result = await entry.ask(Get())

            assert result == b"value1"

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_empty(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-exists-empty")

            result = await entry.ask(Exists())

            assert result is False

    @pytest.mark.asyncio
    async def test_exists_returns_true_after_set(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-exists-set")

            await entry.send(Set(b"value1"))
            result = await entry.ask(Exists())

            assert result is True

    @pytest.mark.asyncio
    async def test_overwrite_value(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-overwrite")

            await entry.send(Set(b"value1"))
            await entry.send(Set(b"value2"))
            result = await entry.ask(Get())

            assert result == b"value2"

    @pytest.mark.asyncio
    async def test_ttl_expires_entry(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-ttl")

            await entry.send(Set(b"value1", ttl=0.1))

            result_before = await entry.ask(Get())
            assert result_before == b"value1"

            await asyncio.sleep(0.2)

            entry2 = await system.actor(CacheEntry, name="entry-ttl")
            result_after = await entry2.ask(Get())
            assert result_after is None

    @pytest.mark.asyncio
    async def test_overwrite_cancels_old_ttl(self):
        async with ActorSystem.local() as system:
            entry = await system.actor(CacheEntry, name="entry-overwrite-ttl")

            await entry.send(Set(b"value1", ttl=0.1))
            await entry.send(Set(b"value2", ttl=None))

            await asyncio.sleep(0.2)

            result = await entry.ask(Get())
            assert result == b"value2"


class TestDistributedCache:
    @pytest.mark.asyncio
    async def test_create_cache(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19001)) as system:
            cache = DistributedCache(system)

            assert cache is not None

    @pytest.mark.asyncio
    async def test_set_and_get(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19002)) as system:
            cache = DistributedCache(system)

            await cache.set("user:1", {"name": "Alice", "age": 30})
            result = await cache.get("user:1")

            assert result == {"name": "Alice", "age": 30}

    @pytest.mark.asyncio
    async def test_get_missing_key_returns_none(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19003)) as system:
            cache = DistributedCache(system)

            result = await cache.get("nonexistent")

            assert result is None

    @pytest.mark.asyncio
    async def test_delete(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19004)) as system:
            cache = DistributedCache(system)

            await cache.set("key1", "value1")
            await cache.delete("key1")
            result = await cache.get("key1")

            assert result is None

    @pytest.mark.asyncio
    async def test_exists(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19005)) as system:
            cache = DistributedCache(system)

            assert await cache.exists("key1") is False
            await cache.set("key1", "value1")
            assert await cache.exists("key1") is True

    @pytest.mark.asyncio
    async def test_various_value_types(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19006)) as system:
            cache = DistributedCache(system)

            await cache.set("string", "hello")
            await cache.set("int", 42)
            await cache.set("float", 3.14)
            await cache.set("list", [1, 2, 3])
            await cache.set("dict", {"nested": {"value": True}})
            await cache.set("none", None)

            assert await cache.get("string") == "hello"
            assert await cache.get("int") == 42
            assert await cache.get("float") == 3.14
            assert await cache.get("list") == [1, 2, 3]
            assert await cache.get("dict") == {"nested": {"value": True}}
            assert await cache.get("none") is None

    @pytest.mark.asyncio
    async def test_ttl(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19007)) as system:
            cache = DistributedCache(system)

            await cache.set("key1", "value1", ttl=0.1)

            result_before = await cache.get("key1")
            assert result_before == "value1"

            await asyncio.sleep(0.2)

            result_after = await cache.get("key1")
            assert result_after is None

    @pytest.mark.asyncio
    async def test_overwrite_value(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19008)) as system:
            cache = DistributedCache(system)

            await cache.set("key1", "value1")
            await cache.set("key1", "value2")

            result = await cache.get("key1")
            assert result == "value2"
