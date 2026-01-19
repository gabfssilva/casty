import asyncio
import pytest
from casty import ActorSystem
from casty.cluster.clustered_system import ClusteredSystem
from casty.cluster.config import ClusterConfig
from casty.cluster.cache import (
    CacheActor,
    DistributedCache,
    Get,
    Set,
    Delete,
    CacheHit,
    CacheMiss,
    Ok,
)


class TestCacheActor:
    @pytest.mark.asyncio
    async def test_get_empty_cache_returns_miss(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-get-empty")

            result = await cache.ask(Get("nonexistent"))

            assert isinstance(result, CacheMiss)

    @pytest.mark.asyncio
    async def test_set_then_get_returns_value(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-set-get")

            await cache.ask(Set("key1", b"value1", None))
            result = await cache.ask(Get("key1"))

            assert isinstance(result, CacheHit)
            assert result.value == b"value1"

    @pytest.mark.asyncio
    async def test_set_returns_ok(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-set-ok")

            result = await cache.ask(Set("key1", b"value1", None))

            assert isinstance(result, Ok)

    @pytest.mark.asyncio
    async def test_delete_removes_key(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-delete")

            await cache.ask(Set("key1", b"value1", None))
            delete_result = await cache.ask(Delete("key1"))
            get_result = await cache.ask(Get("key1"))

            assert isinstance(delete_result, Ok)
            assert isinstance(get_result, CacheMiss)

    @pytest.mark.asyncio
    async def test_delete_nonexistent_key_returns_ok(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-delete-nonexistent")

            result = await cache.ask(Delete("nonexistent"))

            assert isinstance(result, Ok)

    @pytest.mark.asyncio
    async def test_overwrite_key(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-overwrite")

            await cache.ask(Set("key1", b"value1", None))
            await cache.ask(Set("key1", b"value2", None))
            result = await cache.ask(Get("key1"))

            assert isinstance(result, CacheHit)
            assert result.value == b"value2"

    @pytest.mark.asyncio
    async def test_multiple_keys(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-multiple-keys")

            await cache.ask(Set("key1", b"value1", None))
            await cache.ask(Set("key2", b"value2", None))
            await cache.ask(Set("key3", b"value3", None))

            r1 = await cache.ask(Get("key1"))
            r2 = await cache.ask(Get("key2"))
            r3 = await cache.ask(Get("key3"))

            assert isinstance(r1, CacheHit) and r1.value == b"value1"
            assert isinstance(r2, CacheHit) and r2.value == b"value2"
            assert isinstance(r3, CacheHit) and r3.value == b"value3"

    @pytest.mark.asyncio
    async def test_ttl_expires_key(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-ttl-expires")

            await cache.ask(Set("key1", b"value1", 0.1))

            result_before = await cache.ask(Get("key1"))
            assert isinstance(result_before, CacheHit)

            await asyncio.sleep(0.2)

            result_after = await cache.ask(Get("key1"))
            assert isinstance(result_after, CacheMiss)

    @pytest.mark.asyncio
    async def test_overwrite_cancels_old_ttl(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-overwrite-ttl")

            await cache.ask(Set("key1", b"value1", 0.1))
            await cache.ask(Set("key1", b"value2", None))

            await asyncio.sleep(0.2)

            result = await cache.ask(Get("key1"))
            assert isinstance(result, CacheHit)
            assert result.value == b"value2"

    @pytest.mark.asyncio
    async def test_delete_cancels_ttl(self):
        async with ActorSystem.local() as system:
            cache = await system.actor(CacheActor, name="cache-delete-ttl")

            await cache.ask(Set("key1", b"value1", 0.5))
            await cache.ask(Delete("key1"))

            await cache.ask(Set("key1", b"value2", None))

            await asyncio.sleep(0.6)

            result = await cache.ask(Get("key1"))
            assert isinstance(result, CacheHit)
            assert result.value == b"value2"


class TestDistributedCache:
    @pytest.mark.asyncio
    async def test_create_cache(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19001)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

            assert cache is not None
            assert cache.ref is not None

    @pytest.mark.asyncio
    async def test_set_and_get(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19002)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

            await cache.set("user:1", {"name": "Alice", "age": 30})
            result = await cache.get("user:1")

            assert result == {"name": "Alice", "age": 30}

    @pytest.mark.asyncio
    async def test_get_missing_key_returns_none(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19003)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

            result = await cache.get("nonexistent")

            assert result is None

    @pytest.mark.asyncio
    async def test_delete(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19004)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

            await cache.set("key1", "value1")
            await cache.delete("key1")
            result = await cache.get("key1")

            assert result is None

    @pytest.mark.asyncio
    async def test_various_value_types(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19005)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

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
        async with ClusteredSystem(ClusterConfig(bind_port=19006)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

            await cache.set("key1", "value1", ttl=0.1)

            result_before = await cache.get("key1")
            assert result_before == "value1"

            await asyncio.sleep(0.2)

            result_after = await cache.get("key1")
            assert result_after is None

    @pytest.mark.asyncio
    async def test_overwrite_value(self):
        async with ClusteredSystem(ClusterConfig(bind_port=19007)) as system:
            cache = await DistributedCache.create(system, name="test-cache")

            await cache.set("key1", "value1")
            await cache.set("key1", "value2")

            result = await cache.get("key1")
            assert result == "value2"
