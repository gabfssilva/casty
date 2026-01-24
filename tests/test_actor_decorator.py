from __future__ import annotations

import pytest
from casty import actor, Mailbox


def test_actor_default_is_local():
    @actor
    async def local_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    assert local_actor.__replication_config__ is None


def test_actor_clustered_true():
    @actor(clustered=True)
    async def clustered_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    config = clustered_actor.__replication_config__
    assert config is not None
    assert config.clustered is True
    assert config.replicas == 1
    assert config.write_quorum == "async"


def test_actor_with_replicas():
    @actor(replicas=3)
    async def replicated_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    config = replicated_actor.__replication_config__
    assert config is not None
    assert config.clustered is True
    assert config.replicas == 3


def test_actor_with_write_quorum():
    @actor(replicas=5, write_quorum="quorum")
    async def quorum_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    config = quorum_actor.__replication_config__
    assert config.replicas == 5
    assert config.write_quorum == "quorum"
    assert config.resolve_write_quorum() == 3


def test_actor_with_write_quorum_all():
    @actor(replicas=3, write_quorum="all")
    async def all_quorum_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    config = all_quorum_actor.__replication_config__
    assert config.replicas == 3
    assert config.write_quorum == "all"
    assert config.resolve_write_quorum() == 3


def test_actor_with_write_quorum_int():
    @actor(replicas=5, write_quorum=2)
    async def int_quorum_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    config = int_quorum_actor.__replication_config__
    assert config.replicas == 5
    assert config.write_quorum == 2
    assert config.resolve_write_quorum() == 2


def test_actor_clustered_with_custom_replicas():
    @actor(clustered=True, replicas=5)
    async def custom_replicas_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass
    config = custom_replicas_actor.__replication_config__
    assert config.clustered is True
    assert config.replicas == 5
