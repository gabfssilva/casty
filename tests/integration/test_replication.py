from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import (
    FAST_CONFIG,
    kill_node,
    owner_of,
    partition,
    start_nodes,
    stop_all,
    wait_view,
)
from tests.integration.cluster import free_port

pytestmark = pytest.mark.asyncio


@casty.actor(name="itest.RCounter", replicas=3, write=casty.MAJORITY)
class RCounter:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value


async def test_state_survives_owner_death() -> None:
    systems = await start_nodes(3)
    try:
        key = "durable"
        assert await systems[0].actor(RCounter, key).add(41) == 41
        assert await systems[0].actor(RCounter, key).add(1) == 42
        owner = owner_of(systems, "itest.RCounter", key)
        survivors = [n for n in systems if n is not owner]
        await kill_node(owner)
        await wait_view(survivors, 2)
        deadline = asyncio.get_running_loop().time() + 5.0
        while True:
            try:
                assert await survivors[0].actor(RCounter, key).read() == 42
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
    finally:
        await stop_all(systems)


async def test_minority_write_fenced_majority_continues() -> None:
    systems = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        key = "fenced"
        assert await systems[0].actor(RCounter, key).add(10) == 10
        owner = owner_of(systems, "itest.RCounter", key)
        majority = [n for n in systems if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)  # majority saw the owner die

        # minority owner: local activation still exists, but no write quorum
        with pytest.raises(QuorumUnavailableError):
            await owner.actor(RCounter, key).add(5)
        # fencing rolled the in-memory state back
        assert await owner.actor(RCounter, key).read() == 10

        # majority side elects a new owner and keeps writing
        assert await majority[0].actor(RCounter, key).add(7) == 17
    finally:
        if heal is not None:
            heal()
        await stop_all(systems)


async def test_stale_owner_recovers_after_heal_without_lost_updates() -> None:
    systems = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        key = "healed"
        assert await systems[0].actor(RCounter, key).add(10) == 10
        owner = owner_of(systems, "itest.RCounter", key)
        majority = [n for n in systems if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)
        assert await majority[0].actor(RCounter, key).add(7) == 17  # committed on majority

        heal()
        heal = None
        await wait_view(systems, 3, timeout=15.0)  # minority rejoins and refutes

        # post-heal the ring is the original one: the stale old owner gets the
        # key back. Its first write must land on the *committed* state (17),
        # never on its stale in-memory 10.
        deadline = asyncio.get_running_loop().time() + 10.0
        while True:
            try:
                assert await owner.actor(RCounter, key).add(1) == 18
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.2)
        assert await majority[0].actor(RCounter, key).read() == 18
    finally:
        if heal is not None:
            heal()
        await stop_all(systems)


async def test_rolling_restart_loses_nothing() -> None:
    systems = await start_nodes(5)
    replaced: list[casty.Node] = []
    try:
        keys = [f"key-{i}" for i in range(20)]
        for i, key in enumerate(keys):
            assert await systems[0].actor(RCounter, key).add(i + 1) == i + 1

        current = list(systems)
        for _ in range(5):
            leaving = current[0]
            rest = current[1:]
            await leaving.close()
            await wait_view(rest, 4)
            replacement = await casty.start(
                f"127.0.0.1:{free_port()}",
                seeds=[rest[0].member.addr],
                config=FAST_CONFIG,
            )
            replaced.append(replacement)
            current = [*rest, replacement]
            await wait_view(current, 5)

        for i, key in enumerate(keys):
            deadline = asyncio.get_running_loop().time() + 10.0
            while True:
                try:
                    assert await current[0].actor(RCounter, key).read() == i + 1
                    break
                except casty.CastyError:
                    if asyncio.get_running_loop().time() > deadline:
                        raise
                    await asyncio.sleep(0.1)
    finally:
        await stop_all([*systems, *replaced])


async def test_activation_without_quorum_fails() -> None:
    systems = await start_nodes(1)
    try:
        with pytest.raises(QuorumUnavailableError):
            await systems[0].actor(RCounter, "lonely").add(1)
    finally:
        await stop_all(systems)
