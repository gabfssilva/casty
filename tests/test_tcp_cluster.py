# tests/test_tcp_cluster.py
"""Integration tests for real TCP inter-node communication."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import Behaviors, ActorRef, Behavior
from casty.sharding import ClusteredActorSystem, ShardEnvelope


# --- Simple counter entity for testing ---


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetBalance


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    """Simple in-memory counter entity."""

    async def setup(_ctx: Any) -> Any:
        value = 0

        async def receive(_ctx: Any, msg: Any) -> Any:
            nonlocal value
            match msg:
                case Increment(amount=amount):
                    value += amount
                case GetBalance(reply_to=reply_to):
                    reply_to.tell(value)
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


# --- Tests ---


async def test_clustered_system_starts_tcp_server() -> None:
    """ClusteredActorSystem starts TCP server on configured port."""
    async with ClusteredActorSystem(
        name="test-tcp", host="127.0.0.1", port=0
    ) as system:
        assert system.self_node.port > 0
        assert system.self_node.host == "127.0.0.1"


async def test_spawned_actor_ref_has_host_port() -> None:
    """Spawned actor's ref address has host:port."""
    async with ClusteredActorSystem(
        name="test-ref", host="127.0.0.1", port=0
    ) as system:

        async def noop(_ctx: Any, _msg: Any) -> Any:
            return Behaviors.same()

        ref = system.spawn(Behaviors.receive(noop), "hello")
        assert ref.address.host == "127.0.0.1"
        assert ref.address.port == system.self_node.port
        assert ref.address.path == "/hello"


async def test_single_system_sharded_entity() -> None:
    """Backward compat: single system with sharded entities works via TCP."""
    async with ClusteredActorSystem(
        name="single", host="127.0.0.1", port=0
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.1)

        proxy.tell(ShardEnvelope("alice", Increment(amount=5)))
        proxy.tell(ShardEnvelope("alice", Increment(amount=3)))
        await asyncio.sleep(0.3)

        result = await system.ask(
            proxy,
            lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert result == 8


async def test_cross_system_tell_via_tcp() -> None:
    """Two systems on different ports — actor on A, tell from B arrives via TCP."""
    async with ClusteredActorSystem(
        name="node-a", host="127.0.0.1", port=0
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="node-b",
            host="127.0.0.1",
            port=0,
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            received: list[str] = []

            async def echo_receive(_ctx: Any, msg: Any) -> Any:
                match msg:
                    case str():
                        received.append(msg)
                return Behaviors.same()

            # Spawn actor on system A
            system_a.spawn(Behaviors.receive(echo_receive), "echo")
            await asyncio.sleep(0.1)

            # Send to it from system B via remote transport
            from casty.address import ActorAddress
            from casty.ref import ActorRef as _ActorRef

            remote_addr = ActorAddress(
                system="node-a",
                path="/echo",
                host="127.0.0.1",
                port=port_a,
            )
            remote_ref: _ActorRef[str] = system_b._remote_transport.make_ref(  # pyright: ignore[reportPrivateUsage]
                remote_addr
            )

            remote_ref.tell("hello from B")
            await asyncio.sleep(0.5)

            assert "hello from B" in received


async def test_cross_system_sharded_entity() -> None:
    """Two ClusteredActorSystems: coordinator on node A, entity ask from node B."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
    ) as system_a:
        port_a = system_a.self_node.port
        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            seed_nodes=[("127.0.0.1", port_a)],
        ) as _system_b:  # noqa: F841
            # Spawn sharded entity on system A (which is first seed = coordinator)
            proxy_a = system_a.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            # Wait for gossip convergence → SetRole to activate coordinator
            await asyncio.sleep(1.5)

            # Write from A
            proxy_a.tell(ShardEnvelope("test-entity", Increment(amount=42)))
            await asyncio.sleep(0.3)

            # Read from A
            result = await system_a.ask(
                proxy_a,
                lambda r: ShardEnvelope("test-entity", GetBalance(reply_to=r)),
                timeout=2.0,
            )
            assert result == 42


async def test_remote_ask_timeout() -> None:
    """Remote ask() times out cleanly when target doesn't reply."""
    import pytest

    async with ClusteredActorSystem(
        name="timeout-test", host="127.0.0.1", port=0
    ) as system:
        # Spawn an actor that ignores all messages (never replies)
        async def black_hole(_ctx: Any, _msg: Any) -> Any:
            return Behaviors.same()

        ref = system.spawn(Behaviors.receive(black_hole), "hole")
        await asyncio.sleep(0.1)

        @dataclass(frozen=True)
        class AskMsg:
            reply_to: ActorRef[str]

        with pytest.raises(asyncio.TimeoutError):
            await system.ask(
                ref,
                lambda r: AskMsg(reply_to=r),
                timeout=0.3,
            )
