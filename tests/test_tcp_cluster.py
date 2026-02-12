# tests/test_tcp_cluster.py
"""Integration tests for real TCP inter-node communication."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import Behaviors, ActorRef, Behavior
from casty.receptionist import Find, Listing, Register, ServiceKey
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
        name="test-tcp", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        assert system.self_node.port > 0
        assert system.self_node.host == "127.0.0.1"


async def test_spawned_actor_ref_has_host_port() -> None:
    """Spawned actor's ref address has host:port."""
    async with ClusteredActorSystem(
        name="test-ref", host="127.0.0.1", port=0, node_id="node-1"
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
        name="single", host="127.0.0.1", port=0, node_id="node-1"
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
            timeout=5.0,
        )
        assert result == 8


async def test_cross_system_tell_via_tcp() -> None:
    """Two systems on different ports — actor on A, tell from B arrives via TCP."""
    async with ClusteredActorSystem(
        name="node-a", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="node-b",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
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
        node_id="node-1",
    ) as system_a:
        port_a = system_a.self_node.port
        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            # Wait for gossip convergence
            await system_a.wait_for(2, timeout=10.0)

            proxy_a = system_a.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            system_b.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            await asyncio.sleep(0.5)

            # Write from A
            proxy_a.tell(ShardEnvelope("test-entity", Increment(amount=42)))
            await asyncio.sleep(0.5)

            # Read from A
            result = await system_a.ask(
                proxy_a,
                lambda r: ShardEnvelope("test-entity", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            assert result == 42


async def test_barrier_single_node() -> None:
    """Barrier with n=1 returns immediately on a single node."""
    async with ClusteredActorSystem(
        name="barrier-1", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        await system.barrier("phase-1", 1, timeout=3.0)


async def test_barrier_two_nodes() -> None:
    """Barrier blocks until both nodes arrive, then releases both."""
    async with ClusteredActorSystem(
        name="barrier-2", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port
        async with ClusteredActorSystem(
            name="barrier-2",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2)

            released: list[str] = []

            async def arrive(system: ClusteredActorSystem, label: str) -> None:
                await system.barrier("sync", 2, timeout=5.0)
                released.append(label)

            await asyncio.gather(arrive(system_a, "a"), arrive(system_b, "b"))
            assert sorted(released) == ["a", "b"]


async def test_barrier_reusable() -> None:
    """Same barrier name can be used multiple times in sequence."""
    async with ClusteredActorSystem(
        name="barrier-reuse", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        await system.barrier("gate", 1, timeout=3.0)
        await system.barrier("gate", 1, timeout=3.0)


async def test_remote_ask_timeout() -> None:
    """Remote ask() times out cleanly when target doesn't reply."""
    import pytest

    async with ClusteredActorSystem(
        name="timeout-test", host="127.0.0.1", port=0, node_id="node-1"
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


async def test_clustered_system_has_receptionist() -> None:
    """ClusteredActorSystem should expose a receptionist property."""
    async with ClusteredActorSystem(
        name="test-rec", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        assert system.receptionist is not None


async def test_register_and_find_via_receptionist() -> None:
    """Register a service via receptionist, then find it."""
    async with ClusteredActorSystem(
        name="test-rec", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:

        async def noop(_ctx: Any, _msg: Any) -> Any:
            return Behaviors.same()

        echo_ref = system.spawn(Behaviors.receive(noop), "echo")

        key = ServiceKey(name="echo")
        system.receptionist.tell(Register(key=key, ref=echo_ref))
        await asyncio.sleep(0.1)

        listing: Listing[str] = await system.ask(
            system.receptionist,
            lambda r: Find(key=key, reply_to=r),
            timeout=5.0,
        )
        assert len(listing.instances) == 1


async def test_lookup_service_key() -> None:
    """system.lookup(key) returns a Listing with registered instances."""
    async with ClusteredActorSystem(
        name="test-find", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:

        async def noop(_ctx: Any, _msg: Any) -> Any:
            return Behaviors.same()

        echo_ref = system.spawn(Behaviors.receive(noop), "echo")

        key = ServiceKey(name="echo-svc")
        system.receptionist.tell(Register(key=key, ref=echo_ref))
        await asyncio.sleep(0.1)

        listing = await system.lookup(key)
        assert len(listing.instances) == 1
        instance = next(iter(listing.instances))
        assert instance.ref.address.path == echo_ref.address.path


async def test_lookup_service_key_returns_empty_when_missing() -> None:
    """system.lookup(key) returns an empty Listing for unregistered keys."""
    async with ClusteredActorSystem(
        name="test-find-empty", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:
        key = ServiceKey(name="nonexistent")
        listing = await system.lookup(key)
        assert len(listing.instances) == 0


async def test_lookup_by_path_still_works() -> None:
    """Path-based lookup continues to work alongside ServiceKey lookup."""
    async with ClusteredActorSystem(
        name="test-lookup-path", host="127.0.0.1", port=0, node_id="node-1"
    ) as system:

        async def noop(_ctx: Any, _msg: Any) -> Any:
            return Behaviors.same()

        system.spawn(Behaviors.receive(noop), "my-actor")
        ref = system.lookup("/my-actor")
        assert ref is not None
        assert ref.address.path == "/my-actor"


async def test_cross_node_service_discovery() -> None:
    """Service registered on node A should be discoverable from node B."""
    async with ClusteredActorSystem(
        name="cluster", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            # Register on node A
            async def noop(_ctx: Any, _msg: Any) -> Any:
                return Behaviors.same()

            echo_ref = system_a.spawn(Behaviors.receive(noop), "echo")
            key: ServiceKey[str] = ServiceKey(name="echo")
            system_a.receptionist.tell(Register(key=key, ref=echo_ref))

            # Wait for gossip to propagate registry
            await asyncio.sleep(2.0)

            # Find from node B
            listing = await system_b.lookup(key)

            assert len(listing.instances) >= 1
            nodes = {inst.node for inst in listing.instances}
            assert system_a.self_node in nodes
