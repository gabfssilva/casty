# tests/test_cluster_client.py
"""Integration tests for ClusterClient — topology-aware routing without membership."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import Behaviors, ActorRef, Behavior, ActorSystem, MemberStatus, Member
from casty import ServiceKey, ServiceEntry
from casty.client.client import (
    ClusterClient,
    NodeFailed,
    SubscriptionTimeout,
    client_proxy_behavior,
    topology_subscriber,
)
from casty.cluster.state import NodeAddress
from casty.cluster.coordinator import ShardLocation
from casty.cluster.system import ClusteredActorSystem
from casty.cluster.envelope import ShardEnvelope
from casty.cluster.topology import SubscribeTopology, TopologySnapshot
from casty.core.streams import (
    GetSink,
    GetSource,
    SinkRef,
    SourceRef,
    StreamCompleted,
    StreamDemand,
    StreamElement,
    StreamProducerMsg,
    Subscribe,
    stream_consumer,
    stream_producer,
)


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetBalance


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    def active(value: int = 0) -> Behavior[CounterMsg]:
        async def receive(_ctx: Any, msg: Any) -> Any:
            match msg:
                case Increment(amount=amount):
                    return active(value + amount)
                case GetBalance(reply_to=reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active()


async def test_client_connects_and_receives_topology() -> None:
    """ClusterClient receives topology snapshot from cluster contact point."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ):
            await asyncio.sleep(1.0)


async def test_client_routes_tell_to_cluster() -> None:
    """ClusterClient routes ShardEnvelope.tell() to the correct cluster node."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            counter = client.entity_ref("counters", num_shards=10)
            counter.tell(ShardEnvelope("alice", Increment(amount=5)))
            counter.tell(ShardEnvelope("alice", Increment(amount=3)))
            await asyncio.sleep(0.5)

            result = await system.ask(
                system.lookup("counters"),  # type: ignore[arg-type]
                lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            assert result == 8


async def test_client_ask_returns_reply() -> None:
    """ClusterClient.ask() sends message and receives reply via TCP."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            counter = client.entity_ref("counters", num_shards=10)
            counter.tell(ShardEnvelope("bob", Increment(amount=10)))
            counter.tell(ShardEnvelope("bob", Increment(amount=7)))
            await asyncio.sleep(0.5)

            result = await client.ask(
                counter,
                lambda r: ShardEnvelope("bob", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            assert result == 17


async def test_client_routes_to_multiple_entities() -> None:
    """ClusterClient routes to different entities independently."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            counter = client.entity_ref("counters", num_shards=10)
            counter.tell(ShardEnvelope("alice", Increment(amount=1)))
            counter.tell(ShardEnvelope("bob", Increment(amount=100)))
            counter.tell(ShardEnvelope("alice", Increment(amount=2)))
            await asyncio.sleep(0.5)

            alice = await client.ask(
                counter,
                lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            bob = await client.ask(
                counter,
                lambda r: ShardEnvelope("bob", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            assert alice == 3
            assert bob == 100


async def test_client_with_two_node_cluster() -> None:
    """ClusterClient works with a multi-node cluster, routing to the correct node."""
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
            await system_a.wait_for(2, timeout=10.0)

            system_a.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            system_b.spawn(
                Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
                "counters",
            )
            await asyncio.sleep(0.5)

            async with ClusterClient(
                contact_points=[("127.0.0.1", port_a)],
                system_name="cluster",
            ) as client:
                await asyncio.sleep(1.0)

                counter = client.entity_ref("counters", num_shards=10)
                counter.tell(ShardEnvelope("entity-1", Increment(amount=10)))
                counter.tell(ShardEnvelope("entity-2", Increment(amount=20)))
                await asyncio.sleep(0.5)

                r1 = await client.ask(
                    counter,
                    lambda r: ShardEnvelope("entity-1", GetBalance(reply_to=r)),
                    timeout=5.0,
                )
                r2 = await client.ask(
                    counter,
                    lambda r: ShardEnvelope("entity-2", GetBalance(reply_to=r)),
                    timeout=5.0,
                )
                assert r1 == 10
                assert r2 == 20


async def test_client_entity_ref_is_cached() -> None:
    """Calling entity_ref() twice returns the same proxy."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            ref1 = client.entity_ref("counters", num_shards=10)
            ref2 = client.entity_ref("counters", num_shards=10)
            assert ref1.id == ref2.id


# ---------------------------------------------------------------------------
# Unit tests for stale allocation rejection and subscriber reconnection
# ---------------------------------------------------------------------------


class FakeRemoteTransport:
    """Minimal fake that records make_ref calls and routes nothing."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    def make_ref(self, addr: Any) -> ActorRef[Any]:
        class _Capture:
            def __init__(self, key: str, sent: list[tuple[str, Any]]) -> None:
                self._key = key
                self._sent = sent

            def tell(self, msg: Any) -> None:
                self._sent.append((self._key, msg))

        cap = _Capture(f"{addr.host}:{addr.port}{addr.path}", self.sent)
        return cap  # type: ignore[return-value]


async def test_proxy_rejects_stale_allocation_after_node_failure() -> None:
    """After NodeFailed for node-A, ShardLocation pointing to node-A is rejected."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    fake_transport = FakeRemoteTransport()

    async with ActorSystem(name="test") as system:
        proxy = system.spawn(
            client_proxy_behavior(
                shard_name="counters",
                num_shards=10,
                remote_transport=fake_transport,  # type: ignore[arg-type]
                system_name="test-cluster",
            ),
            "proxy",
        )
        await asyncio.sleep(0.1)

        # Send a TopologySnapshot with node-A as leader so the proxy has a leader
        proxy.tell(
            TopologySnapshot(
                members=frozenset(
                    {
                        Member(
                            address=node_a,
                            status=MemberStatus.up,
                            roles=frozenset(),
                            id="a",
                        ),
                        Member(
                            address=node_b,
                            status=MemberStatus.up,
                            roles=frozenset(),
                            id="b",
                        ),
                    }
                ),
                leader=node_a,
                shard_allocations={},
                allocation_epoch=1,
            )
        )
        await asyncio.sleep(0.1)

        # Node-A fails via TCP
        proxy.tell(NodeFailed(host="10.0.0.1", port=25520))
        await asyncio.sleep(0.1)

        # Coordinator responds with stale allocation pointing to dead node-A
        proxy.tell(ShardLocation(shard_id=3, node=node_a))
        await asyncio.sleep(0.1)

        # The stale allocation should be rejected — no route to node-A
        routed_to_a = [
            (path, msg)
            for path, msg in fake_transport.sent
            if "10.0.0.1" in path and isinstance(msg, ShardEnvelope)
        ]
        assert routed_to_a == [], f"Should not route to failed node-A: {routed_to_a}"

        # Send an envelope that maps to shard 3, then resolve it to node-B
        from casty.cluster import entity_shard

        target_id = next(
            eid for i in range(1000) if entity_shard(eid := f"e-{i}", 10) == 3
        )
        proxy.tell(ShardEnvelope(target_id, Increment(amount=1)))
        await asyncio.sleep(0.1)

        # ShardLocation from coordinator pointing to node-B should be accepted
        proxy.tell(ShardLocation(shard_id=3, node=node_b))
        await asyncio.sleep(0.1)

        # Verify the buffered envelope was routed to node-B's region
        routed_to_b = [
            (path, msg)
            for path, msg in fake_transport.sent
            if "10.0.0.2" in path and isinstance(msg, ShardEnvelope)
        ]
        assert len(routed_to_b) > 0, "Should route to healthy node-B"


async def test_proxy_clears_failed_nodes_on_healthy_topology() -> None:
    """TopologySnapshot showing a previously failed node as healthy clears it."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    fake_transport = FakeRemoteTransport()

    async with ActorSystem(name="test") as system:
        proxy = system.spawn(
            client_proxy_behavior(
                shard_name="counters",
                num_shards=10,
                remote_transport=fake_transport,  # type: ignore[arg-type]
                system_name="test-cluster",
            ),
            "proxy",
        )
        await asyncio.sleep(0.1)

        # Give it a leader
        proxy.tell(
            TopologySnapshot(
                members=frozenset(
                    {
                        Member(
                            address=node_a,
                            status=MemberStatus.up,
                            roles=frozenset(),
                            id="a",
                        ),
                    }
                ),
                leader=node_a,
                shard_allocations={},
                allocation_epoch=1,
            )
        )
        await asyncio.sleep(0.1)

        # Node-A fails
        proxy.tell(NodeFailed(host="10.0.0.1", port=25520))
        await asyncio.sleep(0.1)

        # Topology snapshot shows node-A is back and healthy
        proxy.tell(
            TopologySnapshot(
                members=frozenset(
                    {
                        Member(
                            address=node_a,
                            status=MemberStatus.up,
                            roles=frozenset(),
                            id="a",
                        ),
                    }
                ),
                leader=node_a,
                shard_allocations={},
                allocation_epoch=2,
                unreachable=frozenset(),
            )
        )
        await asyncio.sleep(0.1)

        # Now ShardLocation pointing to node-A should be accepted (not rejected)
        proxy.tell(ShardLocation(shard_id=5, node=node_a))
        await asyncio.sleep(0.1)

        # The allocation should be cached — send an envelope for shard 5 to verify routing
        # We need to find which entity_id maps to shard 5 with num_shards=10
        from casty.cluster import entity_shard

        target_id = next(
            eid for i in range(1000) if entity_shard(eid := f"e-{i}", 10) == 5
        )
        proxy.tell(ShardEnvelope(target_id, Increment(amount=1)))
        await asyncio.sleep(0.1)

        routed = [
            (path, msg)
            for path, msg in fake_transport.sent
            if "10.0.0.1" in path and isinstance(msg, ShardEnvelope)
        ]
        assert len(routed) > 0, "Should route to node-A after it healed"


async def test_subscriber_reconnects_on_timeout() -> None:
    """Subscriber rotates to next contact point on SubscriptionTimeout."""
    sent_messages: list[tuple[str, Any]] = []

    class _TrackingTransport:
        def make_ref(self, addr: Any) -> Any:
            class _Ref:
                def __init__(self, path: str, host: str, port: int) -> None:
                    self._path = path
                    self._host = host
                    self._port = port

                def tell(self, msg: Any) -> None:
                    sent_messages.append(
                        (f"{self._host}:{self._port}{self._path}", msg)
                    )

            return _Ref(addr.path, addr.host, addr.port)

    async with ActorSystem(name="test") as system:
        sub_ref = system.spawn(
            topology_subscriber(
                contact_points=[("10.0.0.1", 25520), ("10.0.0.2", 25520)],
                remote_transport=_TrackingTransport(),  # type: ignore[arg-type]
                system_name="test-cluster",
                proxies=[],
            ),
            "sub",
        )
        await asyncio.sleep(0.1)

        # Initial subscription should go to first contact point
        initial_subs = [
            (addr, msg)
            for addr, msg in sent_messages
            if isinstance(msg, SubscribeTopology) and "10.0.0.1" in addr
        ]
        assert len(initial_subs) == 1, "Should subscribe to first contact point"

        # Simulate timeout — subscriber should reconnect to second contact point
        sub_ref.tell(SubscriptionTimeout())
        await asyncio.sleep(0.1)

        reconnect_subs = [
            (addr, msg)
            for addr, msg in sent_messages
            if isinstance(msg, SubscribeTopology) and "10.0.0.2" in addr
        ]
        assert len(reconnect_subs) == 1, "Should subscribe to second contact point"


async def test_subscriber_resets_liveness_on_snapshot() -> None:
    """Receiving TopologySnapshot prevents reconnection timeout."""
    sent_messages: list[tuple[str, Any]] = []

    class _TrackingTransport:
        def make_ref(self, addr: Any) -> Any:
            class _Ref:
                def __init__(self, path: str, host: str, port: int) -> None:
                    self._path = path
                    self._host = host
                    self._port = port

                def tell(self, msg: Any) -> None:
                    sent_messages.append(
                        (f"{self._host}:{self._port}{self._path}", msg)
                    )

            return _Ref(addr.path, addr.host, addr.port)

    import casty.client.client as client_mod

    original_timeout = client_mod.SUBSCRIPTION_TIMEOUT
    client_mod.SUBSCRIPTION_TIMEOUT = 0.5  # Short timeout for testing

    try:
        async with ActorSystem(name="test") as system:
            sub_ref = system.spawn(
                topology_subscriber(
                    contact_points=[("10.0.0.1", 25520), ("10.0.0.2", 25520)],
                    remote_transport=_TrackingTransport(),  # type: ignore[arg-type]
                    system_name="test-cluster",
                    proxies=[],
                ),
                "sub",
            )
            await asyncio.sleep(0.1)

            # Send a topology snapshot to reset the liveness timer
            sub_ref.tell(
                TopologySnapshot(
                    members=frozenset(),
                    leader=None,
                    shard_allocations={},
                    allocation_epoch=1,
                )
            )
            await asyncio.sleep(0.3)

            # Send another snapshot (within the 0.5s window)
            sub_ref.tell(
                TopologySnapshot(
                    members=frozenset(),
                    leader=None,
                    shard_allocations={},
                    allocation_epoch=2,
                )
            )
            await asyncio.sleep(0.3)

            # No reconnection should have happened — only initial subscription
            subscribe_msgs = [
                (addr, msg)
                for addr, msg in sent_messages
                if isinstance(msg, SubscribeTopology)
            ]
            assert len(subscribe_msgs) == 1, (
                f"Should only have initial subscription, got {len(subscribe_msgs)}"
            )
    finally:
        client_mod.SUBSCRIPTION_TIMEOUT = original_timeout


# ---------------------------------------------------------------------------
# Unit tests for ClusterClient.lookup()
# ---------------------------------------------------------------------------


async def test_client_lookup_returns_empty_listing_without_topology() -> None:
    """lookup() before receiving any snapshot returns an empty Listing."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            key: ServiceKey[int] = ServiceKey("my-service")
            listing = client.lookup(key)
            assert listing.key == key
            assert listing.instances == frozenset()


async def test_client_lookup_returns_matching_services() -> None:
    """lookup() returns Listing with refs built from registry entries."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            # Simulate receiving a snapshot with registry entries
            client._on_topology_update(
                TopologySnapshot(
                    members=frozenset(
                        {
                            Member(
                                address=node_a,
                                status=MemberStatus.up,
                                roles=frozenset(),
                                id="a",
                            ),
                            Member(
                                address=node_b,
                                status=MemberStatus.up,
                                roles=frozenset(),
                                id="b",
                            ),
                        }
                    ),
                    leader=node_a,
                    shard_allocations={},
                    allocation_epoch=1,
                    registry=frozenset(
                        {
                            ServiceEntry(
                                key="counter", node=node_a, path="/user/counter-1"
                            ),
                            ServiceEntry(
                                key="counter", node=node_b, path="/user/counter-2"
                            ),
                        }
                    ),
                )
            )

            key: ServiceKey[int] = ServiceKey("counter")
            listing = client.lookup(key)
            assert listing.key == key
            assert len(listing.instances) == 2

            nodes = {inst.node for inst in listing.instances}
            assert nodes == {node_a, node_b}


async def test_client_lookup_filters_by_key() -> None:
    """lookup() only returns entries matching the requested key."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)

    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            client._on_topology_update(
                TopologySnapshot(
                    members=frozenset(
                        {
                            Member(
                                address=node_a,
                                status=MemberStatus.up,
                                roles=frozenset(),
                                id="a",
                            ),
                        }
                    ),
                    leader=node_a,
                    shard_allocations={},
                    allocation_epoch=1,
                    registry=frozenset(
                        {
                            ServiceEntry(
                                key="counter", node=node_a, path="/user/counter"
                            ),
                            ServiceEntry(
                                key="logger", node=node_a, path="/user/logger"
                            ),
                            ServiceEntry(
                                key="counter", node=node_a, path="/user/counter-2"
                            ),
                        }
                    ),
                )
            )

            counter_listing = client.lookup(ServiceKey("counter"))
            assert len(counter_listing.instances) == 2

            logger_listing = client.lookup(ServiceKey("logger"))
            assert len(logger_listing.instances) == 1

            missing_listing = client.lookup(ServiceKey("nonexistent"))
            assert len(missing_listing.instances) == 0


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


type PingMsg = Ping


def ping_actor() -> Behavior[PingMsg]:
    async def receive(_ctx: Any, msg: Any) -> Any:
        match msg:
            case Ping(reply_to=reply_to):
                reply_to.tell("pong")
                return Behaviors.same()
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


async def test_client_lookup_discovers_cluster_service() -> None:
    """Integration: lookup() finds a discoverable actor in a real cluster."""
    key: ServiceKey[PingMsg] = ServiceKey("ping-service")

    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        system.spawn(
            Behaviors.discoverable(ping_actor(), key=key),
            "ping",
        )
        await asyncio.sleep(0.5)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            listing = client.lookup(key)
            assert len(listing.instances) == 1

            instance = next(iter(listing.instances))
            result = await client.ask(
                instance.ref,
                lambda r: Ping(reply_to=r),
                timeout=5.0,
            )
            assert result == "pong"


async def test_client_routes_through_address_map() -> None:
    """ClusterClient with address_map routes via mapped addresses."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        real_port = system.self_node.port

        system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        # Map a fake logical address to the real cluster port
        async with ClusterClient(
            contact_points=[("10.99.99.1", 25520)],
            system_name="cluster",
            address_map={
                ("10.99.99.1", 25520): ("127.0.0.1", real_port),
            },
        ) as client:
            await asyncio.sleep(1.0)

            counter = client.entity_ref("counters", num_shards=10)
            counter.tell(ShardEnvelope("bob", Increment(amount=7)))
            await asyncio.sleep(0.5)

            result = await system.ask(
                system.lookup("counters"),  # type: ignore[arg-type]
                lambda r: ShardEnvelope("bob", GetBalance(reply_to=r)),
                timeout=3.0,
            )
            assert result == 7


async def test_client_ask_through_address_map() -> None:
    """ClusterClient.ask() works when routing through address_map."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        real_port = system.self_node.port

        system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        async with ClusterClient(
            contact_points=[("10.99.99.1", 25520)],
            system_name="cluster",
            address_map={
                ("10.99.99.1", 25520): ("127.0.0.1", real_port),
            },
        ) as client:
            await asyncio.sleep(1.0)

            counter = client.entity_ref("counters", num_shards=10)
            counter.tell(ShardEnvelope("carol", Increment(amount=11)))
            await asyncio.sleep(0.5)

            result = await client.ask(
                counter,
                lambda r: ShardEnvelope("carol", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            assert result == 11


async def test_client_ask_with_advertised_host_port() -> None:
    """ClusterClient.ask() works with advertised_host/port (SSH tunnel scenario)."""
    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        real_port = system.self_node.port

        system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(0.3)

        async with ClusterClient(
            contact_points=[("127.0.0.1", real_port)],
            system_name="cluster",
            client_host="127.0.0.1",
            client_port=0,
            advertised_host="127.0.0.1",
            advertised_port=9999,
        ) as client:
            await asyncio.sleep(1.0)

            counter = client.entity_ref("counters", num_shards=10)
            counter.tell(ShardEnvelope("dave", Increment(amount=42)))
            await asyncio.sleep(0.5)

            result = await client.ask(
                counter,
                lambda r: ShardEnvelope("dave", GetBalance(reply_to=r)),
                timeout=5.0,
            )
            assert result == 42


async def test_client_consumes_stream_from_cluster() -> None:
    """ClusterClient spawns a local stream_consumer and iterates a remote stream."""
    key: ServiceKey[StreamProducerMsg[int]] = ServiceKey(name="producer")

    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        producer = system.spawn(
            Behaviors.discoverable(stream_producer(), key=key), "producer"
        )
        await asyncio.sleep(0.5)

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=5.0
        )

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            listing = client.lookup(key)
            assert len(listing.instances) >= 1
            remote_producer = next(iter(listing.instances)).ref

            consumer = client.spawn(
                stream_consumer(remote_producer, timeout=5.0), "consumer"
            )
            await asyncio.sleep(0.5)

            source: SourceRef[int] = await client.ask(
                consumer, lambda r: GetSource(reply_to=r), timeout=5.0
            )

            results: list[int] = []

            async def consume() -> None:
                async for item in source:
                    results.append(item)

            consume_task = asyncio.create_task(consume())

            await sink.put(1)
            await sink.put(2)
            await sink.put(3)
            await sink.complete()

            await asyncio.wait_for(consume_task, timeout=5.0)
            assert results == [1, 2, 3]


# ---------------------------------------------------------------------------
# Messages for the server-side stream collector
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsumeFrom:
    producer: ActorRef[StreamProducerMsg[int]]


@dataclass(frozen=True)
class GetCollected:
    reply_to: ActorRef[tuple[int, ...]]


type CollectorMsg = ConsumeFrom | GetCollected | StreamElement[int] | StreamCompleted


def stream_collector() -> Behavior[CollectorMsg]:
    """Server-side actor that subscribes to a remote producer and collects elements."""

    def idle() -> Behavior[CollectorMsg]:
        async def receive(_ctx: Any, msg: Any) -> Any:
            match msg:
                case ConsumeFrom(producer=producer):
                    producer.tell(Subscribe(consumer=_ctx.self, demand=16))
                    return collecting(producer, ())
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    def collecting(
        producer: ActorRef[StreamProducerMsg[int]],
        results: tuple[int, ...],
    ) -> Behavior[CollectorMsg]:
        async def receive(_ctx: Any, msg: Any) -> Any:
            match msg:
                case StreamElement(element=element):
                    producer.tell(StreamDemand(n=1))
                    return collecting(producer, (*results, element))
                case StreamCompleted():
                    return done(results)
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    def done(results: tuple[int, ...]) -> Behavior[CollectorMsg]:
        async def receive(_ctx: Any, msg: Any) -> Any:
            match msg:
                case GetCollected(reply_to=reply_to):
                    reply_to.tell(results)
                    return Behaviors.same()
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return idle()


async def test_client_produces_stream_for_cluster() -> None:
    """Client-side producer feeds a server-side consumer over TCP."""
    collector_key: ServiceKey[CollectorMsg] = ServiceKey(name="collector")

    async with ClusteredActorSystem(
        name="cluster",
        host="127.0.0.1",
        port=0,
        node_id="node-1",
    ) as system:
        port = system.self_node.port

        system.spawn(
            Behaviors.discoverable(stream_collector(), key=collector_key),
            "collector",
        )
        await asyncio.sleep(0.5)

        async with ClusterClient(
            contact_points=[("127.0.0.1", port)],
            system_name="cluster",
        ) as client:
            await asyncio.sleep(1.0)

            producer = client.spawn(stream_producer(), "producer")
            await asyncio.sleep(0.1)

            sink: SinkRef[int] = await client.ask(
                producer, lambda r: GetSink(reply_to=r), timeout=5.0
            )

            listing = client.lookup(collector_key)
            assert len(listing.instances) >= 1
            remote_collector = next(iter(listing.instances)).ref

            remote_collector.tell(ConsumeFrom(producer=producer))
            await asyncio.sleep(0.5)

            await sink.put(10)
            await sink.put(20)
            await sink.put(30)
            await sink.complete()
            await asyncio.sleep(1.0)

            results: tuple[int, ...] = await client.ask(
                remote_collector,
                lambda r: GetCollected(reply_to=r),
                timeout=5.0,
            )
            assert results == (10, 20, 30)
