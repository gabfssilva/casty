# tests/test_cluster_client.py
"""Integration tests for ClusterClient — topology-aware routing without membership."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import Behaviors, ActorRef, Behavior, ActorSystem, MemberStatus, Member
from casty.client import (
    ClusterClient,
    NodeFailed,
    SubscriptionTimeout,
    client_proxy_behavior,
    topology_subscriber,
)
from casty.cluster_state import NodeAddress
from casty.shard_coordinator_actor import ShardLocation
from casty.sharding import ClusteredActorSystem, ShardEnvelope
from casty.topology import SubscribeTopology, TopologySnapshot


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
            assert ref1.address == ref2.address


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
        proxy.tell(TopologySnapshot(
            members=frozenset({
                Member(address=node_a, status=MemberStatus.up, roles=frozenset(), id="a"),
                Member(address=node_b, status=MemberStatus.up, roles=frozenset(), id="b"),
            }),
            leader=node_a,
            shard_allocations={},
            allocation_epoch=1,
        ))
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
        from casty.sharding import entity_shard

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
        proxy.tell(TopologySnapshot(
            members=frozenset({
                Member(address=node_a, status=MemberStatus.up, roles=frozenset(), id="a"),
            }),
            leader=node_a,
            shard_allocations={},
            allocation_epoch=1,
        ))
        await asyncio.sleep(0.1)

        # Node-A fails
        proxy.tell(NodeFailed(host="10.0.0.1", port=25520))
        await asyncio.sleep(0.1)

        # Topology snapshot shows node-A is back and healthy
        proxy.tell(TopologySnapshot(
            members=frozenset({
                Member(address=node_a, status=MemberStatus.up, roles=frozenset(), id="a"),
            }),
            leader=node_a,
            shard_allocations={},
            allocation_epoch=2,
            unreachable=frozenset(),
        ))
        await asyncio.sleep(0.1)

        # Now ShardLocation pointing to node-A should be accepted (not rejected)
        proxy.tell(ShardLocation(shard_id=5, node=node_a))
        await asyncio.sleep(0.1)

        # The allocation should be cached — send an envelope for shard 5 to verify routing
        # We need to find which entity_id maps to shard 5 with num_shards=10
        from casty.sharding import entity_shard

        target_id = next(
            eid
            for i in range(1000)
            if entity_shard(eid := f"e-{i}", 10) == 5
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
                    sent_messages.append((f"{self._host}:{self._port}{self._path}", msg))

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
                    sent_messages.append((f"{self._host}:{self._port}{self._path}", msg))

            return _Ref(addr.path, addr.host, addr.port)

    import casty.client as client_mod

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
            sub_ref.tell(TopologySnapshot(
                members=frozenset(),
                leader=None,
                shard_allocations={},
                allocation_epoch=1,
            ))
            await asyncio.sleep(0.3)

            # Send another snapshot (within the 0.5s window)
            sub_ref.tell(TopologySnapshot(
                members=frozenset(),
                leader=None,
                shard_allocations={},
                allocation_epoch=2,
            ))
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
