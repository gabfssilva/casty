"""Throughput benchmarks for Casty actor system.

Run with:
    uv run pytest tests/benchmark/test_throughput.py -v -s

These benchmarks measure:
- send() throughput: Fire-and-forget messages per second
- ask() throughput: Request-response messages per second
- ask() latency: P50, P95, P99 latency in milliseconds
"""

import asyncio
import statistics
import time
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context
from casty.cluster.control_plane import RaftConfig
from casty.persistence import ReplicationConfig


# Fast Raft config for benchmarks
FAST_RAFT_CONFIG = RaftConfig(
    heartbeat_interval=0.05,
    election_timeout_min=0.1,
    election_timeout_max=0.2,
)


@dataclass
class Ping:
    """Simple ping message."""
    pass


@dataclass
class Pong:
    """Simple pong response."""
    pass


@dataclass
class Increment:
    """Increment counter."""
    amount: int = 1


@dataclass
class GetCount:
    """Get current count."""
    pass


class EchoActor(Actor[Ping]):
    """Actor that just receives pings and replies with Pong."""

    def __init__(self):
        self.count = 0

    async def receive(self, msg: Ping, ctx: Context) -> None:
        self.count += 1
        # Always reply - ctx.reply() handles the case when no reply is expected
        ctx.reply(Pong())


class CounterActor(Actor[Increment | GetCount]):
    """Actor that counts increments."""

    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                ctx.reply(self.count)


def print_report(
    title: str,
    total_messages: int,
    duration: float,
    latencies_ms: list[float] | None = None,
    verified: bool = True,
):
    """Print a formatted benchmark report."""
    throughput = total_messages / duration if duration > 0 else 0

    print()
    print("=" * 60)
    print(title)
    print("=" * 60)
    print(f"Total messages:   {total_messages:,}")
    print(f"Duration:         {duration:.3f}s")
    print(f"Throughput:       {throughput:,.1f} msg/s")

    if latencies_ms:
        latencies_sorted = sorted(latencies_ms)
        p50 = latencies_sorted[int(len(latencies_sorted) * 0.50)]
        p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)]
        p99 = latencies_sorted[int(len(latencies_sorted) * 0.99)]
        mean = statistics.mean(latencies_ms)
        stdev = statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0

        print(f"Latency Mean:     {mean:.3f}ms")
        print(f"Latency StdDev:   {stdev:.3f}ms")
        print(f"Latency P50:      {p50:.3f}ms")
        print(f"Latency P95:      {p95:.3f}ms")
        print(f"Latency P99:      {p99:.3f}ms")

    print(f"Verification:     {'PASSED' if verified else 'FAILED'}")
    print("=" * 60)


class TestLocalSystemThroughput:
    """Benchmarks for local ActorSystem."""

    @pytest.mark.asyncio
    async def test_send_throughput(self):
        """Benchmark send() throughput for local system."""
        num_messages = 10_000

        async with ActorSystem() as system:
            actor = await system.spawn(CounterActor)

            # Warm up
            for _ in range(100):
                await actor.send(Increment())
            await asyncio.sleep(0.1)

            # Benchmark - measure only send time
            start = time.perf_counter()

            for _ in range(num_messages):
                await actor.send(Increment())

            duration = time.perf_counter() - start

            # Wait for processing AFTER measuring
            await asyncio.sleep(0.5)

            # Verify
            count = await actor.ask(GetCount())
            verified = count == num_messages + 100  # Including warmup

            print_report(
                "LOCAL send() Throughput",
                num_messages,
                duration,
                verified=verified,
            )

    @pytest.mark.asyncio
    async def test_ask_throughput(self):
        """Benchmark ask() throughput for local system."""
        num_messages = 5_000

        async with ActorSystem() as system:
            actor = await system.spawn(EchoActor)

            # Warm up
            for _ in range(50):
                await actor.ask(Ping())

            # Benchmark with latency tracking
            latencies_ms: list[float] = []
            start = time.perf_counter()

            for _ in range(num_messages):
                msg_start = time.perf_counter()
                await actor.ask(Ping())
                latencies_ms.append((time.perf_counter() - msg_start) * 1000)

            duration = time.perf_counter() - start

            print_report(
                "LOCAL ask() Throughput",
                num_messages,
                duration,
                latencies_ms=latencies_ms,
            )

    @pytest.mark.asyncio
    async def test_concurrent_send_throughput(self):
        """Benchmark concurrent send() throughput."""
        num_messages = 10_000
        concurrency = 100

        async with ActorSystem() as system:
            actor = await system.spawn(CounterActor)

            # Warm up
            await asyncio.gather(*[actor.send(Increment()) for _ in range(100)])
            await asyncio.sleep(0.1)

            # Benchmark - measure only send time
            start = time.perf_counter()

            # Send in batches to maintain concurrency
            for i in range(0, num_messages, concurrency):
                batch_size = min(concurrency, num_messages - i)
                await asyncio.gather(*[actor.send(Increment()) for _ in range(batch_size)])

            duration = time.perf_counter() - start

            # Wait for processing AFTER measuring
            await asyncio.sleep(0.3)

            count = await actor.ask(GetCount())
            verified = count == num_messages + 100

            print_report(
                f"LOCAL Concurrent send() ({concurrency} tasks)",
                num_messages,
                duration,
                verified=verified,
            )

    @pytest.mark.asyncio
    async def test_concurrent_ask_throughput(self):
        """Benchmark concurrent ask() throughput."""
        num_messages = 2_000
        concurrency = 50

        async with ActorSystem() as system:
            actor = await system.spawn(EchoActor)

            # Warm up
            await asyncio.gather(*[actor.ask(Ping()) for _ in range(50)])

            # Benchmark
            latencies_ms: list[float] = []

            async def timed_ask():
                msg_start = time.perf_counter()
                await actor.ask(Ping())
                return (time.perf_counter() - msg_start) * 1000

            start = time.perf_counter()

            for i in range(0, num_messages, concurrency):
                batch_size = min(concurrency, num_messages - i)
                results = await asyncio.gather(*[timed_ask() for _ in range(batch_size)])
                latencies_ms.extend(results)

            duration = time.perf_counter() - start

            print_report(
                f"LOCAL Concurrent ask() ({concurrency} tasks)",
                num_messages,
                duration,
                latencies_ms=latencies_ms,
            )


class TestDistributedSystemThroughput:
    """Benchmarks for DistributedActorSystem."""

    @pytest.mark.asyncio
    async def test_send_throughput_single_node(self):
        """Benchmark send() throughput for single-node distributed system."""
        num_messages = 5_000

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)  # Wait for election

            actor = await system.spawn(CounterActor)

            # Warm up
            for _ in range(100):
                await actor.send(Increment())
            await asyncio.sleep(0.2)

            # Benchmark - measure only send time
            start = time.perf_counter()

            for _ in range(num_messages):
                await actor.send(Increment())

            duration = time.perf_counter() - start

            # Wait for processing AFTER measuring
            await asyncio.sleep(0.5)

            count = await actor.ask(GetCount())
            verified = count == num_messages + 100

            print_report(
                "DISTRIBUTED (1 node) send() Throughput",
                num_messages,
                duration,
                verified=verified,
            )

    @pytest.mark.asyncio
    async def test_ask_throughput_single_node(self):
        """Benchmark ask() throughput for single-node distributed system."""
        num_messages = 2_000

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            actor = await system.spawn(EchoActor)

            # Warm up
            for _ in range(50):
                await actor.ask(Ping())

            # Benchmark
            latencies_ms: list[float] = []
            start = time.perf_counter()

            for _ in range(num_messages):
                msg_start = time.perf_counter()
                await actor.ask(Ping())
                latencies_ms.append((time.perf_counter() - msg_start) * 1000)

            duration = time.perf_counter() - start

            print_report(
                "DISTRIBUTED (1 node) ask() Throughput",
                num_messages,
                duration,
                latencies_ms=latencies_ms,
            )

    @pytest.mark.asyncio
    async def test_singleton_throughput(self):
        """Benchmark singleton actor throughput."""
        num_messages = 2_000

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            actor = await system.spawn(
                CounterActor,
                name="benchmark-singleton",
                singleton=True,
            )

            # Warm up
            for _ in range(50):
                await actor.send(Increment())
            await asyncio.sleep(0.1)

            # Benchmark - measure only send time
            start = time.perf_counter()

            for _ in range(num_messages):
                await actor.send(Increment())

            duration = time.perf_counter() - start

            # Wait for processing AFTER measuring
            await asyncio.sleep(0.3)

            count = await actor.ask(GetCount())
            verified = count == num_messages + 50

            print_report(
                "SINGLETON send() Throughput",
                num_messages,
                duration,
                verified=verified,
            )


class TestShardedThroughput:
    """Benchmarks for sharded actors."""

    @pytest.mark.asyncio
    async def test_sharded_send_throughput(self):
        """Benchmark sharded entity send() throughput."""
        num_messages = 2_000
        num_entities = 10

        class ShardedCounter(Actor[Increment | GetCount]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.count = 0

            async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
                match msg:
                    case Increment(amount):
                        self.count += amount
                    case GetCount():
                        ctx.reply(self.count)

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="benchmark-counters",
                sharded=True,
            )

            # Warm up
            for i in range(50):
                entity_id = f"entity-{i % num_entities}"
                await counters[entity_id].send(Increment())
            await asyncio.sleep(0.2)

            # Benchmark - measure only send time
            start = time.perf_counter()

            for i in range(num_messages):
                entity_id = f"entity-{i % num_entities}"
                await counters[entity_id].send(Increment())

            duration = time.perf_counter() - start

            # Wait for processing AFTER measuring
            await asyncio.sleep(0.5)

            # Verify total count
            total = 0
            for i in range(num_entities):
                count = await counters[f"entity-{i}"].ask(GetCount())
                total += count

            expected = num_messages + 50  # Including warmup
            verified = total == expected

            print_report(
                f"SHARDED ({num_entities} entities) send() Throughput",
                num_messages,
                duration,
                verified=verified,
            )

    @pytest.mark.asyncio
    async def test_sharded_ask_throughput(self):
        """Benchmark sharded entity ask() throughput."""
        num_messages = 1_000
        num_entities = 10

        class ShardedEcho(Actor[Ping]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id

            async def receive(self, msg: Ping, ctx: Context) -> None:
                ctx.reply(Pong())

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            echos = await system.spawn(
                ShardedEcho,
                name="benchmark-echos",
                sharded=True,
            )

            # Warm up
            for i in range(50):
                entity_id = f"entity-{i % num_entities}"
                await echos[entity_id].ask(Ping())

            # Benchmark
            latencies_ms: list[float] = []
            start = time.perf_counter()

            for i in range(num_messages):
                entity_id = f"entity-{i % num_entities}"
                msg_start = time.perf_counter()
                await echos[entity_id].ask(Ping())
                latencies_ms.append((time.perf_counter() - msg_start) * 1000)

            duration = time.perf_counter() - start

            print_report(
                f"SHARDED ({num_entities} entities) ask() Throughput",
                num_messages,
                duration,
                latencies_ms=latencies_ms,
            )


class TestClusterThroughput:
    """Benchmarks for real multi-node clusters."""

    @pytest.mark.asyncio
    async def test_cluster_ask_throughput(self):
        """Benchmark ask() throughput in a 3-node cluster."""
        num_messages = 500
        num_nodes = 3

        # Create cluster nodes
        nodes: list[ActorSystem] = []
        ports: list[int] = []

        try:
            # Start first node (will become leader)
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)
            ports.append(port1)

            # Wait for node1 to become leader
            await asyncio.sleep(0.5)

            # Start additional nodes connecting to first
            for i in range(1, num_nodes):
                node = ActorSystem.distributed(
                    "127.0.0.1",
                    0,
                    seeds=[f"127.0.0.1:{port1}"],
                    raft_config=FAST_RAFT_CONFIG,
                )
                await node.start()
                port = node._transport._server.sockets[0].getsockname()[1]
                nodes.append(node)
                ports.append(port)

            # Wait for cluster to stabilize
            await asyncio.sleep(1.0)

            # Spawn actor on first node
            actor = await nodes[0].spawn(EchoActor)

            # Warm up
            for _ in range(20):
                await actor.ask(Ping())

            # Benchmark from first node
            latencies_ms: list[float] = []
            start = time.perf_counter()

            for _ in range(num_messages):
                msg_start = time.perf_counter()
                await actor.ask(Ping())
                latencies_ms.append((time.perf_counter() - msg_start) * 1000)

            duration = time.perf_counter() - start

            print_report(
                f"CLUSTER ({num_nodes} nodes) ask() Throughput",
                num_messages,
                duration,
                latencies_ms=latencies_ms,
            )

        finally:
            # Shutdown all nodes
            for node in reversed(nodes):
                await node.shutdown()

    @pytest.mark.asyncio
    async def test_cluster_cross_node_ask(self):
        """Benchmark ask() from one node to actor on another node.

        NOTE: This test is currently limited because remote refs
        are not fully implemented. It measures local actor access
        from multiple nodes as a baseline.
        """
        num_messages = 200
        num_nodes = 3

        nodes: list[ActorSystem] = []
        ports: list[int] = []

        try:
            # Start first node
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)
            ports.append(port1)

            await asyncio.sleep(0.5)

            # Start additional nodes
            for i in range(1, num_nodes):
                node = ActorSystem.distributed(
                    "127.0.0.1",
                    0,
                    seeds=[f"127.0.0.1:{port1}"],
                    raft_config=FAST_RAFT_CONFIG,
                )
                await node.start()
                port = node._transport._server.sockets[0].getsockname()[1]
                nodes.append(node)
                ports.append(port)

            await asyncio.sleep(1.0)

            # Each node spawns its own actor
            actors = []
            for node in nodes:
                actor = await node.spawn(CounterActor)
                actors.append(actor)

            # Benchmark concurrent asks from all nodes
            async def ask_from_node(node_idx: int):
                actor = actors[node_idx]
                for _ in range(num_messages // num_nodes):
                    await actor.send(Increment())

            start = time.perf_counter()
            await asyncio.gather(*[ask_from_node(i) for i in range(num_nodes)])
            duration = time.perf_counter() - start

            await asyncio.sleep(0.3)

            # Verify counts
            total = 0
            for actor in actors:
                count = await actor.ask(GetCount())
                total += count

            # num_messages // num_nodes * num_nodes might be less than num_messages
            expected = (num_messages // num_nodes) * num_nodes
            verified = total == expected

            print_report(
                f"CLUSTER ({num_nodes} nodes) parallel send() Throughput",
                expected,  # Use actual messages sent
                duration,
                verified=verified,
            )

        finally:
            for node in reversed(nodes):
                await node.shutdown()


class TestReplicationThroughput:
    """Benchmarks for state replication with different replication factors.

    Tests the throughput impact of state replication across cluster nodes.
    After an entity processes a message, its state is replicated to backup
    nodes asynchronously via STATE_UPDATE messages.

    Current behavior:
    - RF=1: Only primary node handles the entity, no replication overhead
    - RF=2: State replicated to 1 backup node after each message
    - RF=3: State replicated to 2 backup nodes after each message

    Expected throughput characteristics:
    - RF=1 is fastest (no network overhead for replication)
    - RF>1 introduces overhead proportional to (RF-1) state updates
    """

    @pytest.mark.asyncio
    async def test_replication_factor_1(self):
        """Benchmark with RF=1 (primary only, no backups in placement)."""
        await self._run_replication_benchmark(
            replication_factor=1,
            num_nodes=3,
            num_messages=300,
        )

    @pytest.mark.asyncio
    async def test_replication_factor_2(self):
        """Benchmark with RF=2 (primary + 1 backup in placement)."""
        await self._run_replication_benchmark(
            replication_factor=2,
            num_nodes=3,
            num_messages=300,
        )

    @pytest.mark.asyncio
    async def test_replication_factor_3(self):
        """Benchmark with RF=3 (primary + 2 backups in placement)."""
        await self._run_replication_benchmark(
            replication_factor=3,
            num_nodes=3,
            num_messages=300,
        )

    async def _run_replication_benchmark(
        self,
        replication_factor: int,
        num_nodes: int,
        num_messages: int,
    ):
        """Run a replication benchmark with given parameters."""

        class ReplicatedCounter(Actor[Increment | GetCount]):
            """Counter that tracks state for replication."""

            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.count = 0

            def get_state(self) -> dict:
                return {"count": self.count}

            def set_state(self, state: dict) -> None:
                self.count = state.get("count", 0)

            async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
                match msg:
                    case Increment(amount):
                        self.count += amount
                    case GetCount():
                        ctx.reply(self.count)

        nodes: list[ActorSystem] = []
        ports: list[int] = []

        try:
            # Start first node
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)
            ports.append(port1)

            await asyncio.sleep(0.5)

            # Start additional nodes
            for i in range(1, num_nodes):
                node = ActorSystem.distributed(
                    "127.0.0.1",
                    0,
                    seeds=[f"127.0.0.1:{port1}"],
                    raft_config=FAST_RAFT_CONFIG,
                )
                await node.start()
                port = node._transport._server.sockets[0].getsockname()[1]
                nodes.append(node)
                ports.append(port)

            await asyncio.sleep(1.0)

            # Spawn sharded counter with specified replication factor
            counters = await nodes[0].spawn(
                ReplicatedCounter,
                name="replicated-counters",
                sharded=True,
                replication=ReplicationConfig(factor=replication_factor),
            )

            # Warm up
            for i in range(20):
                await counters[f"entity-{i % 5}"].send(Increment())
            await asyncio.sleep(0.2)

            # Benchmark
            num_entities = 5
            start = time.perf_counter()

            for i in range(num_messages):
                entity_id = f"entity-{i % num_entities}"
                await counters[entity_id].send(Increment())

            duration = time.perf_counter() - start

            await asyncio.sleep(0.5)

            # Verify
            total = 0
            for i in range(num_entities):
                count = await counters[f"entity-{i}"].ask(GetCount())
                total += count

            expected = num_messages + 20
            verified = total == expected

            print_report(
                f"REPLICATION RF={replication_factor} ({num_nodes} nodes) send() Throughput",
                num_messages,
                duration,
                verified=verified,
            )

        finally:
            for node in reversed(nodes):
                await node.shutdown()


class TestLargeClusterReplication:
    """Benchmarks for large cluster with varying replication factors.

    Tests a 30-node cluster with RF = 1, 3, 5, 10, 15, 30.
    """

    @pytest.mark.asyncio
    async def test_30_nodes_rf1(self):
        """30-node cluster with RF=1 (no replication)."""
        await self._run_large_cluster_benchmark(num_nodes=30, replication_factor=1)

    @pytest.mark.asyncio
    async def test_30_nodes_rf3(self):
        """30-node cluster with RF=3."""
        await self._run_large_cluster_benchmark(num_nodes=30, replication_factor=3)

    @pytest.mark.asyncio
    async def test_30_nodes_rf5(self):
        """30-node cluster with RF=5."""
        await self._run_large_cluster_benchmark(num_nodes=30, replication_factor=5)

    @pytest.mark.asyncio
    async def test_30_nodes_rf10(self):
        """30-node cluster with RF=10."""
        await self._run_large_cluster_benchmark(num_nodes=30, replication_factor=10)

    @pytest.mark.asyncio
    async def test_30_nodes_rf15(self):
        """30-node cluster with RF=15."""
        await self._run_large_cluster_benchmark(num_nodes=30, replication_factor=15)

    @pytest.mark.asyncio
    async def test_30_nodes_rf30(self):
        """30-node cluster with RF=30 (full replication)."""
        await self._run_large_cluster_benchmark(num_nodes=30, replication_factor=30)

    async def _run_large_cluster_benchmark(
        self,
        num_nodes: int,
        replication_factor: int,
        num_messages: int = 5000,
        num_entities: int = 50,
    ):
        """Run a benchmark on a large cluster."""

        class LargeClusterCounter(Actor[Increment | GetCount]):
            """Counter for large cluster testing."""

            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.count = 0

            def get_state(self) -> dict:
                return {"count": self.count}

            def set_state(self, state: dict) -> None:
                self.count = state.get("count", 0)

            async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
                match msg:
                    case Increment(amount):
                        self.count += amount
                    case GetCount():
                        ctx.reply(self.count)

        nodes: list[ActorSystem] = []
        ports: list[int] = []

        print(f"\n>>> Starting {num_nodes}-node cluster (this may take a moment)...")

        try:
            # Start first node
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)
            ports.append(port1)

            # Wait for leader election
            await asyncio.sleep(0.5)

            # Start remaining nodes in batches to avoid overwhelming
            batch_size = 5
            for batch_start in range(1, num_nodes, batch_size):
                batch_end = min(batch_start + batch_size, num_nodes)
                batch_tasks = []

                for i in range(batch_start, batch_end):
                    node = ActorSystem.distributed(
                        "127.0.0.1",
                        0,
                        seeds=[f"127.0.0.1:{port1}"],
                        raft_config=FAST_RAFT_CONFIG,
                    )
                    batch_tasks.append(node.start())
                    nodes.append(node)

                await asyncio.gather(*batch_tasks)

                # Get ports for new nodes
                for node in nodes[batch_start:batch_end]:
                    port = node._transport._server.sockets[0].getsockname()[1]
                    ports.append(port)

                # Small delay between batches
                await asyncio.sleep(0.2)

            # Wait for cluster stabilization
            await asyncio.sleep(2.0)

            print(f">>> Cluster ready with {len(nodes)} nodes")

            # Spawn sharded counter with specified replication factor
            counters = await nodes[0].spawn(
                LargeClusterCounter,
                name="large-cluster-counters",
                sharded=True,
                replication=ReplicationConfig(factor=replication_factor),
            )

            # Warm up
            for i in range(20):
                await counters[f"entity-{i % num_entities}"].send(Increment())
            await asyncio.sleep(0.5)

            # Benchmark
            start = time.perf_counter()

            for i in range(num_messages):
                entity_id = f"entity-{i % num_entities}"
                await counters[entity_id].send(Increment())

            duration = time.perf_counter() - start

            # Wait for replication to complete
            await asyncio.sleep(1.0)

            # Verify
            total = 0
            for i in range(num_entities):
                count = await counters[f"entity-{i}"].ask(GetCount())
                total += count

            expected = num_messages + 20
            verified = total == expected

            print_report(
                f"LARGE CLUSTER ({num_nodes} nodes, RF={replication_factor}) send() Throughput",
                num_messages,
                duration,
                verified=verified,
            )

        finally:
            print(f">>> Shutting down {len(nodes)} nodes...")
            # Shutdown in reverse order, in batches
            shutdown_batch_size = 5
            for i in range(len(nodes) - 1, -1, -shutdown_batch_size):
                batch_start_idx = max(0, i - shutdown_batch_size + 1)
                shutdown_tasks = [nodes[j].shutdown() for j in range(i, batch_start_idx - 1, -1)]
                await asyncio.gather(*shutdown_tasks, return_exceptions=True)


class TestSyncVsAsyncReplication:
    """Benchmarks comparing synchronous vs asynchronous replication modes.

    Tests the throughput impact of different sync_count settings:
    - sync_count=0: Fire-and-forget (default)
    - sync_count=1: Wait for 1 replica ACK
    - sync_count=2: Wait for 2 replica ACKs (quorum for RF=3)
    - sync_count=-1: Wait for all replicas ACK
    """

    @pytest.mark.asyncio
    async def test_async_replication(self):
        """Benchmark ASYNC mode (fire-and-forget)."""
        await self._run_mode_benchmark(
            sync_count=0,
            mode_name="ASYNC",
            num_nodes=5,
            replication_factor=3,
            num_messages=10000,
        )

    @pytest.mark.asyncio
    async def test_sync_one_replication(self):
        """Benchmark SYNC_ONE mode (wait for 1 replica)."""
        await self._run_mode_benchmark(
            sync_count=1,
            mode_name="SYNC_ONE",
            num_nodes=5,
            replication_factor=3,
            num_messages=10000,
        )

    @pytest.mark.asyncio
    async def test_sync_quorum_replication(self):
        """Benchmark SYNC_QUORUM mode (wait for majority)."""
        await self._run_mode_benchmark(
            sync_count=2,  # Quorum for RF=3
            mode_name="SYNC_QUORUM",
            num_nodes=5,
            replication_factor=3,
            num_messages=10000,
        )

    @pytest.mark.asyncio
    async def test_sync_all_replication(self):
        """Benchmark SYNC_ALL mode (wait for all replicas)."""
        await self._run_mode_benchmark(
            sync_count=-1,
            mode_name="SYNC_ALL",
            num_nodes=5,
            replication_factor=3,
            num_messages=10000,
        )

    async def _run_mode_benchmark(
        self,
        sync_count: int,
        mode_name: str,
        num_nodes: int,
        replication_factor: int,
        num_messages: int,
    ):
        """Run benchmark with specific replication mode."""

        class SyncTestCounter(Actor[Increment | GetCount]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.count = 0

            def get_state(self) -> dict:
                return {"count": self.count}

            def set_state(self, state: dict) -> None:
                self.count = state.get("count", 0)

            async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
                match msg:
                    case Increment(amount):
                        self.count += amount
                    case GetCount():
                        ctx.reply(self.count)

        nodes: list[ActorSystem] = []
        print(f"\n>>> Starting {num_nodes}-node cluster for {mode_name} mode...")

        try:
            # Start first node
            node1 = ActorSystem.distributed(
                "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
            )
            await node1.start()
            port1 = node1._transport._server.sockets[0].getsockname()[1]
            nodes.append(node1)

            await asyncio.sleep(0.5)

            # Start remaining nodes
            for i in range(1, num_nodes):
                node = ActorSystem.distributed(
                    "127.0.0.1",
                    0,
                    seeds=[f"127.0.0.1:{port1}"],
                    raft_config=FAST_RAFT_CONFIG,
                )
                await node.start()
                nodes.append(node)

            await asyncio.sleep(1.5)
            print(f">>> Cluster ready with {len(nodes)} nodes")

            # Spawn sharded counter with specific mode
            counters = await nodes[0].spawn(
                SyncTestCounter,
                name="sync-test-counters",
                sharded=True,
                replication=ReplicationConfig(
                    factor=replication_factor,
                    sync_count=sync_count,
                    timeout=5.0,
                ),
            )

            # Warm up
            num_entities = 10
            for i in range(20):
                await counters[f"entity-{i % num_entities}"].send(Increment())
            await asyncio.sleep(0.3)

            # Benchmark
            start = time.perf_counter()

            for i in range(num_messages):
                entity_id = f"entity-{i % num_entities}"
                await counters[entity_id].send(Increment())

            duration = time.perf_counter() - start

            # Wait for async replication to complete
            await asyncio.sleep(1.0)

            # Verify
            total = 0
            for i in range(num_entities):
                count = await counters[f"entity-{i}"].ask(GetCount())
                total += count

            expected = num_messages + 20
            verified = total == expected

            print_report(
                f"{mode_name} REPLICATION ({num_nodes} nodes, RF={replication_factor})",
                num_messages,
                duration,
                verified=verified,
            )

        finally:
            print(f">>> Shutting down {len(nodes)} nodes...")
            for node in reversed(nodes):
                await node.shutdown()


class TestMailboxPressure:
    """Benchmarks for mailbox under pressure."""

    @pytest.mark.asyncio
    async def test_burst_throughput(self):
        """Benchmark handling message bursts."""
        num_messages = 20_000
        burst_size = 1000

        async with ActorSystem() as system:
            actor = await system.spawn(CounterActor)

            # Benchmark - measure only send time
            start = time.perf_counter()

            for _ in range(num_messages // burst_size):
                # Send burst without waiting
                for _ in range(burst_size):
                    await actor.send(Increment())
                # Small pause between bursts
                await asyncio.sleep(0.01)

            duration = time.perf_counter() - start

            # Wait for processing AFTER measuring
            await asyncio.sleep(1.0)

            count = await actor.ask(GetCount())
            verified = count == num_messages

            print_report(
                f"BURST ({burst_size} msg/burst) Throughput",
                num_messages,
                duration,
                verified=verified,
            )
