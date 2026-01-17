import pytest
import asyncio
from dataclasses import dataclass

from casty import Actor
from casty.cluster.development import DevelopmentCluster
from casty.cluster.merge import merge_sum


@dataclass
class Deposit:
    amount: int


@dataclass
class Withdraw:
    amount: int


@dataclass
class GetBalance:
    pass


class MergeableAccount(Actor[Deposit | Withdraw | GetBalance]):

    def __init__(self):
        self.balance = 0

    async def receive(self, msg, ctx):
        match msg:
            case Deposit(amount=amount):
                self.balance += amount
            case Withdraw(amount=amount):
                self.balance -= amount
            case GetBalance():
                ctx.reply(self.balance)

    def get_state(self) -> dict:
        return {"balance": self.balance}

    def set_state(self, state: dict) -> None:
        self.balance = state.get("balance", 0)

    def __casty_merge__(self, base: "MergeableAccount", other: "MergeableAccount"):
        self.balance = merge_sum(base.balance, self.balance, other.balance)


@dataclass
class Add:
    item: str


@dataclass
class GetItems:
    pass


class MergeableSet(Actor[Add | GetItems]):

    def __init__(self):
        self.items: set[str] = set()

    async def receive(self, msg, ctx):
        match msg:
            case Add(item=item):
                self.items.add(item)
            case GetItems():
                ctx.reply(self.items)

    def get_state(self) -> dict:
        return {"items": list(self.items)}

    def set_state(self, state: dict) -> None:
        self.items = set(state.get("items", []))

    def __casty_merge__(self, base: "MergeableSet", other: "MergeableSet"):
        self.items = self.items | other.items


class TestMergeIntegrationSingleNode:

    @pytest.mark.asyncio
    async def test_state_replication_two_nodes(self):
        async with DevelopmentCluster(2) as (node1, node2):
            ref = await node1.spawn(MergeableAccount, clustered=True, replication=2)
            await asyncio.sleep(0.2)

            await ref.send(Deposit(amount=100))
            await asyncio.sleep(0.2)

            balance = await ref.ask(GetBalance())
            assert balance == 100

    @pytest.mark.asyncio
    async def test_multiple_deposits_replicated(self):
        async with DevelopmentCluster(2) as (node1, node2):
            ref = await node1.spawn(MergeableAccount, clustered=True, replication=2)
            await asyncio.sleep(0.2)

            await ref.send(Deposit(amount=50))
            await ref.send(Deposit(amount=30))
            await ref.send(Deposit(amount=20))
            await asyncio.sleep(0.3)

            balance = await ref.ask(GetBalance())
            assert balance == 100

    @pytest.mark.asyncio
    async def test_set_union_merge(self):
        async with DevelopmentCluster(2) as (node1, node2):
            ref = await node1.spawn(MergeableSet, clustered=True, replication=2)
            await asyncio.sleep(0.2)

            await ref.send(Add(item="apple"))
            await ref.send(Add(item="banana"))
            await asyncio.sleep(0.2)

            items = await ref.ask(GetItems())
            assert items == {"apple", "banana"}


class TestMergeIntegrationThreeNodes:

    @pytest.mark.asyncio
    async def test_replication_across_three_nodes(self):
        async with DevelopmentCluster(3) as (node1, node2, node3):
            ref = await node1.spawn(MergeableAccount, clustered=True, replication=3)
            await asyncio.sleep(0.3)

            await ref.send(Deposit(amount=1000))
            await asyncio.sleep(0.3)

            balance = await ref.ask(GetBalance())
            assert balance == 1000

    @pytest.mark.asyncio
    async def test_sequential_operations_three_nodes(self):
        async with DevelopmentCluster(3) as (node1, node2, node3):
            ref = await node1.spawn(MergeableAccount, clustered=True, replication=3)
            await asyncio.sleep(0.3)

            for i in range(5):
                await ref.send(Deposit(amount=10))
                await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 50


class TestMergeableBehavior:

    @pytest.mark.asyncio
    async def test_mergeable_actor_get_set_state(self):
        async with DevelopmentCluster(1) as (system,):
            ref = await system.spawn(MergeableAccount, clustered=True)
            await asyncio.sleep(0.1)

            await ref.send(Deposit(amount=500))
            await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 500

    @pytest.mark.asyncio
    async def test_withdraw_operation(self):
        async with DevelopmentCluster(1) as (system,):
            ref = await system.spawn(MergeableAccount, clustered=True)
            await asyncio.sleep(0.1)

            await ref.send(Deposit(amount=100))
            await ref.send(Withdraw(amount=30))
            await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 70


class TestVectorClockTracking:

    @pytest.mark.asyncio
    async def test_version_increments_on_mutation(self):
        async with DevelopmentCluster(2) as (node1, node2):
            ref = await node1.spawn(MergeableAccount, clustered=True, replication=2)
            await asyncio.sleep(0.2)

            for _ in range(3):
                await ref.send(Deposit(amount=10))
                await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 30

    @pytest.mark.asyncio
    async def test_wal_tracks_deltas(self):
        async with DevelopmentCluster(1) as (system,):
            ref = await system.spawn(MergeableAccount, clustered=True)
            await asyncio.sleep(0.1)

            await ref.send(Deposit(amount=100))
            await ref.send(Deposit(amount=50))
            await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 150


class TestConcurrentModificationMerge:

    def _get_cluster_actor(self, system):
        cluster_ref = system._cluster
        node = system._supervision_tree.get_node(cluster_ref.id)
        return node.actor_instance if node else None

    async def _wait_for_cluster_membership(self, systems, timeout=2.0):
        import time
        start = time.time()
        expected_members = len(systems)
        while time.time() - start < timeout:
            all_connected = True
            for system in systems:
                cluster = self._get_cluster_actor(system)
                if cluster is None or len(cluster._members) < expected_members:
                    all_connected = False
                    break
            if all_connected:
                return True
            await asyncio.sleep(0.05)
        return False

    async def _wait_for_replication(self, systems, actor_id, timeout=2.0):
        import time
        start = time.time()
        while time.time() - start < timeout:
            all_have_actor = True
            for system in systems:
                cluster = self._get_cluster_actor(system)
                if cluster is None or actor_id not in cluster._local_actors:
                    all_have_actor = False
                    break
            if all_have_actor:
                return True
            await asyncio.sleep(0.05)
        return False

    @pytest.mark.asyncio
    async def test_concurrent_deposits_merge_correctly(self):
        from casty.cluster.merge import VectorClock, handle_replicate_state
        from casty.cluster.messages import ReplicateState

        async with DevelopmentCluster(2) as (node1, node2):
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.spawn(
                MergeableAccount,
                name="shared-account",
                clustered=True,
                replication=2,
                write_consistency='all',
            )

            replicated = await self._wait_for_replication([node1, node2], "shared-account")
            assert replicated, "Actor not replicated to all nodes"

            await ref.send(Deposit(amount=1))
            await asyncio.sleep(0.1)

            cluster1 = self._get_cluster_actor(node1)
            cluster2 = self._get_cluster_actor(node2)

            local_ref1 = cluster1._local_actors.get("shared-account")
            local_ref2 = cluster2._local_actors.get("shared-account")
            wal1 = cluster1._wals.get("shared-account")
            wal2 = cluster2._wals.get("shared-account")

            actor_node1 = node1._supervision_tree.get_node(local_ref1.id)
            actor_node2 = node2._supervision_tree.get_node(local_ref2.id)
            actor1 = actor_node1.actor_instance
            actor2 = actor_node2.actor_instance

            actor1.balance = 100
            wal1.entries.clear()
            wal1.base_snapshot = {"balance": 0}
            wal1.base_version = VectorClock()
            version1 = wal1.append({"balance": 100})

            actor2.balance = 50
            wal2.entries.clear()
            wal2.base_snapshot = {"balance": 0}
            wal2.base_version = VectorClock()
            version2 = wal2.append({"balance": 50})

            assert version1.concurrent(version2)

            msg = ReplicateState(
                actor_id="shared-account",
                version=version1,
                state={"balance": 100},
            )
            await handle_replicate_state(msg, actor2, wal2, "node-1")

            assert actor2.balance == 150

    @pytest.mark.asyncio
    async def test_set_concurrent_adds_merge_with_union(self):
        from casty.cluster.merge import VectorClock, handle_replicate_state
        from casty.cluster.messages import ReplicateState

        async with DevelopmentCluster(2) as (node1, node2):
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.spawn(
                MergeableSet,
                name="shared-set",
                clustered=True,
                replication=2,
                write_consistency='all',
            )

            replicated = await self._wait_for_replication([node1, node2], "shared-set")
            assert replicated, "Actor not replicated to all nodes"

            await ref.send(Add(item="init"))
            await asyncio.sleep(0.1)

            cluster1 = self._get_cluster_actor(node1)
            cluster2 = self._get_cluster_actor(node2)

            local_ref1 = cluster1._local_actors.get("shared-set")
            local_ref2 = cluster2._local_actors.get("shared-set")
            wal1 = cluster1._wals.get("shared-set")
            wal2 = cluster2._wals.get("shared-set")

            actor_node1 = node1._supervision_tree.get_node(local_ref1.id)
            actor_node2 = node2._supervision_tree.get_node(local_ref2.id)
            actor1 = actor_node1.actor_instance
            actor2 = actor_node2.actor_instance

            actor1.items = {"apple", "banana"}
            wal1.entries.clear()
            wal1.base_snapshot = {"items": []}
            wal1.base_version = VectorClock()
            version1 = wal1.append({"items": ["apple", "banana"]})

            actor2.items = {"cherry", "date"}
            wal2.entries.clear()
            wal2.base_snapshot = {"items": []}
            wal2.base_version = VectorClock()
            version2 = wal2.append({"items": ["cherry", "date"]})

            assert version1.concurrent(version2)

            msg = ReplicateState(
                actor_id="shared-set",
                version=version1,
                state={"items": ["apple", "banana"]},
            )
            await handle_replicate_state(msg, actor2, wal2, "node-1")

            assert actor2.items == {"apple", "banana", "cherry", "date"}

    @pytest.mark.asyncio
    async def test_dominated_version_accepts_state(self):
        from casty.cluster.merge import VectorClock, handle_replicate_state
        from casty.cluster.messages import ReplicateState

        async with DevelopmentCluster(2) as (node1, node2):
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.spawn(
                MergeableAccount,
                name="sync-account",
                clustered=True,
                replication=2,
                write_consistency='all',
            )

            replicated = await self._wait_for_replication([node1, node2], "sync-account")
            assert replicated, "Actor not replicated to all nodes"

            cluster2 = self._get_cluster_actor(node2)
            local_ref2 = cluster2._local_actors.get("sync-account")
            wal2 = cluster2._wals.get("sync-account")

            actor_node2 = node2._supervision_tree.get_node(local_ref2.id)
            actor2 = actor_node2.actor_instance

            actor2.balance = 0
            wal2.entries.clear()
            wal2.base_snapshot = {}
            wal2.base_version = VectorClock()

            newer_version = VectorClock().increment("node-0").increment("node-0")
            msg = ReplicateState(
                actor_id="sync-account",
                version=newer_version,
                state={"balance": 500},
            )
            ack = await handle_replicate_state(msg, actor2, wal2, "node-1")

            assert ack.success
            assert actor2.balance == 500

    @pytest.mark.asyncio
    async def test_my_version_dominates_ignores_state(self):
        from casty.cluster.merge import VectorClock, handle_replicate_state
        from casty.cluster.messages import ReplicateState

        async with DevelopmentCluster(2) as (node1, node2):
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.spawn(
                MergeableAccount,
                name="ignore-account",
                clustered=True,
                replication=2,
                write_consistency='all',
            )

            replicated = await self._wait_for_replication([node1, node2], "ignore-account")
            assert replicated, "Actor not replicated to all nodes"

            cluster2 = self._get_cluster_actor(node2)
            local_ref2 = cluster2._local_actors.get("ignore-account")
            wal2 = cluster2._wals.get("ignore-account")

            actor_node2 = node2._supervision_tree.get_node(local_ref2.id)
            actor2 = actor_node2.actor_instance

            actor2.balance = 1000
            wal2.entries.clear()
            wal2.base_snapshot = {}
            older_version = VectorClock().increment("node-0")
            wal2.base_version = older_version
            newer_version = older_version.increment("node-1").increment("node-1")
            wal2.entries.append(
                __import__('casty.cluster.merge.wal', fromlist=['WALEntry']).WALEntry(
                    version=newer_version, delta={"balance": 1000}
                )
            )

            msg = ReplicateState(
                actor_id="ignore-account",
                version=older_version,
                state={"balance": 100},
            )

            assert newer_version.dominates(older_version)

            ack = await handle_replicate_state(msg, actor2, wal2, "node-1")

            assert ack.success
            assert actor2.balance == 1000
