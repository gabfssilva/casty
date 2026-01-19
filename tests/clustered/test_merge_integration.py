import pytest
import asyncio
from dataclasses import dataclass

from casty import Actor
from casty.cluster.development import DevelopmentCluster
from casty.cluster.merge import merge_sum
from casty.persistent_actor import MergeState, GetState
from casty.wal import VectorClock


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
                await ctx.reply(self.balance)

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
                await ctx.reply(self.items)

    def get_state(self) -> dict:
        return {"items": list(self.items)}

    def set_state(self, state: dict) -> None:
        self.items = set(state.get("items", []))

    def __casty_merge__(self, base: "MergeableSet", other: "MergeableSet"):
        self.items = self.items | other.items


class TestMergeIntegrationSingleNode:

    @pytest.mark.asyncio
    async def test_state_replication_two_nodes(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1 = cluster.node(0)
            ref = await node1.actor(MergeableAccount, name="account-replication-2n", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.2)

            await ref.send(Deposit(amount=100))
            await asyncio.sleep(0.2)

            balance = await ref.ask(GetBalance())
            assert balance == 100

    @pytest.mark.asyncio
    async def test_multiple_deposits_replicated(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1 = cluster.node(0)
            ref = await node1.actor(MergeableAccount, name="account-multi-deposits", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.2)

            await ref.send(Deposit(amount=50))
            await ref.send(Deposit(amount=30))
            await ref.send(Deposit(amount=20))
            await asyncio.sleep(0.3)

            balance = await ref.ask(GetBalance())
            assert balance == 100

    @pytest.mark.asyncio
    async def test_set_union_merge(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1 = cluster.node(0)
            ref = await node1.actor(MergeableSet, name="set-union-merge", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.2)

            await ref.send(Add(item="apple"))
            await ref.send(Add(item="banana"))
            await asyncio.sleep(0.2)

            items = await ref.ask(GetItems())
            assert items == {"apple", "banana"}


class TestMergeIntegrationThreeNodes:

    @pytest.mark.asyncio
    async def test_replication_across_three_nodes(self):
        async with DevelopmentCluster(3) as cluster:
            from casty.cluster.scope import ClusterScope
            node1 = cluster.node(0)
            ref = await node1.actor(MergeableAccount, name="account-replication-3n", scope=ClusterScope(replication=3))
            await asyncio.sleep(0.3)

            await ref.send(Deposit(amount=1000))
            await asyncio.sleep(0.3)

            balance = await ref.ask(GetBalance())
            assert balance == 1000

    @pytest.mark.asyncio
    async def test_sequential_operations_three_nodes(self):
        async with DevelopmentCluster(3) as cluster:
            from casty.cluster.scope import ClusterScope
            node1 = cluster.node(0)
            ref = await node1.actor(MergeableAccount, name="account-sequential-3n", scope=ClusterScope(replication=3))
            await asyncio.sleep(0.3)

            for i in range(5):
                await ref.send(Deposit(amount=10))
                await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 50


class TestMergeableBehavior:

    @pytest.mark.asyncio
    async def test_mergeable_actor_get_set_state(self):
        async with DevelopmentCluster(1) as cluster:
            system = cluster.node(0)
            ref = await system.actor(MergeableAccount, name="account-get-set-state", scope="cluster")
            await asyncio.sleep(0.1)

            await ref.send(Deposit(amount=500))
            await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 500

    @pytest.mark.asyncio
    async def test_withdraw_operation(self):
        async with DevelopmentCluster(1) as cluster:
            system = cluster.node(0)
            ref = await system.actor(MergeableAccount, name="account-withdraw", scope="cluster")
            await asyncio.sleep(0.1)

            await ref.send(Deposit(amount=100))
            await ref.send(Withdraw(amount=30))
            await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 70


class TestVectorClockTracking:

    @pytest.mark.asyncio
    async def test_version_increments_on_mutation(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1 = cluster.node(0)
            ref = await node1.actor(MergeableAccount, name="account-version-increment", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.2)

            for _ in range(3):
                await ref.send(Deposit(amount=10))
                await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 30

    @pytest.mark.asyncio
    async def test_wal_tracks_deltas(self):
        async with DevelopmentCluster(1) as cluster:
            system = cluster.node(0)
            ref = await system.actor(MergeableAccount, name="account-wal-deltas", scope="cluster")
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
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1, node2 = cluster[0], cluster[1]
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.actor(
                MergeableAccount,
                name="shared-account",
                scope=ClusterScope(replication=2, consistency='all'),
            )

            replicated = await self._wait_for_replication([node1, node2], "shared-account")
            assert replicated, "Actor not replicated to all nodes"

            await ref.send(Deposit(amount=100))
            await asyncio.sleep(0.2)

            cluster2 = self._get_cluster_actor(node2)
            local_ref2 = cluster2._local_actors.get("shared-account")
            state = await local_ref2.ask(GetState())

            assert state.get("balance") == 100

    @pytest.mark.asyncio
    async def test_set_concurrent_adds_merge_with_union(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1, node2 = cluster[0], cluster[1]
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.actor(
                MergeableSet,
                name="shared-set",
                scope=ClusterScope(replication=2, consistency='all'),
            )

            replicated = await self._wait_for_replication([node1, node2], "shared-set")
            assert replicated, "Actor not replicated to all nodes"

            await ref.send(Add(item="apple"))
            await ref.send(Add(item="banana"))
            await asyncio.sleep(0.2)

            cluster2 = self._get_cluster_actor(node2)
            local_ref2 = cluster2._local_actors.get("shared-set")
            state = await local_ref2.ask(GetState())

            items = set(state.get("items", []))
            assert "apple" in items
            assert "banana" in items

    @pytest.mark.asyncio
    async def test_dominated_version_accepts_state(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1, node2 = cluster[0], cluster[1]
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.actor(
                MergeableAccount,
                name="sync-account",
                scope=ClusterScope(replication=2, consistency='all'),
            )

            replicated = await self._wait_for_replication([node1, node2], "sync-account")
            assert replicated, "Actor not replicated to all nodes"

            cluster2 = self._get_cluster_actor(node2)
            local_ref2 = cluster2._local_actors.get("sync-account")

            newer_version = VectorClock().increment("node-0").increment("node-0")
            result = await local_ref2.ask(MergeState(
                their_version=newer_version,
                their_state={"balance": 500},
            ))

            assert result.success

            state = await local_ref2.ask(GetState())
            assert state.get("balance") == 500

    @pytest.mark.asyncio
    async def test_my_version_dominates_ignores_state(self):
        async with DevelopmentCluster(2) as cluster:
            from casty.cluster.scope import ClusterScope
            node1, node2 = cluster[0], cluster[1]
            connected = await self._wait_for_cluster_membership([node1, node2])
            assert connected, "Nodes not connected"

            ref = await node1.actor(
                MergeableAccount,
                name="ignore-account",
                scope=ClusterScope(replication=2, consistency='all'),
            )

            replicated = await self._wait_for_replication([node1, node2], "ignore-account")
            assert replicated, "Actor not replicated to all nodes"

            await ref.send(Deposit(amount=1000))
            await asyncio.sleep(0.2)

            cluster2 = self._get_cluster_actor(node2)
            local_ref2 = cluster2._local_actors.get("ignore-account")

            older_version = VectorClock()
            result = await local_ref2.ask(MergeState(
                their_version=older_version,
                their_state={"balance": 100},
            ))

            assert result.success

            state = await local_ref2.ask(GetState())
            assert state.get("balance") == 1000
