import pytest
import asyncio
from dataclasses import dataclass

from casty import Actor
from casty.cluster.development import DevelopmentCluster
from casty.cluster.scope import ClusterScope


@dataclass
class Deposit:
    amount: int


@dataclass
class Withdraw:
    amount: int


@dataclass
class GetBalance:
    pass


class Account(Actor[Deposit | Withdraw | GetBalance]):

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


@dataclass
class Add:
    item: str


@dataclass
class GetItems:
    pass


class ItemSet(Actor[Add | GetItems]):

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


class TestClusteredRefAsk:

    @pytest.mark.asyncio
    async def test_clustered_ref_ask_single_node(self):
        """Test ClusteredRef.ask() on single node - simplest case."""
        async with DevelopmentCluster(1) as cluster:
            node1 = cluster[0]

            ref = await node1.actor(Account, name="account-single", scope="cluster")
            await asyncio.sleep(0.2)

            await ref.send(Deposit(amount=100))
            await asyncio.sleep(0.1)

            balance = await ref.ask(GetBalance())
            assert balance == 100, f"Expected 100, got {balance}"

    @pytest.mark.asyncio
    async def test_clustered_ref_ask_two_nodes(self):
        """Test ClusteredRef.ask() with 2-node cluster."""
        async with DevelopmentCluster(2) as cluster:
            node1 = cluster[0]

            ref = await node1.actor(Account, name="account-two", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.3)

            await ref.send(Deposit(amount=200))
            await asyncio.sleep(0.2)

            balance = await ref.ask(GetBalance())
            assert balance == 200, f"Expected 200, got {balance}"

    @pytest.mark.asyncio
    async def test_local_spawn_ask_works(self):
        """Verify local spawn ask works (baseline)."""
        async with DevelopmentCluster(1) as cluster:
            node1 = cluster[0]

            local_ref = await node1.spawn(Account)
            await local_ref.send(Deposit(amount=50))

            balance = await local_ref.ask(GetBalance())
            assert balance == 50, f"Expected 50, got {balance}"


class TestReplication:

    @pytest.mark.asyncio
    async def test_replication_two_nodes(self):
        async with DevelopmentCluster(2) as cluster:
            node1 = cluster[0]

            ref = await node1.actor(Account, name="account-repl", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.3)

            await ref.send(Deposit(amount=100))
            await asyncio.sleep(0.2)

            balance = await ref.ask(GetBalance())
            assert balance == 100

    @pytest.mark.asyncio
    async def test_multiple_deposits(self):
        async with DevelopmentCluster(2) as cluster:
            node1 = cluster[0]

            ref = await node1.actor(Account, name="account-multi", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.3)

            for i in range(5):
                await ref.send(Deposit(amount=10))
                await asyncio.sleep(0.05)

            await asyncio.sleep(0.2)

            balance = await ref.ask(GetBalance())
            assert balance == 50

    @pytest.mark.asyncio
    async def test_set_items(self):
        async with DevelopmentCluster(2) as cluster:
            node1 = cluster[0]

            ref = await node1.actor(ItemSet, name="set-items", scope=ClusterScope(replication=2))
            await asyncio.sleep(0.3)

            await ref.send(Add(item="apple"))
            await ref.send(Add(item="banana"))
            await asyncio.sleep(0.2)

            items = await ref.ask(GetItems())
            assert items == {"apple", "banana"}
