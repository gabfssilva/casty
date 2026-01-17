"""Distributed system example for Casty.

Demonstrates:
- Singleton actors (cluster-wide unique)
- Sharded actors (distributed entities)

Run with:
    uv run python examples/03-distributed.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, Context, ActorSystem

# Messages
@dataclass
class Deposit:
    amount: float


@dataclass
class Withdraw:
    amount: float


@dataclass
class GetBalance:
    pass


@dataclass
class CacheGet:
    key: str


@dataclass
class CacheSet:
    key: str
    value: str


# Singleton cache manager (only one in the cluster)
class CacheManager(Actor[CacheGet | CacheSet]):
    """Cluster-wide unique cache manager.

    Only one instance exists in the entire cluster,
    providing a centralized cache service.
    """

    def __init__(self):
        self.cache: dict[str, str] = {}
        print("[CacheManager] Initialized")

    async def receive(self, msg: CacheGet | CacheSet, ctx: Context) -> None:
        match msg:
            case CacheGet(key):
                value = self.cache.get(key)
                print(f"[CacheManager] GET {key} -> {value}")
                await ctx.reply(value)
            case CacheSet(key, value):
                self.cache[key] = value
                print(f"[CacheManager] SET {key} = {value}")


# Sharded account actor (distributed by entity_id)
class Account(Actor[Deposit | Withdraw | GetBalance]):
    """Bank account distributed across the cluster.

    Each account (entity_id) is assigned to a specific node
    using consistent hashing for even distribution.
    """

    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0.0
        print(f"[Account:{entity_id}] Created with balance ${self.balance:.2f}")

    async def receive(
        self, msg: Deposit | Withdraw | GetBalance, ctx: Context
    ) -> None:
        match msg:
            case Deposit(amount):
                self.balance += amount
                print(f"[Account:{self.entity_id}] Deposited ${amount:.2f}, balance: ${self.balance:.2f}")

            case Withdraw(amount):
                if amount <= self.balance:
                    self.balance -= amount
                    print(f"[Account:{self.entity_id}] Withdrew ${amount:.2f}, balance: ${self.balance:.2f}")
                    await ctx.reply(True)
                else:
                    print(f"[Account:{self.entity_id}] Insufficient funds for ${amount:.2f}")
                    await ctx.reply(False)

            case GetBalance():
                await ctx.reply(self.balance)


async def main():
    print("=" * 60)
    print("Casty Distributed System Example")
    print("=" * 60)
    print()

    async with ActorSystem.clustered("127.0.0.1", 0) as system:
        # Wait for Raft election (single node becomes leader)
        await asyncio.sleep(1.0)
        print(f"Node: {system.node_id}")
        print()

        # Create singleton cache (cluster-wide unique)
        print("Creating singleton cache manager...")
        cache = await system.spawn(
            CacheManager,
            name="global-cache",
        )

        # Use the cache
        await cache.send(CacheSet("user:1:name", "Alice"))
        await cache.send(CacheSet("user:2:name", "Bob"))
        await asyncio.sleep(0.1)

        name1 = await cache.ask(CacheGet("user:1:name"))
        print(f"Retrieved user:1:name = {name1}")
        print()

        # Create sharded accounts (distributed entities)
        print("Creating sharded accounts...")
        accounts = await system.spawn(
            Account,
            name="accounts",
            sharded=True,
            replication_factor=3,
        )

        # Operate on different accounts
        # Each account is a separate entity that could be on different nodes
        print()
        print("Operating on accounts...")

        await accounts["alice"].send(Deposit(1000.0))
        await accounts["bob"].send(Deposit(500.0))
        await accounts["charlie"].send(Deposit(250.0))
        await asyncio.sleep(0.1)

        # Transfer from Alice to Bob
        await accounts["alice"].send(Withdraw(200.0))
        await accounts["bob"].send(Deposit(200.0))
        await asyncio.sleep(0.1)

        # Check balances
        print()
        print("Final balances:")
        alice_balance = await accounts["alice"].ask(GetBalance())
        bob_balance = await accounts["bob"].ask(GetBalance())
        charlie_balance = await accounts["charlie"].ask(GetBalance())

        print(f"  Alice:   ${alice_balance:.2f}")
        print(f"  Bob:     ${bob_balance:.2f}")
        print(f"  Charlie: ${charlie_balance:.2f}")
        print(f"  Total:   ${alice_balance + bob_balance + charlie_balance:.2f}")

        print()
        print("=" * 60)
        print("Example completed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
