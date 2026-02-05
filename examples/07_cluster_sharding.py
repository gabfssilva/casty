"""Cluster Sharding — simplified API with ClusteredActorSystem.

Two ClusteredActorSystem nodes in the same process.
Bank accounts as sharded entities. Deposits + balance queries.

    Node 1 (:25520)                  Node 2 (:25521)
    ┌──────────────────┐             ┌──────────────────┐
    │  ClusteredSystem  │             │  ClusteredSystem  │
    │  ┌──────────────┐ │             │  ┌──────────────┐ │
    │  │  accounts    │ │  peer refs  │  │  accounts    │ │
    │  │  (proxy)     │ │ ◄──────────►│  │  (proxy)     │ │
    │  │  _region     │ │             │  │  _region     │ │
    │  │  _coord      │ │             │  └──────────────┘ │
    │  └──────────────┘ │             └──────────────────┘
    └──────────────────┘
"""

import asyncio
from dataclasses import dataclass

from casty import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ShardEnvelope,
)
from casty.sharding import ClusteredActorSystem

NUM_SHARDS = 10


# --- Messages ---


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountMsg = Deposit | GetBalance


# --- Entity behavior (one actor per bank account) ---


def account_entity(entity_id: str) -> Behavior[AccountMsg]:
    def active(balance: int) -> Behavior[AccountMsg]:
        async def receive(
            ctx: ActorContext[AccountMsg], msg: AccountMsg
        ) -> Behavior[AccountMsg]:
            match msg:
                case Deposit(amount):
                    new_balance = balance + amount
                    print(f"  [{entity_id}] deposit +{amount} -> balance={new_balance}")
                    return active(new_balance)
                case GetBalance(reply_to):
                    reply_to.tell(balance)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)


async def main() -> None:
    print("=== Cluster Sharding (ClusteredActorSystem) ===\n")

    async with (
        ClusteredActorSystem(
            name="bank", host="127.0.0.1", port=25520,
            seed_nodes=[("127.0.0.1", 25521)],
        ) as s1,
        ClusteredActorSystem(
            name="bank", host="127.0.0.1", port=25521,
            seed_nodes=[("127.0.0.1", 25520)],
        ) as s2,
    ):
        # spawn returns ActorRef[ShardEnvelope[AccountMsg]]
        accounts1 = s1.spawn(Behaviors.sharded(account_entity, num_shards=NUM_SHARDS), "accounts")
        accounts2 = s2.spawn(Behaviors.sharded(account_entity, num_shards=NUM_SHARDS), "accounts")
        await asyncio.sleep(0.1)

        # --- Deposits via node 1 ---
        print("--- Deposits (via node 1) ---")
        for i, name in enumerate(["alice", "bob", "carol", "dave", "eve"]):
            accounts1.tell(ShardEnvelope(name, Deposit(100 * (i + 1))))
        await asyncio.sleep(0.3)

        # --- Deposits via node 2 ---
        print("\n--- Deposits (via node 2) ---")
        accounts2.tell(ShardEnvelope("alice", Deposit(500)))
        accounts2.tell(ShardEnvelope("eve", Deposit(500)))
        await asyncio.sleep(0.3)

        # --- Query balances ---
        print("\n--- Final balances ---")
        for name in ["alice", "bob", "carol", "dave", "eve"]:
            balance: int = await s1.ask(
                accounts1,
                lambda r, n=name: ShardEnvelope(n, GetBalance(reply_to=r)),
                timeout=2.0,
            )
            print(f"  {name:>5}: ${balance}")

    print("\n=== Done ===")


asyncio.run(main())
