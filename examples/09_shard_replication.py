"""Shard Replication — event-sourced entities with passive replicas.

Extends cluster sharding with replication: each entity has a primary
and N passive replicas on different nodes. The primary pushes persisted
events to replicas after each command. Replicas maintain their own
journal copy and can be promoted on failover.

    Node 1                   Node 2                   Node 3
    ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
    │ primary "alice"  │     │ replica "alice"  │     │ replica "alice"  │
    │ balance: 700     │────>│ balance: 700     │     │ balance: 700     │
    │                  │──────────────────────────────>│                  │
    │ replica "bob"    │     │ primary "bob"    │     │ replica "bob"    │
    │ balance: 400     │<────│ balance: 400     │────>│ balance: 400     │
    └──────────────────┘     └──────────────────┘     └──────────────────┘

Three nodes, each entity gets 2 passive replicas on different nodes.
Deposits arrive from any node. All replicas stay in sync via event push.

Configuration:
  ReplicationConfig(
      replicas=2,      # 2 passive replicas per entity
      min_acks=0,      # fire-and-forget for low latency
      ack_timeout=5.0, # (unused when min_acks=0)
  )
"""

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, Behaviors
from casty.actor import Behavior
from casty.journal import InMemoryJournal
from casty.replication import ReplicationConfig
from casty.sharding import ClusteredActorSystem, ShardEnvelope

NUM_SHARDS = 10


# --- State ---


@dataclass(frozen=True)
class AccountState:
    balance: int


# --- Events ---


@dataclass(frozen=True)
class Deposited:
    amount: int


@dataclass(frozen=True)
class Withdrawn:
    amount: int


type AccountEvent = Deposited | Withdrawn


# --- Commands ---


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountCommand = Deposit | GetBalance


# --- Event handler ---


def on_event(state: AccountState, event: AccountEvent) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(balance=state.balance + amount)
        case Withdrawn(amount):
            return AccountState(balance=state.balance - amount)
    return state


# --- Command handler ---


async def on_command(ctx: Any, state: AccountState, cmd: AccountCommand) -> Any:
    match cmd:
        case Deposit(amount):
            print(f"  [account] deposit +{amount} (balance will be {state.balance + amount})")
            return Behaviors.persisted(events=[Deposited(amount)])
        case GetBalance(reply_to):
            reply_to.tell(state.balance)
            return Behaviors.same()
    return Behaviors.unhandled()


# --- Entity factory ---

# Shared journal so we can inspect events after the example runs.
# In production, each node would have its own journal (or a shared database).
journal = InMemoryJournal()


def account_entity(entity_id: str) -> Behavior[AccountCommand]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(balance=0),
        on_event=on_event,
        on_command=on_command,
    )


# --- Main ---


async def main() -> None:
    print("=== Shard Replication (3 nodes) ===\n")

    # Three nodes in the same process for demonstration
    async with (
        ClusteredActorSystem(
            name="bank",
            host="127.0.0.1",
            port=25520,
            node_id="node-1",
            seed_nodes=[("127.0.0.1", 25521), ("127.0.0.1", 25522)],
        ) as node1,
        ClusteredActorSystem(
            name="bank",
            host="127.0.0.1",
            port=25521,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", 25520), ("127.0.0.1", 25522)],
        ) as node2,
        ClusteredActorSystem(
            name="bank",
            host="127.0.0.1",
            port=25522,
            node_id="node-3",
            seed_nodes=[("127.0.0.1", 25520), ("127.0.0.1", 25521)],
        ) as node3,
    ):
        # 2 passive replicas per entity — with 3 nodes, every entity
        # has a primary on one node and replicas on the other two.
        repl_config = ReplicationConfig(replicas=2, min_acks=0)

        accounts1 = node1.spawn(
            Behaviors.sharded(account_entity, num_shards=NUM_SHARDS, replication=repl_config),
            "accounts",
        )
        accounts2 = node2.spawn(
            Behaviors.sharded(account_entity, num_shards=NUM_SHARDS, replication=repl_config),
            "accounts",
        )
        accounts3 = node3.spawn(
            Behaviors.sharded(account_entity, num_shards=NUM_SHARDS, replication=repl_config),
            "accounts",
        )
        await asyncio.sleep(0.1)

        # --- Deposits from all three nodes ---
        print("--- Deposits via node 1 ---")
        for name, amount in [("alice", 500), ("bob", 300), ("carol", 100)]:
            accounts1.tell(ShardEnvelope(name, Deposit(amount)))
        await asyncio.sleep(0.3)

        print("\n--- Deposits via node 2 ---")
        accounts2.tell(ShardEnvelope("alice", Deposit(200)))
        accounts2.tell(ShardEnvelope("dave", Deposit(50)))
        await asyncio.sleep(0.3)

        print("\n--- Deposits via node 3 ---")
        accounts3.tell(ShardEnvelope("bob", Deposit(100)))
        accounts3.tell(ShardEnvelope("carol", Deposit(400)))
        accounts3.tell(ShardEnvelope("eve", Deposit(1000)))
        await asyncio.sleep(0.3)

        # --- Query balances from different nodes ---
        print("\n--- Final balances (queried from node 1) ---")
        for name in ["alice", "bob", "carol", "dave", "eve"]:
            balance: int = await node1.ask(
                accounts1,
                lambda r, n=name: ShardEnvelope(n, GetBalance(reply_to=r)),
                timeout=2.0,
            )
            print(f"  {name:>5}: ${balance}")

        # --- Inspect journal ---
        print("\n--- Journal contents ---")
        for name in ["alice", "bob", "carol", "dave", "eve"]:
            events = await journal.load(name)
            event_strs = ", ".join(
                f"+{e.event.amount}" if isinstance(e.event, Deposited) else f"-{e.event.amount}"
                for e in events
            )
            print(f"  {name:>5}: {len(events)} events [{event_strs}]")

    print("\n=== Done ===")


asyncio.run(main())
