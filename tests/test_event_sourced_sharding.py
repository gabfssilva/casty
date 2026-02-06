# tests/test_event_sourced_sharding.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorSystem, Behaviors, ActorRef, Behavior
from casty.journal import InMemoryJournal, PersistedEvent
from casty.sharding import ClusteredActorSystem, ShardEnvelope
from casty.cluster_state import NodeAddress


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountMsg = Deposit | GetBalance


@dataclass(frozen=True)
class Deposited:
    amount: int


@dataclass(frozen=True)
class AccountState:
    balance: int


def apply_event(state: AccountState, event: Deposited) -> AccountState:
    return AccountState(balance=state.balance + event.amount)


async def handle_command(ctx: Any, state: AccountState, cmd: AccountMsg) -> Any:
    match cmd:
        case Deposit(amount):
            return Behaviors.persisted(events=[Deposited(amount)])
        case GetBalance(reply_to):
            reply_to.tell(state.balance)
            return Behaviors.same()
    return Behaviors.unhandled()


# Shared journal so we can inspect events after test
_journal = InMemoryJournal()


def account_entity(entity_id: str) -> Behavior[AccountMsg]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=_journal,
        initial_state=AccountState(balance=0),
        on_event=apply_event,
        on_command=handle_command,
    )


async def test_event_sourced_sharding_workflow() -> None:
    global _journal
    _journal = InMemoryJournal()

    async with ClusteredActorSystem(
        name="bank", host="127.0.0.1", port=25520
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=account_entity, num_shards=10),
            "accounts",
        )
        await asyncio.sleep(0.1)

        proxy.tell(ShardEnvelope("alice", Deposit(100)))
        proxy.tell(ShardEnvelope("alice", Deposit(50)))
        proxy.tell(ShardEnvelope("bob", Deposit(200)))
        await asyncio.sleep(0.3)

        balance_alice = await system.ask(
            proxy,
            lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance_alice == 150

        balance_bob = await system.ask(
            proxy,
            lambda r: ShardEnvelope("bob", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance_bob == 200

    # Verify events were persisted
    alice_events = await _journal.load("alice")
    assert len(alice_events) == 2
    bob_events = await _journal.load("bob")
    assert len(bob_events) == 1


async def test_sharded_with_replication_config() -> None:
    """Verify that sharded entities accept ReplicationConfig."""
    from casty.replication import ReplicationConfig

    _local_journal = InMemoryJournal()

    def replicated_account(entity_id: str) -> Behavior[AccountMsg]:
        return Behaviors.event_sourced(
            entity_id=entity_id,
            journal=_local_journal,
            initial_state=AccountState(balance=0),
            on_event=apply_event,
            on_command=handle_command,
        )

    async with ClusteredActorSystem(
        name="bank-repl", host="127.0.0.1", port=25530
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(
                entity_factory=replicated_account,
                num_shards=10,
                replication=ReplicationConfig(replicas=1),
            ),
            "accounts",
        )
        await asyncio.sleep(0.1)

        proxy.tell(ShardEnvelope("alice", Deposit(100)))
        await asyncio.sleep(0.2)

        balance = await system.ask(
            proxy,
            lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        assert balance == 100
