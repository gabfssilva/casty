import asyncio
import pytest
from dataclasses import dataclass

from casty import actor, Mailbox, message
from casty.state import State
from casty.cluster import DevelopmentCluster
from casty.messages import GetLeaderId, GetAliveMembers


@message
class Inc:
    amount: int = 1


@message
class Get:
    pass


@message
class SetValue:
    value: int


@pytest.mark.asyncio
async def test_state_recovery_after_leader_failure():
    """State should be recovered when leader node fails and new leader is elected"""

    @actor(replicas=3, write_quorum="all")
    async def counter(state: State[int], *, mailbox: Mailbox[Inc | Get | SetValue]):
        async for msg, ctx in mailbox:
            match msg:
                case Inc(amount):
                    state.set(state.value + amount)
                case SetValue(value):
                    state.set(value)
                case Get():
                    await ctx.reply(state.value)

    async with asyncio.timeout(30):
        async with DevelopmentCluster(nodes=3) as cluster:
            node0 = cluster[0]
            ref = await node0.actor(counter(State(0)), name="counter")

            await ref.send(SetValue(100))
            await asyncio.sleep(0.1)

            result = await ref.ask(Get())
            assert result == 100, f"Initial state should be 100, got {result}"

            await ref.send(Inc(50))
            await asyncio.sleep(0.1)

            result = await ref.ask(Get())
            assert result == 150, f"State after increment should be 150, got {result}"

            membership_ref = await node0._system.actor(name="membership")
            leader_id = await membership_ref.ask(GetLeaderId(actor_id="counter", replicas=3))
            assert leader_id is not None, "Should have a leader"

            leader_index = int(leader_id.split("-")[1])

            survivor_indices = [i for i in range(3) if i != leader_index]
            survivor_node = cluster[survivor_indices[0]]

            await cluster.decluster(leader_id)

            await asyncio.sleep(15)

            survivor_ref = await survivor_node.actor(counter(State(0)), name="counter")

            result = await survivor_ref.ask(Get())
            assert result == 150, f"State should be preserved after leader failure, got {result}"

            await survivor_ref.send(Inc(25))
            await asyncio.sleep(0.1)

            result = await survivor_ref.ask(Get())
            assert result == 175, f"New leader should be able to process messages, got {result}"


@pytest.mark.asyncio
async def test_multiple_node_failures_with_quorum():
    """Cluster should continue operating as long as quorum is maintained"""
    from casty.messages import GetResponsibleNodes

    @actor(replicas=3, write_quorum="quorum")
    async def counter(state: State[int], *, mailbox: Mailbox[Inc | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Inc(amount):
                    state.set(state.value + amount)
                case Get():
                    await ctx.reply(state.value)

    async with asyncio.timeout(25):
        async with DevelopmentCluster(nodes=5) as cluster:
            node0 = cluster[0]
            ref = await node0.actor(counter(State(0)), name="counter")

            for _ in range(10):
                await ref.send(Inc(1))
            await asyncio.sleep(0.5)

            result = await ref.ask(Get())
            assert result == 10, f"Initial state should be 10, got {result}"

            membership_ref = await node0._system.actor(name="membership")
            responsible_nodes = await membership_ref.ask(GetResponsibleNodes(actor_id="counter", count=3))
            responsible_indices = {int(n.split("-")[1]) for n in responsible_nodes}
            non_responsible = [i for i in range(5) if i not in responsible_indices]

            await cluster.decluster(f"node-{non_responsible[0]}")
            await cluster.decluster(f"node-{non_responsible[1]}")

            await ref.send(Inc(5))
            await asyncio.sleep(0.3)

            result = await ref.ask(Get())
            assert result == 15, f"State should be 15 after node failures, got {result}"
