import pytest
import asyncio

from casty import ActorSystem, actor, Mailbox


@pytest.mark.asyncio
async def test_gossip_stores_updates():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor
    from casty.cluster.messages import StateUpdate, GossipTick
    from casty.cluster.transport_messages import Connect

    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect():
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=1,
            state=b"count=10",
        ))

        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_gossip_keeps_latest_update():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor
    from casty.cluster.messages import StateUpdate, StatePull
    from casty.cluster.transport_messages import Connect

    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect():
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=1,
            state=b"count=10",
        ))

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=5,
            state=b"count=50",
        ))

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=3,
            state=b"count=30",
        ))

        result = await gossip_ref.ask(StatePull(
            actor_id="counter/c1",
            since_sequence=0,
        ))

        assert result is not None
        assert result.sequence == 5
        assert result.state == b"count=50"


@pytest.mark.asyncio
async def test_gossip_pull_returns_none_if_no_update():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor
    from casty.cluster.messages import StatePull
    from casty.cluster.transport_messages import Connect

    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect():
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        result = await gossip_ref.ask(StatePull(
            actor_id="counter/c1",
            since_sequence=0,
        ))

        assert result is None


@pytest.mark.asyncio
async def test_gossip_pull_returns_none_if_not_newer():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor
    from casty.cluster.messages import StateUpdate, StatePull
    from casty.cluster.transport_messages import Connect

    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect():
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=5,
            state=b"count=50",
        ))

        result = await gossip_ref.ask(StatePull(
            actor_id="counter/c1",
            since_sequence=5,
        ))

        assert result is None

        result = await gossip_ref.ask(StatePull(
            actor_id="counter/c1",
            since_sequence=10,
        ))

        assert result is None


@pytest.mark.asyncio
async def test_gossip_tick_sends_to_random_members():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor, MemberInfo, MemberState
    from casty.cluster.messages import StateUpdate, GossipTick
    from casty.cluster.transport_messages import Connect, Transmit

    transmissions = []
    connections_requested = []

    async with ActorSystem() as system:
        initial_members = {
            "node-1": MemberInfo("node-1", "127.0.0.1:8001", MemberState.ALIVE, 0),
            "node-2": MemberInfo("node-2", "127.0.0.1:8002", MemberState.ALIVE, 0),
            "node-3": MemberInfo("node-3", "127.0.0.1:8003", MemberState.ALIVE, 0),
        }

        membership_ref = await system.actor(
            membership_actor(initial_members),
            name="membership"
        )

        @actor
        async def mock_connection(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Transmit(data):
                        transmissions.append(data)

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, address):
                        connections_requested.append((node_id, address))
                        conn_ref = await ctx.actor(mock_connection(), name=f"conn-{node_id}")
                        await ctx.reply(conn_ref)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=1,
            state=b"count=10",
        ))

        await gossip_ref.send(GossipTick())

        await asyncio.sleep(0.1)

        assert len(connections_requested) == 2
        assert all(node_id != "node-1" for node_id, _ in connections_requested)
        assert len(transmissions) == 2


@pytest.mark.asyncio
async def test_gossip_tick_with_no_pending_updates():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor, MemberInfo, MemberState
    from casty.cluster.messages import GossipTick
    from casty.cluster.transport_messages import Connect

    connections_requested = []

    async with ActorSystem() as system:
        initial_members = {
            "node-1": MemberInfo("node-1", "127.0.0.1:8001", MemberState.ALIVE, 0),
            "node-2": MemberInfo("node-2", "127.0.0.1:8002", MemberState.ALIVE, 0),
        }

        membership_ref = await system.actor(
            membership_actor(initial_members),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, address):
                        connections_requested.append((node_id, address))
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(GossipTick())

        await asyncio.sleep(0.05)

        assert len(connections_requested) == 0


@pytest.mark.asyncio
async def test_gossip_tick_with_no_other_members():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor, MemberInfo, MemberState
    from casty.cluster.messages import StateUpdate, GossipTick, StatePull
    from casty.cluster.transport_messages import Connect

    connections_requested = []

    async with ActorSystem() as system:
        initial_members = {
            "node-1": MemberInfo("node-1", "127.0.0.1:8001", MemberState.ALIVE, 0),
        }

        membership_ref = await system.actor(
            membership_actor(initial_members),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, address):
                        connections_requested.append((node_id, address))
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=1,
            state=b"count=10",
        ))

        await gossip_ref.send(GossipTick())

        await asyncio.sleep(0.05)

        assert len(connections_requested) == 0

        result = await gossip_ref.ask(StatePull(
            actor_id="counter/c1",
            since_sequence=0,
        ))
        assert result is not None
        assert result.sequence == 1


@pytest.mark.asyncio
async def test_gossip_clears_pending_after_tick():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor, MemberInfo, MemberState
    from casty.cluster.messages import StateUpdate, GossipTick, StatePull
    from casty.cluster.transport_messages import Connect, Transmit

    async with ActorSystem() as system:
        initial_members = {
            "node-1": MemberInfo("node-1", "127.0.0.1:8001", MemberState.ALIVE, 0),
            "node-2": MemberInfo("node-2", "127.0.0.1:8002", MemberState.ALIVE, 0),
        }

        membership_ref = await system.actor(
            membership_actor(initial_members),
            name="membership"
        )

        @actor
        async def mock_connection(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                pass

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, address):
                        conn_ref = await ctx.actor(mock_connection(), name=f"conn-{node_id}")
                        await ctx.reply(conn_ref)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=1,
            state=b"count=10",
        ))

        await gossip_ref.send(GossipTick())

        await asyncio.sleep(0.05)

        result = await gossip_ref.ask(StatePull(
            actor_id="counter/c1",
            since_sequence=0,
        ))
        assert result is None


@pytest.mark.asyncio
async def test_gossip_multiple_actors():
    from casty.cluster.gossip import gossip_actor
    from casty.cluster.membership import membership_actor
    from casty.cluster.messages import StateUpdate, StatePull
    from casty.cluster.transport_messages import Connect

    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect():
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        gossip_ref = await system.actor(
            gossip_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                fanout=2,
            ),
            name="gossip"
        )

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c1",
            sequence=1,
            state=b"count=10",
        ))

        await gossip_ref.send(StateUpdate(
            actor_id="counter/c2",
            sequence=3,
            state=b"count=30",
        ))

        await gossip_ref.send(StateUpdate(
            actor_id="timer/t1",
            sequence=2,
            state=b"elapsed=100",
        ))

        result1 = await gossip_ref.ask(StatePull(actor_id="counter/c1", since_sequence=0))
        result2 = await gossip_ref.ask(StatePull(actor_id="counter/c2", since_sequence=0))
        result3 = await gossip_ref.ask(StatePull(actor_id="timer/t1", since_sequence=0))

        assert result1.sequence == 1
        assert result1.state == b"count=10"

        assert result2.sequence == 3
        assert result2.state == b"count=30"

        assert result3.sequence == 2
        assert result3.state == b"elapsed=100"
