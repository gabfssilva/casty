import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, ActorSystem
from casty.cluster.membership import membership_actor, MemberInfo, MemberState
from casty.cluster.messages import (
    Join, GetAliveMembers, SwimTick, Ping, Ack, ProbeTimeout,
    PingReq, PingReqAck, PingReqTimeout, MembershipUpdate, ApplyUpdate,
)
from casty.cluster.transport_messages import Connect, Transmit


@pytest.mark.asyncio
async def test_swim_sends_ping():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        sent_messages = []

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, address):
                        sent_messages.append(("connect", node_id, address))
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await membership_ref.send(Join(node_id="node-2", address="127.0.0.1:9999"))

        await swim_ref.send(SwimTick())
        await asyncio.sleep(0.05)

        assert len(sent_messages) > 0
        assert sent_messages[0][1] == "node-2"


@pytest.mark.asyncio
async def test_swim_no_ping_when_no_other_members():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        sent_messages = []

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, address):
                        sent_messages.append(("connect", node_id, address))
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await swim_ref.send(SwimTick())
        await asyncio.sleep(0.05)

        assert len(sent_messages) == 0


@pytest.mark.asyncio
async def test_swim_handles_ping_and_replies_with_ack():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        updates = [MembershipUpdate(node_id="node-3", status="alive", incarnation=1)]
        ack = await swim_ref.ask(Ping(updates=updates))

        assert isinstance(ack, Ack)


@pytest.mark.asyncio
async def test_swim_applies_updates_from_ping():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        updates = [MembershipUpdate(node_id="node-2", status="down", incarnation=1)]
        await swim_ref.send(Ping(updates=updates))
        await asyncio.sleep(0.05)

        all_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" not in all_members


@pytest.mark.asyncio
async def test_swim_probe_timeout_marks_member_down():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await swim_ref.send(SwimTick())
        await asyncio.sleep(0.01)

        await swim_ref.send(ProbeTimeout(target="node-2"))
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" not in alive_members


@pytest.mark.asyncio
async def test_swim_ack_clears_pending_probe():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
            }),
            name="membership"
        )

        transmitted = []

        @actor
        async def mock_connection(*, mailbox: Mailbox[Transmit]):
            async for msg, ctx in mailbox:
                transmitted.append(msg)

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            nonlocal mock_conn_ref
            async for msg, ctx in mailbox:
                mock_conn_ref = await ctx.actor(mock_connection(), name="conn")
                await ctx.reply(mock_conn_ref)

        mock_conn_ref = None
        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await swim_ref.send(SwimTick())
        await asyncio.sleep(0.01)

        await swim_ref.send(Ack(updates=[]), sender="node-2")
        await asyncio.sleep(0.01)

        await swim_ref.send(ProbeTimeout(target="node-2"))
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" in alive_members


@pytest.mark.asyncio
async def test_swim_handles_ping_req():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({}),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        result = await swim_ref.ask(PingReq(target="node-3", updates=[]))

        assert isinstance(result, PingReqAck)
        assert result.target == "node-3"


@pytest.mark.asyncio
async def test_swim_ping_req_timeout_marks_member_down():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await swim_ref.send(SwimTick())
        await asyncio.sleep(0.01)

        await swim_ref.send(PingReqTimeout(target="node-2"))
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" not in alive_members


@pytest.mark.asyncio
async def test_swim_successful_ping_req_ack_clears_pending():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await swim_ref.send(SwimTick())
        await asyncio.sleep(0.01)

        await swim_ref.send(PingReqAck(target="node-2", success=True, updates=[]))
        await asyncio.sleep(0.01)

        await swim_ref.send(PingReqTimeout(target="node-2"))
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" in alive_members


@pytest.mark.asyncio
async def test_swim_probe_timeout_ignores_unknown_target():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        await swim_ref.send(ProbeTimeout(target="node-unknown"))
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" in alive_members


@pytest.mark.asyncio
async def test_swim_multiple_members_picks_random():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                ),
                "node-3": MemberInfo(
                    node_id="node-3",
                    address="127.0.0.1:8003",
                    state=MemberState.ALIVE,
                    incarnation=0,
                ),
            }),
            name="membership"
        )

        probed_nodes = set()

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                match msg:
                    case Connect(node_id, _):
                        probed_nodes.add(node_id)
                        await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        for _ in range(20):
            await swim_ref.send(SwimTick())
            await asyncio.sleep(0.01)

        assert probed_nodes == {"node-2", "node-3"}


@pytest.mark.asyncio
async def test_swim_applies_multiple_updates_from_ping():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                ),
                "node-3": MemberInfo(
                    node_id="node-3",
                    address="127.0.0.1:8003",
                    state=MemberState.ALIVE,
                    incarnation=0,
                ),
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        updates = [
            MembershipUpdate(node_id="node-2", status="down", incarnation=1),
            MembershipUpdate(node_id="node-3", status="down", incarnation=1),
        ]
        await swim_ref.send(Ping(updates=updates))
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-2" not in alive_members
        assert "node-3" not in alive_members


@pytest.mark.asyncio
async def test_swim_ack_applies_updates():
    from casty.cluster.swim import swim_actor

    async with ActorSystem(node_id="node-1") as system:
        membership_ref = await system.actor(
            membership_actor({
                "node-2": MemberInfo(
                    node_id="node-2",
                    address="127.0.0.1:8002",
                    state=MemberState.ALIVE,
                    incarnation=0,
                ),
                "node-3": MemberInfo(
                    node_id="node-3",
                    address="127.0.0.1:8003",
                    state=MemberState.ALIVE,
                    incarnation=0,
                ),
            }),
            name="membership"
        )

        @actor
        async def mock_outbound(*, mailbox: Mailbox[Connect]):
            async for msg, ctx in mailbox:
                await ctx.reply(None)

        outbound_ref = await system.actor(mock_outbound(), name="outbound")

        swim_ref = await system.actor(
            swim_actor(
                node_id="node-1",
                membership_ref=membership_ref,
                outbound_ref=outbound_ref,
                probe_interval=0.1,
                probe_timeout=0.05,
                ping_req_fanout=2,
            ),
            name="swim"
        )

        updates = [
            MembershipUpdate(node_id="node-3", status="down", incarnation=1),
        ]
        await swim_ref.send(Ack(updates=updates), sender="node-2")
        await asyncio.sleep(0.05)

        alive_members = await membership_ref.ask(GetAliveMembers())
        assert "node-3" not in alive_members
        assert "node-2" in alive_members