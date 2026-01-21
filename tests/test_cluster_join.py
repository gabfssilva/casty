"""Minimal test to debug cluster join propagation."""
import asyncio
import pytest
from casty import ActorSystem, actor, Mailbox
from casty.cluster.membership import membership_actor, MemberInfo, MemberState
from casty.cluster.messages import Join, GetAliveMembers, SetLocalAddress
from casty.remote import remote, Listen, Connect, Lookup, Expose


@pytest.mark.asyncio
async def test_membership_join_local():
    """Test that Join message adds member to local membership."""
    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        # Send Join directly
        await membership_ref.send(Join(node_id="node-2", address="127.0.0.1:9002"))

        # Give it time to process
        await asyncio.sleep(0.1)

        # Query members
        members = await membership_ref.ask(GetAliveMembers())
        print(f"Members: {list(members.keys())}")

        assert "node-2" in members


@pytest.mark.asyncio
async def test_membership_join_via_ask():
    """Test that Join via ask adds member and replies."""
    async with ActorSystem() as system:
        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        # Ask with Join
        response = await membership_ref.ask(Join(node_id="node-2", address="127.0.0.1:9002"))
        print(f"Join response: {response}")

        # Query members
        members = await membership_ref.ask(GetAliveMembers())
        print(f"Members: {list(members.keys())}")

        assert isinstance(response, Join)
        assert response.node_id == "node-1"
        assert "node-2" in members


@pytest.mark.asyncio
async def test_membership_join_via_remote():
    """Test membership join via remote actor communication."""
    async with ActorSystem(node_id="node-1") as system1:
        async with ActorSystem(node_id="node-2") as system2:
            # Node 1 setup
            remote1 = await system1.actor(remote(), name="remote")
            listen_result = await remote1.ask(Listen(port=0, host="127.0.0.1"))
            port1 = listen_result.address[1]
            print(f"Node 1 listening on port {port1}")

            membership1 = await system1.actor(
                membership_actor("node-1"),
                name="membership"
            )
            await membership1.send(SetLocalAddress(f"127.0.0.1:{port1}"))
            await remote1.ask(Expose(ref=membership1, name="membership"))

            # Node 2 setup
            remote2 = await system2.actor(remote(), name="remote")

            # Node 2 connects to Node 1
            await remote2.ask(Connect(host="127.0.0.1", port=port1))
            print("Node 2 connected to Node 1")

            # Node 2 looks up Node 1's membership
            result = await remote2.ask(Lookup("membership", peer=f"127.0.0.1:{port1}"))
            print(f"Lookup result: {result}")
            assert result.ref is not None

            # Node 2 sends Join to Node 1's membership via RemoteRef
            response = await result.ref.ask(Join(node_id="node-2", address="127.0.0.1:9999"))
            print(f"Join response: {response}")

            # Give some time for processing
            await asyncio.sleep(0.1)

            # Query Node 1's membership
            members = await membership1.ask(GetAliveMembers())
            print(f"Node 1 members: {list(members.keys())}")

            assert isinstance(response, Join)
            assert "node-2" in members, f"node-2 not in members: {list(members.keys())}"
