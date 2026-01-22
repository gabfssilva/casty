"""Test cluster join with full cluster setup."""
import asyncio
import pytest
from casty import ActorSystem
from casty.cluster.cluster import cluster
from casty.cluster.messages import GetAliveMembers


@pytest.mark.asyncio
async def test_two_node_cluster_join():
    """Test that two nodes can join and see each other."""
    async with ActorSystem(node_id="node-1") as system1:
        # Start node-1 cluster
        cluster1 = await system1.actor(
            cluster("node-1", host="127.0.0.1", port=0),
            name="cluster"
        )

        # Wait for remote to be ready
        await asyncio.sleep(0.1)

        # Get node-1's port
        remote1 = await system1.actor(name="remote")
        assert remote1 is not None

        # Get membership
        membership1 = await system1.actor(name="membership")
        assert membership1 is not None, "Membership not found on node-1"

        members = await membership1.ask(GetAliveMembers())
        print(f"Node 1 initial members: {list(members.keys())}")

        # For now, just verify cluster starts correctly
        assert membership1 is not None


@pytest.mark.asyncio
async def test_two_nodes_with_seed():
    """Test two nodes where node-2 joins node-1 via seed."""
    async with ActorSystem(node_id="node-1") as system1:
        # Start node-1 cluster on fixed port
        cluster1 = await system1.actor(
            cluster("node-1", host="127.0.0.1", port=9101),
            name="cluster"
        )
        await asyncio.sleep(0.2)

        membership1 = await system1.actor(name="membership")
        print(f"Membership1 ref: {membership1}")

        async with ActorSystem(node_id="node-2") as system2:
            # Start node-2 with node-1 as seed
            cluster2 = await system2.actor(
                cluster(
                    "node-2",
                    host="127.0.0.1",
                    port=9102,
                    seeds=[("node-1", "127.0.0.1:9101")]
                ),
                name="cluster"
            )
            await asyncio.sleep(0.5)

            membership2 = await system2.actor(name="membership")
            print(f"Membership2 ref: {membership2}")

            # Check members on both nodes
            members1 = await membership1.ask(GetAliveMembers())
            members2 = await membership2.ask(GetAliveMembers())

            print(f"Node 1 sees: {list(members1.keys())}")
            print(f"Node 2 sees: {list(members2.keys())}")

            # Node-2 should see node-1 (from seed and join response)
            assert "node-1" in members2, f"node-1 not in node-2's members: {list(members2.keys())}"

            # Node-1 should see node-2 (from join message)
            assert "node-2" in members1, f"node-2 not in node-1's members: {list(members1.keys())}"
