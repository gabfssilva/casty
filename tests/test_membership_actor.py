# tests/test_membership_actor.py
import pytest
from casty import ActorSystem


@pytest.mark.asyncio
async def test_membership_actor_join():
    from casty.cluster.membership import membership_actor, MemberInfo, MemberState
    from casty.cluster.messages import Join, GetAliveMembers

    async with ActorSystem() as system:
        ref = await system.actor(membership_actor({}), name="membership")

        await ref.send(Join(node_id="node-2", address="localhost:8002"))

        members = await ref.ask(GetAliveMembers())
        assert "node-2" in members


@pytest.mark.asyncio
async def test_membership_actor_apply_update():
    from casty.cluster.membership import membership_actor, MemberInfo, MemberState
    from casty.cluster.messages import Join, ApplyUpdate, MembershipUpdate, GetAliveMembers

    async with ActorSystem() as system:
        ref = await system.actor(membership_actor({}), name="membership")

        await ref.send(Join(node_id="node-2", address="localhost:8002"))

        await ref.send(ApplyUpdate(MembershipUpdate("node-2", "down", 1)))

        members = await ref.ask(GetAliveMembers())
        assert "node-2" not in members
