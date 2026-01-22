# casty/cluster/membership.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from casty import actor, Mailbox
from .messages import Join, MergeMembership, MarkDown, MemberSnapshot, GetAliveMembers, GetAllMembers, GetResponsibleNodes, SetLocalAddress, GetAddress
from .hash_ring import HashRing


class MemberState(Enum):
    ALIVE = "alive"
    DOWN = "down"


@dataclass
class MemberInfo:
    node_id: str
    address: str
    state: MemberState
    incarnation: int


@actor
async def membership_actor(
    node_id: str,
    initial_members: dict[str, MemberInfo] | None = None,
    *,
    mailbox: Mailbox[Join | MergeMembership | MarkDown | GetAliveMembers | GetAllMembers | GetResponsibleNodes | SetLocalAddress | GetAddress],
):
    members = dict(initial_members or {})
    hash_ring = HashRing()
    hash_ring.add_node(node_id)
    local_address = ""

    for member_id, info in members.items():
        if info.state == MemberState.ALIVE:
            hash_ring.add_node(member_id)

    async for msg, ctx in mailbox:
        match msg:
            case SetLocalAddress(addr):
                local_address = addr

            case Join(joined_node_id, address):
                is_new = joined_node_id not in members
                members[joined_node_id] = MemberInfo(
                    node_id=joined_node_id,
                    address=address,
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
                hash_ring.add_node(joined_node_id)

                if is_new and local_address:
                    await ctx.reply(Join(node_id=node_id, address=local_address))

            case MergeMembership(remote_members):
                for snapshot in remote_members:
                    if snapshot.node_id == node_id:
                        continue

                    if snapshot.node_id not in members:
                        members[snapshot.node_id] = MemberInfo(
                            node_id=snapshot.node_id,
                            address=snapshot.address,
                            state=MemberState(snapshot.state),
                            incarnation=snapshot.incarnation,
                        )
                        if snapshot.state == "alive":
                            hash_ring.add_node(snapshot.node_id)
                    else:
                        local = members[snapshot.node_id]
                        if snapshot.incarnation > local.incarnation:
                            old_state = local.state
                            local.state = MemberState(snapshot.state)
                            local.incarnation = snapshot.incarnation
                            local.address = snapshot.address

                            match (local.state, old_state):
                                case (MemberState.DOWN, MemberState.ALIVE):
                                    hash_ring.remove_node(snapshot.node_id)
                                case (MemberState.ALIVE, MemberState.DOWN):
                                    hash_ring.add_node(snapshot.node_id)

            case MarkDown(target_node_id):
                if target_node_id in members:
                    member = members[target_node_id]
                    if member.state == MemberState.ALIVE:
                        member.state = MemberState.DOWN
                        member.incarnation += 1
                        hash_ring.remove_node(target_node_id)

            case GetAliveMembers():
                await ctx.reply({
                    mid: info for mid, info in members.items()
                    if info.state == MemberState.ALIVE
                })

            case GetAllMembers():
                await ctx.reply(dict(members))

            case GetResponsibleNodes(actor_id, count):
                try:
                    await ctx.reply(hash_ring.get_nodes(actor_id, count))
                except RuntimeError:
                    await ctx.reply([])

            case GetAddress(target_node_id):
                if target_node_id in members:
                    await ctx.reply(members[target_node_id].address)
                else:
                    await ctx.reply(None)
