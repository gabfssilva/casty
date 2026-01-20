# casty/cluster/membership.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from casty import actor, Mailbox
from .messages import Join, ApplyUpdate, GetAliveMembers, GetAllMembers, GetResponsibleNodes, SetLocalAddress
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
    mailbox: Mailbox[Join | ApplyUpdate | GetAliveMembers | GetAllMembers | GetResponsibleNodes | SetLocalAddress],
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

            case ApplyUpdate(update):
                if update.node_id in members:
                    member = members[update.node_id]
                    if update.incarnation >= member.incarnation:
                        old_state = member.state
                        member.state = MemberState(update.status)
                        member.incarnation = update.incarnation

                        if member.state == MemberState.DOWN and old_state == MemberState.ALIVE:
                            hash_ring.remove_node(update.node_id)
                        elif member.state == MemberState.ALIVE and old_state == MemberState.DOWN:
                            hash_ring.add_node(update.node_id)

            case GetAliveMembers():
                alive = {
                    member_id: info
                    for member_id, info in members.items()
                    if info.state == MemberState.ALIVE
                }
                await ctx.reply(alive)

            case GetAllMembers():
                await ctx.reply(dict(members))

            case GetResponsibleNodes(actor_id, count):
                try:
                    nodes = hash_ring.get_nodes(actor_id, count)
                    await ctx.reply(nodes)
                except RuntimeError:
                    await ctx.reply([])
