# casty/cluster/membership.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from casty import actor, Mailbox
from .messages import Join, ApplyUpdate, GetAliveMembers, GetAllMembers, MembershipUpdate


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
    initial_members: dict[str, MemberInfo],
    *,
    mailbox: Mailbox[Join | ApplyUpdate | GetAliveMembers | GetAllMembers],
):
    members = dict(initial_members)

    async for msg, ctx in mailbox:
        match msg:
            case Join(node_id, address):
                members[node_id] = MemberInfo(
                    node_id=node_id,
                    address=address,
                    state=MemberState.ALIVE,
                    incarnation=0,
                )

            case ApplyUpdate(update):
                if update.node_id in members:
                    member = members[update.node_id]
                    if update.incarnation >= member.incarnation:
                        member.state = MemberState(update.status)
                        member.incarnation = update.incarnation

            case GetAliveMembers():
                alive = {
                    node_id: info
                    for node_id, info in members.items()
                    if info.state == MemberState.ALIVE
                }
                await ctx.reply(alive)

            case GetAllMembers():
                await ctx.reply(dict(members))
