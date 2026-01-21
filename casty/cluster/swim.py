from __future__ import annotations

import random
import time

from casty import actor, Mailbox
from casty.protocols import System
from casty.remote import Connect, Lookup
from .messages import (
    Ping, Ack, PingReq, PingReqAck,
    MemberSnapshot, SwimTick, ProbeTimeout, PingReqTimeout,
    GetAliveMembers, GetAllMembers, MergeMembership, MarkDown,
)
from .membership import MemberInfo


def to_snapshots(members: dict[str, MemberInfo]) -> list[MemberSnapshot]:
    return [
        MemberSnapshot(m.node_id, m.address, m.state.value, m.incarnation)
        for m in members.values()
    ]


@actor
async def swim_actor(
    node_id: str,
    probe_interval: float = 1.0,
    probe_timeout: float = 0.5,
    ping_req_fanout: int = 3,
    *,
    mailbox: Mailbox[SwimTick | Ping | Ack | PingReq | PingReqAck | ProbeTimeout | PingReqTimeout],
    system: System,
):
    pending_probes: dict[str, float] = {}

    membership_ref = await system.actor(name="membership_actor/membership")
    remote_ref = await system.actor(name="remote/remote")

    if membership_ref is None or remote_ref is None:
        return

    await mailbox.schedule(SwimTick(), every=probe_interval)

    async def get_member_snapshots() -> list[MemberSnapshot]:
        all_members = await membership_ref.ask(GetAllMembers())
        return to_snapshots(all_members)

    async def send_to_node(address: str, actor_name: str, message) -> bool:
        if remote_ref is None:
            return False
        host, port = address.rsplit(":", 1)
        await remote_ref.ask(Connect(host=host, port=int(port)))
        result = await remote_ref.ask(Lookup(actor_name, peer=address))
        if result.ref:
            await result.ref.send(message)
            return True
        return False

    async for msg, ctx in mailbox:
        match msg:
            case SwimTick():
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id]

                if not other_members:
                    continue

                target = random.choice(other_members)
                target_info = members[target]

                pending_probes[target] = time.time()
                snapshots = await get_member_snapshots()
                await send_to_node(
                    target_info.address,
                    "swim",
                    Ping(sender=node_id, members=snapshots),
                )
                await ctx.schedule(ProbeTimeout(target), delay=probe_timeout)

            case Ping(sender=ping_sender, members=remote_members):
                await membership_ref.send(MergeMembership(remote_members))

                snapshots = await get_member_snapshots()
                ack = Ack(sender=node_id, members=snapshots)

                if ctx.reply_to is not None or ctx.sender is not None:
                    await ctx.reply(ack)
                else:
                    members = await membership_ref.ask(GetAliveMembers())
                    if ping_sender in members:
                        sender_info = members[ping_sender]
                        await send_to_node(sender_info.address, "swim", ack)

            case Ack(sender=ack_sender, members=remote_members):
                if ack_sender in pending_probes:
                    del pending_probes[ack_sender]
                await membership_ref.send(MergeMembership(remote_members))

            case ProbeTimeout(target):
                if target not in pending_probes:
                    continue

                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id and m != target]

                if not other_members:
                    del pending_probes[target]
                    await membership_ref.send(MarkDown(target))
                    continue

                probers = random.sample(other_members, min(ping_req_fanout, len(other_members)))
                snapshots = await get_member_snapshots()
                for prober in probers:
                    prober_info = members[prober]
                    await send_to_node(
                        prober_info.address,
                        "swim",
                        PingReq(sender=node_id, target=target, members=snapshots),
                    )

                await ctx.schedule(PingReqTimeout(target), delay=probe_timeout)

            case PingReq(sender=req_sender, target=req_target, members=remote_members):
                await membership_ref.send(MergeMembership(remote_members))

                success = False
                members = await membership_ref.ask(GetAliveMembers())
                if req_target in members:
                    target_info = members[req_target]
                    snapshots = await get_member_snapshots()
                    success = await send_to_node(
                        target_info.address,
                        "swim",
                        Ping(sender=node_id, members=snapshots),
                    )

                snapshots = await get_member_snapshots()
                ack = PingReqAck(sender=node_id, target=req_target, success=success, members=snapshots)

                if ctx.reply_to is not None or ctx.sender is not None:
                    await ctx.reply(ack)
                elif req_sender in members:
                    sender_info = members[req_sender]
                    await send_to_node(sender_info.address, "swim", ack)

            case PingReqAck(sender=_, target=ack_target, success=success, members=remote_members):
                if success and ack_target in pending_probes:
                    del pending_probes[ack_target]
                await membership_ref.send(MergeMembership(remote_members))

            case PingReqTimeout(target):
                if target in pending_probes:
                    del pending_probes[target]
                    await membership_ref.send(MarkDown(target))
