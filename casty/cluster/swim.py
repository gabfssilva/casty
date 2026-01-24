from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any

from casty import actor, Mailbox
from casty.protocols import System
from casty.remote import Connect, Lookup, Connected, LookupResult
from casty.reply import Reply
from .constants import REMOTE_ACTOR_ID, MEMBERSHIP_ACTOR_ID, SWIM_NAME
from .messages import (
    Ping, Ack, PingReq, PingReqAck,
    MemberSnapshot, SwimTick, ProbeTimeout, PingReqTimeout,
    GetAliveMembers, GetAllMembers, MergeMembership, MarkDown, MarkAlive,
)
from .membership import MemberInfo


def to_snapshots(members: dict[str, MemberInfo]) -> list[MemberSnapshot]:
    return [
        MemberSnapshot(m.node_id, m.address, m.state.value, m.incarnation)
        for m in members.values()
    ]


@dataclass
class _PendingSend:
    message: Any


@actor
async def swim_actor(
    node_id: str,
    probe_interval: float = 1.0,
    probe_timeout: float = 2.0,
    ping_req_fanout: int = 3,
    *,
    mailbox: Mailbox[SwimTick | Ping | Ack | PingReq | PingReqAck | ProbeTimeout | PingReqTimeout | Reply],
    system: System,
):
    pending_probes: dict[str, float] = {}
    pending_sends: dict[str, _PendingSend] = {}

    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)

    if membership_ref is None or remote_ref is None:
        return

    await mailbox.schedule(SwimTick(), every=probe_interval)

    async def get_member_snapshots() -> list[MemberSnapshot]:
        all_members = await membership_ref.ask(GetAllMembers())
        return to_snapshots(all_members)

    async def initiate_send(address: str, actor_name: str, message: Any) -> None:
        if remote_ref is None:
            return
        pending_sends[address] = _PendingSend(message=message)
        host, port = address.rsplit(":", 1)
        await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())
        await remote_ref.send(Lookup(actor_name, peer=address), sender=mailbox.ref())

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
                await initiate_send(
                    target_info.address,
                    SWIM_NAME,
                    Ping(sender=node_id, members=snapshots),
                )
                await ctx.schedule(ProbeTimeout(target), delay=probe_timeout)

            case Ping(sender=ping_sender, members=remote_members):
                for m in remote_members:
                    if m.node_id == ping_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ping_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

                snapshots = await get_member_snapshots()
                ack = Ack(sender=node_id, members=snapshots)

                if ctx.reply_to is not None or ctx.sender is not None:
                    await ctx.reply(ack)
                else:
                    members = await membership_ref.ask(GetAliveMembers())
                    if ping_sender in members:
                        sender_info = members[ping_sender]
                        await initiate_send(sender_info.address, SWIM_NAME, ack)

            case Ack(sender=ack_sender, members=remote_members):
                if ack_sender in pending_probes:
                    del pending_probes[ack_sender]
                for m in remote_members:
                    if m.node_id == ack_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ack_sender, m.address))
                        break
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
                    await initiate_send(
                        prober_info.address,
                        SWIM_NAME,
                        PingReq(sender=node_id, target=target, members=snapshots),
                    )

                await ctx.schedule(PingReqTimeout(target), delay=probe_timeout)

            case PingReq(sender=req_sender, target=req_target, members=remote_members):
                for m in remote_members:
                    if m.node_id == req_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(req_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

                members = await membership_ref.ask(GetAliveMembers())
                if req_target in members:
                    target_info = members[req_target]
                    snapshots = await get_member_snapshots()
                    await initiate_send(
                        target_info.address,
                        SWIM_NAME,
                        Ping(sender=node_id, members=snapshots),
                    )

                snapshots = await get_member_snapshots()
                ack = PingReqAck(sender=node_id, target=req_target, success=True, members=snapshots)

                if ctx.reply_to is not None or ctx.sender is not None:
                    await ctx.reply(ack)
                elif req_sender in members:
                    sender_info = members[req_sender]
                    await initiate_send(sender_info.address, SWIM_NAME, ack)

            case PingReqAck(sender=_, target=ack_target, success=success, members=remote_members):
                if success and ack_target in pending_probes:
                    del pending_probes[ack_target]
                    for m in remote_members:
                        if m.node_id == ack_target and m.state == "alive":
                            await membership_ref.send(MarkAlive(ack_target, m.address))
                            break
                await membership_ref.send(MergeMembership(remote_members))

            case PingReqTimeout(target):
                if target in pending_probes:
                    del pending_probes[target]
                    await membership_ref.send(MarkDown(target))

            case Reply(result=Connected()):
                pass

            case Reply(result=Ack() as ack_msg):
                ack_sender = ack_msg.sender
                remote_members = ack_msg.members
                if ack_sender in pending_probes:
                    del pending_probes[ack_sender]
                for m in remote_members:
                    if m.node_id == ack_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ack_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

            case Reply(result=PingReqAck() as ack_msg):
                ack_target = ack_msg.target
                success = ack_msg.success
                remote_members = ack_msg.members
                if success and ack_target in pending_probes:
                    del pending_probes[ack_target]
                    for m in remote_members:
                        if m.node_id == ack_target and m.state == "alive":
                            await membership_ref.send(MarkAlive(ack_target, m.address))
                            break
                await membership_ref.send(MergeMembership(remote_members))

            case Reply(result=LookupResult(ref=ref, peer=peer)) if ref is not None and peer is not None:
                pending = pending_sends.pop(peer, None)
                if pending:
                    await ref.send(pending.message, sender=mailbox.ref())

            case Reply(result=LookupResult(ref=None, peer=peer)) if peer is not None:
                pending_sends.pop(peer, None)

            case Reply():
                pass
