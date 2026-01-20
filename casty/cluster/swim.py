from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.envelope import Envelope
from casty.serializable import serialize
from .messages import (
    Ping, Ack, PingReq, PingReqAck,
    MembershipUpdate, SwimTick, ProbeTimeout, PingReqTimeout,
    GetAliveMembers, ApplyUpdate,
)
from .transport_messages import Connect, Transmit

if TYPE_CHECKING:
    from casty.ref import ActorRef


@actor
async def swim_actor(
    node_id: str,
    membership_ref: "ActorRef",
    outbound_ref: "ActorRef",
    probe_interval: float,
    probe_timeout: float,
    ping_req_fanout: int,
    *,
    mailbox: Mailbox[SwimTick | Ping | Ack | PingReq | PingReqAck | ProbeTimeout | PingReqTimeout],
):
    pending_updates: list[MembershipUpdate] = []
    pending_probes: dict[str, float] = {}

    await mailbox.schedule(SwimTick(), every=probe_interval)

    async for msg, ctx in mailbox:
        match msg:
            case SwimTick():
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id]

                if not other_members:
                    continue

                target = random.choice(other_members)
                target_info = members[target]

                conn = await outbound_ref.ask(Connect(
                    node_id=target,
                    address=target_info.address,
                ))
                pending_probes[target] = time.time()
                if conn:
                    envelope = Envelope(
                        payload=Ping(updates=list(pending_updates)),
                        target=f"swim_actor/swim",
                        sender=node_id,
                    )
                    await conn.send(Transmit(data=serialize(envelope)))
                await ctx.schedule(ProbeTimeout(target), delay=probe_timeout)

            case Ping(updates):
                for update in updates:
                    await membership_ref.send(ApplyUpdate(update))

                await ctx.reply(Ack(updates=list(pending_updates)))

            case Ack(updates):
                if ctx.sender and ctx.sender in pending_probes:
                    del pending_probes[ctx.sender]

                for update in updates:
                    await membership_ref.send(ApplyUpdate(update))

            case ProbeTimeout(target):
                if target not in pending_probes:
                    continue

                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id and m != target]

                if not other_members:
                    del pending_probes[target]
                    update = MembershipUpdate(
                        node_id=target,
                        status="down",
                        incarnation=0,
                    )
                    await membership_ref.send(ApplyUpdate(update))
                    pending_updates.append(update)
                    continue

                probers = random.sample(other_members, min(ping_req_fanout, len(other_members)))
                for prober in probers:
                    prober_info = members[prober]
                    conn = await outbound_ref.ask(Connect(
                        node_id=prober,
                        address=prober_info.address,
                    ))
                    if conn:
                        envelope = Envelope(
                            payload=PingReq(target=target, updates=list(pending_updates)),
                            target=f"swim_actor/swim",
                            sender=node_id,
                        )
                        await conn.send(Transmit(data=serialize(envelope)))

                await ctx.schedule(PingReqTimeout(target), delay=probe_timeout)

            case PingReq(target, updates):
                for update in updates:
                    await membership_ref.send(ApplyUpdate(update))

                members = await membership_ref.ask(GetAliveMembers())
                if target in members:
                    target_info = members[target]
                    conn = await outbound_ref.ask(Connect(
                        node_id=target,
                        address=target_info.address,
                    ))
                    if conn:
                        envelope = Envelope(
                            payload=Ping(updates=[]),
                            target=f"swim_actor/swim",
                            sender=node_id,
                        )
                        await conn.send(Transmit(data=serialize(envelope)))
                        await ctx.reply(PingReqAck(target=target, success=True, updates=list(pending_updates)))
                    else:
                        await ctx.reply(PingReqAck(target=target, success=False, updates=list(pending_updates)))
                else:
                    await ctx.reply(PingReqAck(target=target, success=False, updates=list(pending_updates)))

            case PingReqAck(target, success, updates):
                if success and target in pending_probes:
                    del pending_probes[target]
                for update in updates:
                    await membership_ref.send(ApplyUpdate(update))

            case PingReqTimeout(target):
                if target in pending_probes:
                    del pending_probes[target]
                    update = MembershipUpdate(
                        node_id=target,
                        status="down",
                        incarnation=0,
                    )
                    await membership_ref.send(ApplyUpdate(update))
                    pending_updates.append(update)
