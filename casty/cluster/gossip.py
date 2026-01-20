from __future__ import annotations

import random

from casty import actor, Mailbox
from casty.ref import ActorRef
from casty.envelope import Envelope
from casty.serializable import serialize
from .messages import StateUpdate, StatePull, GossipTick, GetAliveMembers
from .transport_messages import Connect, Transmit


@actor
async def gossip_actor(
    node_id: str,
    membership_ref: ActorRef,
    outbound_ref: ActorRef,
    fanout: int,
    *,
    mailbox: Mailbox[StateUpdate | StatePull | GossipTick],
):
    pending_updates: dict[str, StateUpdate] = {}

    async for msg, ctx in mailbox:
        match msg:
            case GossipTick():
                if not pending_updates:
                    continue

                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id]

                if not other_members:
                    continue

                targets = random.sample(
                    other_members,
                    min(fanout, len(other_members))
                )

                for target in targets:
                    target_info = members[target]
                    conn = await outbound_ref.ask(Connect(
                        node_id=target,
                        address=target_info.address,
                    ))
                    if conn:
                        for update in pending_updates.values():
                            envelope = Envelope(
                                payload=update,
                                target="gossip_actor/gossip",
                                sender=node_id,
                            )
                            await conn.send(Transmit(data=serialize(envelope)))

                pending_updates.clear()

            case StateUpdate(actor_id, sequence, state):
                current = pending_updates.get(actor_id)
                if current is None or sequence > current.sequence:
                    pending_updates[actor_id] = msg

            case StatePull(actor_id, since_sequence):
                update = pending_updates.get(actor_id)
                if update and update.sequence > since_sequence:
                    await ctx.reply(update)
                else:
                    await ctx.reply(None)
