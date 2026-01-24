from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

from casty import actor, Mailbox
from casty.protocols import System
from casty.serializable import serializable
from ..logger import debug as log_debug
from casty.reply import Reply
from casty.remote import Connect, Connected, Lookup, LookupResult
from .constants import REMOTE_ACTOR_ID, MEMBERSHIP_ACTOR_ID, GOSSIP_NAME
from .messages import GetAliveMembers


@serializable
@dataclass
class Put:
    key: str
    value: bytes
    version: int = 0  # 0 means auto-increment


@dataclass
class Get:
    key: str


@dataclass
class _Tick:
    pass


GossipMessage = Put | Get | _Tick | Reply[Any]


@actor
async def gossip_actor(
    node_id: str,
    fanout: int = 3,
    tick_interval: float = 0.5,
    *,
    mailbox: Mailbox[GossipMessage],
    system: System,
):
    store: dict[str, tuple[bytes, int]] = {}  # key -> (value, version)

    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)

    await mailbox.schedule(_Tick(), every=tick_interval)

    async for msg, ctx in mailbox:
        match msg:
            case _Tick():
                if not store or membership_ref is None:
                    continue
                await membership_ref.send(GetAliveMembers(), sender=mailbox.ref())

            case Reply(result=members) if isinstance(members, dict):
                if not store or remote_ref is None:
                    continue

                other_members = [m for m in members if m != node_id]
                if not other_members:
                    continue

                targets = random.sample(
                    other_members,
                    min(fanout, len(other_members))
                )

                for t in targets:
                    addr = members[t].address
                    host, port = addr.rsplit(":", 1)
                    await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())
                    await remote_ref.send(Lookup(GOSSIP_NAME, peer=addr), sender=mailbox.ref())

            case Reply(result=Connected()):
                pass

            case Reply(result=LookupResult(ref=ref)) if ref is not None:
                for key, (value, version) in store.items():
                    await ref.send(Put(key, value, version))

            case Reply(result=LookupResult(ref=None)):
                pass

            case Put(key, value, version):
                current = store.get(key)
                current_version = current[1] if current else 0

                if version == 0:
                    new_version = current_version + 1
                    store[key] = (value, new_version)
                    log_debug("Gossip put", f"gossip/{node_id}", key=key, version=new_version)
                elif version > current_version:
                    store[key] = (value, version)
                    log_debug("Gossip replicated", f"gossip/{node_id}", key=key, version=version)

            case Get(key):
                entry = store.get(key)
                await ctx.reply(entry[0] if entry else None)
