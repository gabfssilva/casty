# casty/wal/actor.py
from __future__ import annotations

import time
from dataclasses import dataclass

from casty import actor, Mailbox
from .backend import WALBackend
from .entry import WALEntry


@dataclass
class Append:
    data: bytes


@dataclass
class ReadAll:
    pass


@dataclass
class Snapshot:
    data: bytes


@dataclass
class GetSnapshot:
    pass


@actor
async def wal_actor(
    backend: WALBackend,
    actor_id: str,
    *,
    mailbox: Mailbox[Append | ReadAll | Snapshot | GetSnapshot],
):
    sequence = 0

    async for msg, ctx in mailbox:
        match msg:
            case Append(data):
                sequence += 1
                entry = WALEntry(
                    actor_id=actor_id,
                    sequence=sequence,
                    timestamp=time.time(),
                    data=data,
                )
                await backend.append(entry)

            case ReadAll():
                entries = await backend.read(actor_id)
                await ctx.reply(entries)

            case Snapshot(data):
                await backend.snapshot(actor_id, data)
                sequence = 0

            case GetSnapshot():
                snapshot = await backend.get_snapshot(actor_id)
                await ctx.reply(snapshot)
