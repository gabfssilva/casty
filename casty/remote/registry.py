from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from .messages import (
    Expose, Unexpose, Lookup,
    Exposed, Unexposed, LookupResult,
)

if TYPE_CHECKING:
    from casty.ref import ActorRef


@dataclass
class SessionConnected:
    session: "ActorRef"


@dataclass
class SessionDisconnected:
    session: "ActorRef"


type RegistryMessage = Expose | Unexpose | Lookup | SessionConnected | SessionDisconnected


@actor
async def registry_actor(*, mailbox: Mailbox[RegistryMessage]):
    exposed: dict[str, ActorRef] = {}

    async for msg, ctx in mailbox:
        match msg:
            case Expose(ref, name):
                exposed[name] = ref
                await ctx.reply(Exposed(name))

            case Unexpose(name):
                exposed.pop(name, None)
                await ctx.reply(Unexposed(name))

            case Lookup(name):
                ref = exposed.get(name)
                await ctx.reply(LookupResult(ref=ref))

            case SessionConnected(_) | SessionDisconnected(_):
                pass
