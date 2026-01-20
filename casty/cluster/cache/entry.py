from dataclasses import dataclass

from casty import actor, Mailbox, State
from casty.serializable import serializable


@serializable
@dataclass
class Set:
    value: bytes
    ttl: float | None = None


@serializable
@dataclass
class Get:
    pass


@serializable
@dataclass
class Delete:
    pass


@serializable
@dataclass
class Exists:
    pass


@dataclass
class Expire:
    pass


CacheMsg = Set | Get | Delete | Exists | Expire


@actor
async def cache_entry(state: State[bytes | None], mailbox: Mailbox[CacheMsg]):
    async for msg, ctx in mailbox:
        match msg:
            case Set(new_value, ttl):
                state.set(new_value)

                if ttl is not None:
                    await ctx.schedule(Expire(), delay=ttl)

                await ctx.reply(True)

            case Get():
                await ctx.reply(state.value)

            case Delete():
                state.set(None)

            case Exists():
                await ctx.reply(state.value is not None)

            case Expire():
                state.set(None)
