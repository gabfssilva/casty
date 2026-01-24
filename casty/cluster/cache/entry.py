from dataclasses import dataclass

from casty import actor, Mailbox
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
async def cache_entry(data: bytes | None, *, mailbox: Mailbox[CacheMsg]):
    async for msg, ctx in mailbox:
        match msg:
            case Set(new_value, ttl):
                data = new_value

                if ttl is not None:
                    await ctx.schedule(Expire(), delay=ttl)

                await ctx.reply(True)

            case Get():
                await ctx.reply(data)

            case Delete():
                data = None

            case Exists():
                await ctx.reply(data is not None)

            case Expire():
                data = None
