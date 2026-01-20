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
async def cache_entry(*, mailbox: Mailbox[CacheMsg]):
    value: bytes | None = None
    deleted = False

    async for msg, ctx in mailbox:
        if deleted:
            match msg:
                case Get():
                    await ctx.reply(None)
                case Exists():
                    await ctx.reply(False)
            continue

        match msg:
            case Set(new_value, ttl):
                value = new_value
                if ttl is not None:
                    await ctx.schedule(Expire(), delay=ttl)

            case Get():
                await ctx.reply(value)

            case Delete():
                value = None
                deleted = True

            case Exists():
                await ctx.reply(value is not None)

            case Expire():
                value = None
                deleted = True
