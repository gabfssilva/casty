from dataclasses import dataclass
from time import time

from casty import Actor, Context

from ..serializable import serializable


@serializable
@dataclass(frozen=True, slots=True)
class Set:
    value: bytes
    ttl: float | None = None


@serializable
@dataclass(frozen=True, slots=True)
class Get:
    pass


@serializable
@dataclass(frozen=True, slots=True)
class Delete:
    pass


@serializable
@dataclass(frozen=True, slots=True)
class Exists:
    pass


@serializable
@dataclass(frozen=True, slots=True)
class Expire:
    pass


@dataclass
class CacheEntry(Actor[Set | Get | Delete | Exists | Expire]):
    value: bytes | None = None
    created_at: float | None = None
    ttl_schedule_id: str | None = None

    async def receive(self, msg: Set | Get | Delete | Exists | Expire, ctx: Context):
        match msg:
            case Set(value, ttl):
                self.value = value
                self.created_at = time()
                if self.ttl_schedule_id is not None:
                    await ctx.cancel_schedule(self.ttl_schedule_id)
                    self.ttl_schedule_id = None
                if ttl is not None:
                    self.ttl_schedule_id = await ctx.schedule(ttl, Expire())
            case Get():
                await ctx.reply(self.value)
            case Delete():
                await ctx.stop()
            case Exists():
                await ctx.reply(self.value is not None)
            case Expire():
                await ctx.stop()
