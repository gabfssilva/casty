from __future__ import annotations

from casty import Actor, Context

from .messages import CacheEntry, CacheHit, CacheMiss, Delete, Expire, Get, Ok, Set


class CacheActor(Actor[Get | Set | Delete | Expire]):
    def __init__(self):
        self.entries: dict[str, CacheEntry] = {}

    async def receive(self, msg: Get | Set | Delete | Expire, ctx: Context):
        match msg:
            case Set(key, value, ttl):
                if key in self.entries and self.entries[key].ttl_task_id:
                    await ctx.cancel_schedule(self.entries[key].ttl_task_id)

                ttl_task_id = None
                if ttl:
                    ttl_task_id = await ctx.schedule(ttl, Expire(key))

                self.entries[key] = CacheEntry(value, ttl_task_id)
                await ctx.reply(Ok())

            case Get(key):
                entry = self.entries.get(key)
                await ctx.reply(CacheMiss() if entry is None else CacheHit(entry.value))

            case Delete(key):
                if key in self.entries:
                    entry = self.entries.pop(key)
                    if entry.ttl_task_id:
                        await ctx.cancel_schedule(entry.ttl_task_id)
                await ctx.reply(Ok())

            case Expire(key):
                self.entries.pop(key, None)
