import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta


async def eventually(check: Callable[[], Awaitable[None]], within: timedelta = timedelta(seconds=5)) -> None:
    """Run `check` until it returns without raising; after `within`, raise what it last raised."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within.total_seconds()
    last: Exception | None = None
    while True:
        try:
            async with asyncio.timeout_at(deadline):
                await check()
            return
        except Exception as failure:
            # The deadline cancels the call in progress and `timeout_at` puts a `TimeoutError` of its own in its
            # place, which says only that the time ran out. What names the thing that never became true is the
            # failure before it, so that one is what is kept.
            last = last if last is not None and isinstance(failure, TimeoutError) else failure
            if loop.time() >= deadline:
                raise last from None
        await asyncio.sleep(0.01)


class Records:
    """A store in memory, kept as `casty.Store` asks: of the saves of a key, the one of the greatest version stays.

    `failing` makes every save raise, and `saves` counts the ones that went through.
    """

    def __init__(self) -> None:
        self.records: dict[tuple[str, str], tuple[bytes, bytes | None]] = {}
        self.saves = 0
        self.failing = False

    async def load(self, actor: str, key: str, /) -> tuple[bytes, bytes | None] | None:
        await asyncio.sleep(0)
        return self.records.get((actor, key))

    async def save(self, actor: str, key: str, version: bytes, state: bytes | None, /) -> None:
        await asyncio.sleep(0)
        if self.failing:
            raise OSError("the disk is full")
        self.saves += 1
        kept = self.records.get((actor, key))
        if kept is None or kept[0] <= version:
            self.records[(actor, key)] = (version, state)

    async def drop(self, actor: str, key: str, version: bytes, /) -> None:
        await asyncio.sleep(0)
        kept = self.records.get((actor, key))
        if kept is not None and kept[0] <= version:
            del self.records[(actor, key)]
