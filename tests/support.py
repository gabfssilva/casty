import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta


async def eventually(check: Callable[[], Awaitable[None]], within: timedelta = timedelta(seconds=5)) -> None:
    """Run `check` until it returns without raising; after `within`, raise what it last raised."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within.total_seconds()
    while True:
        try:
            async with asyncio.timeout_at(deadline):
                await check()
            return
        except Exception:
            if loop.time() >= deadline:
                raise
        await asyncio.sleep(0.01)
