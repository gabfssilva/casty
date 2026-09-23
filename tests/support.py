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
