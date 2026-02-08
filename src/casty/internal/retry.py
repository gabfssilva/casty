"""Internal retry decorator for async functions. Not part of the public API."""

from __future__ import annotations

import asyncio
import enum
import functools
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger("casty.internal.retry")


class Backoff(enum.Enum):
    fixed = enum.auto()
    exponential = enum.auto()


def retry[**P, R](
    *,
    max_retries: int = 3,
    delay: float = 0.1,
    max_delay: float = 10.0,
    backoff: Backoff = Backoff.exponential,
    on: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_exc: BaseException | None = None
            for attempt in range(1 + max_retries):
                try:
                    return await fn(*args, **kwargs)
                except on as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        match backoff:
                            case Backoff.fixed:
                                wait = delay
                            case Backoff.exponential:
                                wait = delay * (2**attempt)
                        wait = min(wait, max_delay)
                        logger.debug(
                            "%s attempt %d/%d failed (%s), retrying in %.2fs",
                            fn.__qualname__,
                            attempt + 1,
                            max_retries + 1,
                            exc,
                            wait,
                        )
                        await asyncio.sleep(wait)
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
