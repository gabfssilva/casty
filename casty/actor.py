from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine
from functools import wraps

from .mailbox import Mailbox


@dataclass
class Behavior:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_args: tuple[Any, ...]
    initial_kwargs: dict[str, Any]
    supervision: Any = None


def actor[M](
    func: Callable[..., Coroutine[Any, Any, None]]
) -> Callable[..., Behavior]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Behavior:
        supervision = getattr(func, "__supervision__", None)
        return Behavior(
            func=func,
            initial_args=args,
            initial_kwargs=kwargs,
            supervision=supervision,
        )

    return wrapper
