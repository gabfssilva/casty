from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, get_origin, get_args
from functools import wraps

from .mailbox import Mailbox


@dataclass
class Behavior:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_args: tuple[Any, ...]
    initial_kwargs: dict[str, Any]
    supervision: Any = None
    state_param: str | None = None
    state_initial: Any = None


def _get_state_param(func: Callable[..., Any]) -> str | None:
    from typing import get_type_hints

    from .state import State

    try:
        hints = get_type_hints(func)
        for name, annotation in hints.items():
            origin = get_origin(annotation)
            if origin is State:
                return name
    except Exception:
        sig = inspect.signature(func)
        for name, param in sig.parameters.items():
            annotation = param.annotation
            if annotation is inspect.Parameter.empty:
                continue
            if isinstance(annotation, str) and annotation.startswith("State["):
                return name

    return None


def actor[M](
    func: Callable[..., Coroutine[Any, Any, None]]
) -> Callable[..., Behavior]:
    state_param = _get_state_param(func)

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Behavior:
        supervision = getattr(func, "__supervision__", None)

        state_initial = None
        remaining_args = args

        if state_param is not None:
            if state_param in kwargs:
                state_initial = kwargs.pop(state_param)
            elif args:
                state_initial = args[0]
                remaining_args = args[1:]

        return Behavior(
            func=func,
            initial_args=remaining_args,
            initial_kwargs=kwargs,
            supervision=supervision,
            state_param=state_param,
            state_initial=state_initial,
        )

    return wrapper
