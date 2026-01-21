from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, get_origin, get_type_hints
from functools import wraps


@dataclass
class Behavior:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_args: tuple[Any, ...]
    initial_kwargs: dict[str, Any]
    supervision: Any = None
    state_param: str | None = None
    state_initial: Any = None
    system_param: str | None = None


def _find_param(func: Callable[..., Any], target: type, *, generic: bool = False) -> str | None:
    try:
        hints = get_type_hints(func)
        for name, annotation in hints.items():
            if generic and get_origin(annotation) is target:
                return name
            if not generic and annotation is target:
                return name
    except Exception:
        pass
    return None


def actor(
    func: Callable[..., Coroutine[Any, Any, None]]
) -> Callable[..., Behavior]:
    from .state import State
    from .protocols import System

    state_param = _find_param(func, State, generic=True)
    system_param = _find_param(func, System)

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
            system_param=system_param,
        )

    return wrapper
