from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, get_origin, get_type_hints
from functools import wraps

from .actor_config import ActorReplicationConfig, Routing


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
    func: Callable[..., Coroutine[Any, Any, None]] | None = None,
    *,
    clustered: bool = False,
    replicated: int | None = None,
    persistence: Any = None,
    routing: dict[type, Any] | None = None,
) -> Callable[..., Behavior] | Callable[[Callable[..., Coroutine[Any, Any, None]]], Callable[..., Behavior]]:
    def decorator(f: Callable[..., Coroutine[Any, Any, None]]) -> Callable[..., Behavior]:
        from .state import State
        from .protocols import System

        if clustered or replicated is not None or persistence is not None or routing is not None:
            f.__replication_config__ = ActorReplicationConfig(
                clustered=clustered or (replicated is not None),
                replicated=replicated,
                persistence=persistence,
                routing=routing or {},
            )

        state_param = _find_param(f, State, generic=True)
        system_param = _find_param(f, System)

        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Behavior:
            supervision = getattr(f, "__supervision__", None)

            state_initial = None
            remaining_args = args

            if state_param is not None:
                if state_param in kwargs:
                    state_initial = kwargs.pop(state_param)
                elif args:
                    state_initial = args[0]
                    remaining_args = args[1:]

            return Behavior(
                func=f,
                initial_args=remaining_args,
                initial_kwargs=kwargs,
                supervision=supervision,
                state_param=state_param,
                state_initial=state_initial,
                system_param=system_param,
            )

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
