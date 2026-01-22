from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, get_origin, get_type_hints

from .actor_config import ActorReplicationConfig, Routing


_actor_registry: dict[str, "Behavior"] = {}


def get_registered_actor(name: str) -> "Behavior | None":
    return _actor_registry.get(name)


def register_behavior(name: str, behavior: "Behavior") -> None:
    _actor_registry[name] = behavior


def clear_actor_registry() -> None:
    _actor_registry.clear()


# Aliases for backward compatibility
get_behavior = get_registered_actor
clear_registry = clear_actor_registry


@dataclass
class Behavior[**P]:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_args: tuple[Any, ...] = ()
    initial_kwargs: dict[str, Any] = field(default_factory=dict)
    supervision: Any = None
    state_param: str | None = None
    state_initial: Any = None
    system_param: str | None = None
    __replication_config__: "ActorReplicationConfig | None" = None

    @property
    def __name__(self) -> str:
        return self.func.__name__

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> "Behavior[()]":
        state_initial = self.state_initial
        remaining_args = args

        if self.state_param is not None:
            if self.state_param in kwargs:
                state_initial = kwargs.pop(self.state_param)
            elif args:
                state_initial = args[0]
                remaining_args = args[1:]

        return Behavior(
            func=self.func,
            initial_args=remaining_args,
            initial_kwargs=kwargs,
            supervision=self.supervision,
            state_param=self.state_param,
            state_initial=state_initial,
            system_param=self.system_param,
            __replication_config__=self.__replication_config__,
        )


def _find_param(func: Callable[..., Any], target: type, *, generic: bool = False) -> str | None:
    try:
        hints = get_type_hints(func)
        for name, annotation in hints.items():
            if generic and get_origin(annotation) is target:
                return name
            if not generic and annotation is target:
                return name
    except (NameError, AttributeError, TypeError):
        pass

    target_name = target.__name__
    annotations = getattr(func, "__annotations__", {})
    for name, annotation in annotations.items():
        match annotation:
            case str() if generic and annotation.startswith(f"{target_name}["):
                return name
            case str() if not generic and annotation == target_name:
                return name

    return None


def actor[**P](
    func: Callable[P, Coroutine[Any, Any, None]] | None = None,
    *,
    clustered: bool = False,
    replicated: int | None = None,
    persistence: Any = None,
    routing: dict[type, Any] | None = None,
) -> Behavior[P] | Callable[[Callable[P, Coroutine[Any, Any, None]]], Behavior[P]]:
    def decorator(f: Callable[P, Coroutine[Any, Any, None]]) -> Behavior[P]:
        from .state import State
        from .protocols import System

        replication_config = None
        if clustered or replicated is not None or persistence is not None or routing is not None:
            replication_config = ActorReplicationConfig(
                clustered=clustered or (replicated is not None),
                replicated=replicated,
                persistence=persistence,
                routing=routing or {},
            )

        state_param = _find_param(f, State, generic=True)
        system_param = _find_param(f, System)
        supervision = getattr(f, "__supervision__", None)

        behavior: Behavior[P] = Behavior(
            func=f,
            supervision=supervision,
            state_param=state_param,
            system_param=system_param,
            __replication_config__=replication_config,
        )

        _actor_registry[f.__name__] = behavior
        return behavior

    if func is not None:
        return decorator(func)
    return decorator
