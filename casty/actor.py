from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, get_origin, get_type_hints

from .actor_config import ActorReplicationConfig, Routing


_actor_registry: dict[str, Callable] = {}


def get_registered_actor(name: str) -> Callable | None:
    return _actor_registry.get(name)


def clear_actor_registry() -> None:
    _actor_registry.clear()


@dataclass
class Behavior[**P]:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_args: tuple[Any, ...] = ()
    initial_kwargs: dict[str, Any] = None  # type: ignore[assignment]
    supervision: Any = None
    state_param: str | None = None
    state_initial: Any = None
    system_param: str | None = None
    __replication_config__: "ActorReplicationConfig | None" = None

    def __post_init__(self) -> None:
        if self.initial_kwargs is None:
            self.initial_kwargs = {}

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
    except Exception:
        pass

    target_name = target.__name__
    annotations = getattr(func, "__annotations__", {})
    for name, annotation in annotations.items():
        if isinstance(annotation, str):
            if generic and annotation.startswith(f"{target_name}["):
                return name
            if not generic and annotation == target_name:
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
