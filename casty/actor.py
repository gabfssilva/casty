from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, get_origin, get_type_hints

from .actor_config import ActorReplicationConfig, WriteQuorum


_actor_registry: dict[str, "Behavior"] = {}


def get_registered_actor(name: str) -> "Behavior | None":
    return _actor_registry.get(name)


def register_behavior(name: str, behavior: "Behavior") -> None:
    _actor_registry[name] = behavior


def clear_actor_registry() -> None:
    _actor_registry.clear()


# Aliases for backward compatibility
get_behavior = get_registered_actor


@dataclass
class Behavior[**P]:
    func: Callable[..., Coroutine[Any, Any, None]]
    initial_kwargs: dict[str, Any] = field(default_factory=dict)
    supervision: Any = None
    state_params: list[str] = field(default_factory=list)
    state_defaults: list[Any] = field(default_factory=list)
    state_initials: list[Any] = field(default_factory=list)
    config_params: list[str] = field(default_factory=list)
    config_defaults: list[Any] = field(default_factory=list)
    config_values: list[Any] = field(default_factory=list)
    system_param: str | None = None
    __replication_config__: "ActorReplicationConfig | None" = None

    @property
    def __name__(self) -> str:
        return self.func.__name__

    @property
    def state_param(self) -> str | None:
        return self.state_params[0] if self.state_params else None

    @property
    def state_initial(self) -> Any:
        return self.state_initials[0] if self.state_initials else None

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> "Behavior[()]":
        total_positional = len(self.state_params) + len(self.config_params)
        all_args = list(args)

        # Fill missing args from defaults (state defaults then config defaults)
        all_defaults = self.state_defaults + self.config_defaults
        if len(all_args) < total_positional:
            missing = total_positional - len(all_args)
            defaults_start = len(all_defaults) - missing
            all_args.extend(all_defaults[defaults_start:])

        # Split args between state and config
        state_initials = all_args[:len(self.state_params)]
        config_values = all_args[len(self.state_params):]

        return Behavior(
            func=self.func,
            initial_kwargs=kwargs,
            supervision=self.supervision,
            state_params=self.state_params,
            state_defaults=self.state_defaults,
            state_initials=state_initials,
            config_params=self.config_params,
            config_defaults=self.config_defaults,
            config_values=config_values,
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
    replicas: int | None = None,
    write_quorum: WriteQuorum = "async",
) -> Behavior[P] | Callable[[Callable[P, Coroutine[Any, Any, None]]], Behavior[P]]:
    def decorator(f: Callable[P, Coroutine[Any, Any, None]]) -> Behavior[P]:
        from .protocols import System
        from .transform import (
            transform_actor_function,
            get_positional_params,
            get_defaults_for_params,
            find_assigned_params,
        )

        replication_config = None
        if clustered or replicas is not None:
            replication_config = ActorReplicationConfig(
                clustered=clustered,
                replicas=replicas,
                write_quorum=write_quorum,
            )

        positional_params = get_positional_params(f)
        state_params = find_assigned_params(f, positional_params)
        config_params = [p for p in positional_params if p not in state_params]
        state_defaults = get_defaults_for_params(f, state_params)
        config_defaults = get_defaults_for_params(f, config_params)
        system_param = _find_param(f, System)
        supervision = getattr(f, "__supervision__", None)

        transformed = transform_actor_function(f, state_params) if state_params else f

        behavior: Behavior[P] = Behavior(
            func=transformed,
            supervision=supervision,
            state_params=state_params,
            state_defaults=state_defaults,
            config_params=config_params,
            config_defaults=config_defaults,
            system_param=system_param,
            __replication_config__=replication_config,
        )

        _actor_registry[f.__name__] = behavior
        return behavior

    if func is not None:
        return decorator(func)
    return decorator
