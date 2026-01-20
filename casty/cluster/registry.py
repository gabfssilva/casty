from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty.actor import Behavior

_behaviors: dict[str, "Behavior"] = {}


def register_behavior(name: str, behavior: "Behavior") -> None:
    _behaviors[name] = behavior


def get_behavior(name: str) -> "Behavior | None":
    return _behaviors.get(name)


def clear_registry() -> None:
    _behaviors.clear()
