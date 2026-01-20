from __future__ import annotations

from dataclasses import dataclass

from .serializable import serializable


def message[T](cls: type[T] | None = None, *, readonly: bool = False) -> type[T]:
    def decorator(cls: type[T]) -> type[T]:
        cls = dataclass(cls)
        cls = serializable(cls)
        cls.__readonly__ = readonly
        return cls

    if cls is None:
        return decorator
    return decorator(cls)
