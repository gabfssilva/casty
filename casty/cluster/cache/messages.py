from dataclasses import dataclass

from ..serializable import serializable


@serializable
@dataclass(frozen=True, slots=True)
class Get:
    key: str


@serializable
@dataclass(frozen=True, slots=True)
class Set:
    key: str
    value: bytes
    ttl: float | None = None


@serializable
@dataclass(frozen=True, slots=True)
class Delete:
    key: str


@serializable
@dataclass(frozen=True, slots=True)
class Expire:
    key: str


@serializable
@dataclass(frozen=True, slots=True)
class CacheHit:
    value: bytes


@serializable
@dataclass(frozen=True, slots=True)
class CacheMiss:
    pass


@serializable
@dataclass(frozen=True, slots=True)
class Ok:
    pass


@dataclass(slots=True)
class CacheEntry:
    value: bytes
    ttl_task_id: str | None = None
