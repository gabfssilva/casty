from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable

from casty.serializable import serializable


class Routing(Enum):
    LEADER = "leader"
    ANY = "any"
    LOCAL_FIRST = "local"


@serializable
@dataclass
class ReplicationConfig:
    factor: int = 3
    write_quorum: int = 2
    routing: Routing = Routing.LEADER


def replicated[T](
    factor: int = 3,
    write_quorum: int = 2,
    routing: Routing = Routing.LEADER,
) -> Callable[[T], T]:
    def decorator(func: T) -> T:
        func.__replication__ = ReplicationConfig(
            factor=factor,
            write_quorum=write_quorum,
            routing=routing,
        )
        return func

    return decorator
