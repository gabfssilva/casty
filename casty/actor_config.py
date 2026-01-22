from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Routing(Enum):
    LEADER = "leader"
    ANY = "any"
    LOCAL_FIRST = "local"


@dataclass
class ActorReplicationConfig:
    clustered: bool = False
    replicated: int | None = None
    persistence: Any = None
    routing: dict[type, Routing] = field(default_factory=dict)
