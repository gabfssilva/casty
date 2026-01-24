from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


WriteQuorum = int | Literal["async", "all", "quorum"]


@dataclass
class ActorReplicationConfig:
    clustered: bool = False
    replicas: int | None = None
    write_quorum: WriteQuorum = "async"
    persistence: Any = None

    def __post_init__(self) -> None:
        if self.replicas is not None:
            if self.replicas <= 0:
                raise ValueError("replicas must be > 0")
            self.clustered = True
        if self.clustered and self.replicas is None:
            self.replicas = 2

    def resolve_write_quorum(self) -> int:
        match self.write_quorum:
            case "async":
                return 0
            case "all":
                return self.replicas or 0
            case "quorum":
                return ((self.replicas or 0) // 2) + 1
            case int() as n:
                return n
