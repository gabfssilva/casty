from __future__ import annotations

from dataclasses import dataclass

from casty.serializable import serializable
from casty.actor_config import Routing


@serializable
@dataclass
class ReplicationConfig:
    factor: int = 3
    write_quorum: int = 2
    routing: Routing = Routing.LEADER
