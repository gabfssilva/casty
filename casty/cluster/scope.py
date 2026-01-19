from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ClusterScope:
    replication: int = 1


type Scope = Literal['local', 'cluster'] | ClusterScope
