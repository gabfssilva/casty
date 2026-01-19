from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .consistency import Consistency


@dataclass(frozen=True)
class ClusterScope:
    replication: int = 1
    consistency: Consistency = 'one'


type Scope = Literal['local', 'cluster'] | ClusterScope
