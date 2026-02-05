from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.ref import ActorRef


@dataclass(frozen=True)
class Terminated:
    ref: ActorRef[Any]
