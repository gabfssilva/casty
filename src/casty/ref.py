from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ActorRef[M]:
    _send: Callable[[M], None]

    def tell(self, msg: M) -> None:
        self._send(msg)
