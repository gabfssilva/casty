from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from time import time
from typing import Any

import msgpack

from .version import VectorClock


class EntryType(Enum):
    DELTA = auto()
    SNAPSHOT = auto()
    SYNC = auto()


@dataclass(slots=True)
class WALEntry:
    version: VectorClock
    delta: dict[str, Any]
    timestamp: float = field(default_factory=time)
    entry_type: EntryType = EntryType.DELTA

    @property
    def as_bytes(self) -> bytes:
        return msgpack.packb({
            "v": self.version.to_dict(),
            "d": self.delta,
            "t": self.timestamp,
            "e": self.entry_type.value,
        }, use_bin_type=True)

    @classmethod
    def from_bytes(cls, data: bytes) -> WALEntry:
        d = msgpack.unpackb(data, raw=False)
        return cls(
            version=VectorClock.from_dict(d["v"]),
            delta=d["d"],
            timestamp=d["t"],
            entry_type=EntryType(d["e"]),
        )
