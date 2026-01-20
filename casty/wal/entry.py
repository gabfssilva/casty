from __future__ import annotations

import msgpack
from dataclasses import dataclass


@dataclass
class WALEntry:
    actor_id: str
    sequence: int
    timestamp: float
    data: bytes

    def to_bytes(self) -> bytes:
        return msgpack.packb({
            "actor_id": self.actor_id,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "data": self.data,
        }, use_bin_type=True)

    @classmethod
    def from_bytes(cls, data: bytes) -> WALEntry:
        d = msgpack.unpackb(data, raw=False)
        return cls(
            actor_id=d["actor_id"],
            sequence=d["sequence"],
            timestamp=d["timestamp"],
            data=d["data"],
        )
