from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class RemoteEnvelope:
    type: str
    correlation_id: str | None = None
    target: str | None = None
    name: str | None = None
    payload: bytes | None = None
    error: str | None = None
    sender_name: str | None = None
    version: int | None = None
    ensure: bool = False
    initial_state: bytes | None = None
    behavior: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RemoteEnvelope:
        return cls(
            type=data.get("type", ""),
            correlation_id=data.get("correlation_id"),
            target=data.get("target"),
            name=data.get("name"),
            payload=data.get("payload"),
            error=data.get("error"),
            sender_name=data.get("sender_name"),
            version=data.get("version"),
            ensure=data.get("ensure", False),
            initial_state=data.get("initial_state"),
            behavior=data.get("behavior"),
        )


class RemoteError(Exception):
    pass
