from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from .serializer import Serializer


@dataclass
class Listen:
    port: int
    host: str = "0.0.0.0"
    serializer: "Serializer | None" = None


@dataclass
class Connect:
    host: str
    port: int
    serializer: "Serializer | None" = None


@dataclass
class Listening:
    address: tuple[str, int]


@dataclass
class Connected:
    remote_address: tuple[str, int]
    peer_id: str


@dataclass
class ListenFailed:
    reason: str


@dataclass
class ConnectFailed:
    reason: str


@dataclass
class Expose:
    ref: "ActorRef"
    name: str


@dataclass
class Unexpose:
    name: str


@dataclass
class Lookup:
    name: str
    peer: str | None = None
    timeout: float | None = None
    ensure: bool = False


@dataclass
class Exposed:
    name: str


@dataclass
class Unexposed:
    name: str


@dataclass
class LookupResult:
    ref: "ActorRef | None"
    error: str | None = None
    peer: str | None = None
