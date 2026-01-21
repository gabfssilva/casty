from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.io.framing import Framer


type InboundEvent = (
    Bound | BindFailed |
    Connected | ConnectFailed |
    Received |
    PeerClosed | ErrorClosed | Aborted | WritingResumed
)

type OutboundEvent = Register | Write | Close


@dataclass
class Bind:
    handler: "ActorRef[InboundEvent]"
    port: int
    host: str = "0.0.0.0"
    framing: "Framer | None" = None


@dataclass
class Connect:
    handler: "ActorRef[InboundEvent]"
    host: str
    port: int
    framing: "Framer | None" = None


@dataclass
class Register:
    handler: "ActorRef[InboundEvent]"


@dataclass
class Write:
    data: bytes


@dataclass
class Close:
    pass


@dataclass
class Bound:
    local_address: tuple[str, int]


@dataclass
class BindFailed:
    reason: str


@dataclass
class Connected:
    connection: "ActorRef[OutboundEvent]"
    remote_address: tuple[str, int]
    local_address: tuple[str, int]


@dataclass
class ConnectFailed:
    reason: str


@dataclass
class Received:
    data: bytes


@dataclass
class PeerClosed:
    pass


@dataclass
class ErrorClosed:
    reason: str


@dataclass
class Aborted:
    pass


@dataclass
class WritingResumed:
    pass
