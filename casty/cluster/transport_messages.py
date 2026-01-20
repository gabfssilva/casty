from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty.serializable import serializable

if TYPE_CHECKING:
    from casty.ref import ActorRef


@serializable
@dataclass
class Transmit:
    """Send data over a connection."""
    data: bytes


@dataclass
class Deliver:
    """Deliver received data to router."""
    data: bytes
    reply_connection: "ActorRef | None" = None


@dataclass
class Register:
    """Register an actor with the router."""
    ref: "ActorRef"


@dataclass
class NewConnection:
    """New TCP connection accepted."""
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


@serializable
@dataclass
class Connect:
    """Request connection to a node."""
    node_id: str
    address: str


@serializable
@dataclass
class GetConnection:
    """Get existing connection to a node."""
    node_id: str


@serializable
@dataclass
class Disconnect:
    """Connection closed."""
    pass


@dataclass
class Received:
    """Data received from TCP (internal to connection actor)."""
    data: bytes
