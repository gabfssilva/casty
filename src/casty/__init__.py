"""Typed, clustered actor framework. Virtual actors (Orleans-style) are the
single primitive: identity is `(class, key)`, activation happens on demand on
the node that owns the key's token, deactivation happens on idle. Collections,
services and streaming are sugar over that.

Declare with `@actor` / `@service` / `@message`, run with `start` (cluster),
`connect` (lite client) or `local` (in-process), reach actors through
`ActorSystem.actor` and the collection factories.
"""

from casty import inspection
from casty.actors.context import ActorContext, Schedule
from casty.actors.context import current_context as context
from casty.actors.host import Reply
from casty.actors.registry import (
    Consistency,
    Directive,
    FailureContext,
    Pager,
    PageSet,
    Supervisor,
    activate,
    actor,
    deactivate,
    explain,
    paged,
    transient,
)
from casty.collections import (
    Barrier,
    Counter,
    Lease,
    Lock,
    Map,
    MultiMap,
    Queue,
    Register,
    Semaphore,
    Set,
)
from casty.config import TLS, CompressionConfig, MembershipConfig, TransportConfig
from casty.errors import (
    ActorFailedError,
    ActorUnavailableError,
    CastyError,
    CastyTimeoutError,
    ConnectionLostError,
    HandshakeError,
    ProtocolError,
    QuorumUnavailableError,
    RangeMovingError,
    ReentrancyError,
    RemoteError,
    SerializationError,
    SerializationSchemaError,
    StreamResetError,
    TransportError,
    UnknownActorTypeError,
)
from casty.local import local
from casty.membership.table import Member
from casty.node import Client, Config, Managed, Node, connect, start
from casty.serde.registry import message
from casty.services import service
from casty.system import ActorSystem

KEEP = Directive.KEEP
RESET = Directive.RESET
STOP = Directive.STOP

ONE = Consistency.ONE
MAJORITY = Consistency.MAJORITY
ALL = Consistency.ALL

__all__ = [
    "ALL",
    "KEEP",
    "MAJORITY",
    "ONE",
    "RESET",
    "STOP",
    "TLS",
    "ActorContext",
    "ActorFailedError",
    "ActorSystem",
    "ActorUnavailableError",
    "Barrier",
    "CastyError",
    "CastyTimeoutError",
    "Client",
    "CompressionConfig",
    "Config",
    "ConnectionLostError",
    "Consistency",
    "Counter",
    "Directive",
    "FailureContext",
    "HandshakeError",
    "Lease",
    "Lock",
    "Managed",
    "Map",
    "Member",
    "MembershipConfig",
    "MultiMap",
    "Node",
    "PageSet",
    "Pager",
    "ProtocolError",
    "Queue",
    "QuorumUnavailableError",
    "RangeMovingError",
    "ReentrancyError",
    "Register",
    "RemoteError",
    "Reply",
    "Schedule",
    "Semaphore",
    "SerializationError",
    "SerializationSchemaError",
    "Set",
    "StreamResetError",
    "Supervisor",
    "TransportConfig",
    "TransportError",
    "UnknownActorTypeError",
    "activate",
    "actor",
    "connect",
    "context",
    "deactivate",
    "explain",
    "inspection",
    "local",
    "message",
    "paged",
    "service",
    "start",
    "transient",
]
