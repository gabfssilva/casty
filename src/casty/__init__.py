"""casty: virtual actors with a Rust core.

Everything that runs is in `casty._casty`: the schema, the transport, the placement, the replication and the bodies of
the collections. What is here is the surface those things are reached through: the names the core answers to, and,
from `casty.model`, the values a caller builds and the protocols a caller writes against.
"""

from __future__ import annotations

from importlib import import_module

from casty.model import (
    TLS,
    ActorDefinition,
    Backoff,
    Body,
    Cluster,
    Compression,
    Durable,
    Limits,
    Member,
    NodeId,
    OnFull,
    Opaque,
    Overlay,
    Placement,
    Store,
    System,
    Write,
)
from casty.model import address as address

_core = import_module("casty._casty")

ActorFailed = _core.ActorFailed
MailboxFull = _core.MailboxFull
MessageTooLarge = _core.MessageTooLarge
NotStarted = _core.NotStarted
ReentrancyError = _core.ReentrancyError
Refused = _core.Refused
SchemaError = _core.SchemaError
Unavailable = _core.Unavailable
UnknownActor = _core.UnknownActor

Actor = _core.Actor
ActorSystem = _core.ActorSystem
Client = _core.Client
Context = _core.Context
DefaultedActor = _core.DefaultedActor
Ref = _core.Ref
Runtime = _core.Runtime
Schedule = _core.Schedule
State = _core.State
actor = _core.actor

#: The nodes that keep a key, the first being the one the ring gives it to. A test hook, not part of the API.
replicas = _core.replicas


from casty.collections import Collections as Collections  # noqa: E402
from casty.observer import (  # noqa: E402
    Activation,
    ActivationEnded,
    ActivationFailed,
    ActivationStarted,
    ActorStats,
    ConnectionLost,
    Event,
    HandoffEnded,
    HandoffStarted,
    LoggingObserver,
    MemberChanged,
    MessageDropped,
    Observer,
    Stats,
    WriteFailed,
)

__all__ = [
    "TLS",
    "Activation",
    "ActivationEnded",
    "ActivationFailed",
    "ActivationStarted",
    "Actor",
    "ActorDefinition",
    "ActorFailed",
    "ActorStats",
    "ActorSystem",
    "Backoff",
    "Body",
    "Client",
    "Cluster",
    "Collections",
    "Compression",
    "ConnectionLost",
    "Context",
    "DefaultedActor",
    "Durable",
    "Event",
    "HandoffEnded",
    "HandoffStarted",
    "Limits",
    "LoggingObserver",
    "MailboxFull",
    "Member",
    "MemberChanged",
    "MessageDropped",
    "MessageTooLarge",
    "NodeId",
    "NotStarted",
    "Observer",
    "OnFull",
    "Opaque",
    "Overlay",
    "Placement",
    "ReentrancyError",
    "Ref",
    "Refused",
    "Runtime",
    "Schedule",
    "SchemaError",
    "State",
    "Stats",
    "Store",
    "System",
    "Unavailable",
    "UnknownActor",
    "Write",
    "WriteFailed",
    "actor",
]
