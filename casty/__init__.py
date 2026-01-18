"""Casty - A minimalist actor framework for Python 3.12+.

Casty provides a type-safe actor model implementation with:
- Local and distributed actor systems
- Supervision hierarchies for fault tolerance
- Sharded actors with consistent hashing
- Event sourcing for persistence

Basic usage:
    from casty import Actor, ActorSystem, Context
    from dataclasses import dataclass

    @dataclass
    class Increment:
        amount: int

    @dataclass
    class GetCount:
        pass

    class Counter(Actor[Increment | GetCount]):
        async def receive(self, msg: Increment | GetCount, ctx: Context):
            match msg:
                case Increment(amount):
                    self.count += amount
                case GetCount():
                    await ctx.reply(self.count)
"""

import asyncio
import sys

_UVLOOP_INSTALLED = False


def _install_uvloop() -> bool:
    """Install uvloop if available for better performance.

    Returns True if uvloop was installed, False otherwise.
    Only works on Unix-like systems (Linux, macOS).
    """
    global _UVLOOP_INSTALLED

    if sys.platform == "win32":
        return False

    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        _UVLOOP_INSTALLED = True
        return True
    except ImportError:
        return False


_install_uvloop()


def is_uvloop_enabled() -> bool:
    """Check if uvloop is being used as the event loop."""
    return _UVLOOP_INSTALLED

# Protocols
from .protocols import System, ActorRef

# Core actor primitives
from .actor import Actor, ActorId, LocalActorRef, CompositeRef, Behavior, Context, Envelope, EntityRef, ShardedRef, on

# Decorator/Factory (the new ActorSystem)
from .actor_system import ActorSystem

# Implementations
from .system import LocalSystem
from .cluster.clustered_system import ClusteredSystem
from .cluster.clustered_ref import ClusteredActorRef

# Supervision
from .supervision import (
    MultiChildStrategy,
    SupervisionDecision,
    SupervisionStrategy,
    SupervisorConfig,
    SupervisorConfigPresets,
    supervised,
)

# Persistence and WAL
from .persistence import (
    WriteAheadLog,
    StoreBackend,
    FileStoreBackend,
    Append,
    Snapshot,
    Recover,
    Close,
    PersistentActor,
)

# Serialization
from .cluster.serializable import serializable, deserialize

__all__ = [
    # Protocols
    "System",
    "ActorRef",
    # Core
    "Actor",
    "ActorId",
    "LocalActorRef",
    "CompositeRef",
    "Behavior",
    "Context",
    "Envelope",
    "on",
    # Sharding
    "EntityRef",
    "ShardedRef",
    # System (decorator/factory)
    "ActorSystem",
    # Implementations
    "LocalSystem",
    "ClusteredSystem",
    "ClusteredActorRef",
    # Supervision
    "MultiChildStrategy",
    "SupervisionDecision",
    "SupervisionStrategy",
    "SupervisorConfig",
    "SupervisorConfigPresets",
    "supervised",
    # Persistence & WAL
    "WriteAheadLog",
    "StoreBackend",
    "FileStoreBackend",
    "Append",
    "Snapshot",
    "Recover",
    "Close",
    "PersistentActor",
    # Serialization
    "serializable",
    "deserialize",
    # Utilities
    "is_uvloop_enabled",
]

__version__ = "1.0.0-dev"
