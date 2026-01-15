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
        def __init__(self):
            self.count = 0

        async def receive(self, msg: Increment | GetCount, ctx: Context):
            match msg:
                case Increment(amount):
                    self.count += amount
                case GetCount():
                    ctx.reply(self.count)

    async def main():
        async with ActorSystem() as system:
            counter = await system.spawn(Counter)
            await counter.send(Increment(5))
            result = await counter.ask(GetCount())
            print(result)  # 5
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


# Core actor primitives
from .actor import Actor, ActorId, LocalRef, CompositeRef, Behavior, Context, Envelope, EntityRef, ShardedRef

# Actor system
from .system import ActorSystem

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

__all__ = [
    # Core
    "Actor",
    "ActorId",
    "LocalRef",
    "CompositeRef",
    "Behavior",
    "Context",
    "Envelope",
    # Sharding
    "EntityRef",
    "ShardedRef",
    # System
    "ActorSystem",
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
    # Utilities
    "is_uvloop_enabled",
]

__version__ = "1.0.0-dev"
