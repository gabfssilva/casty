"""High-level distributed data structures backed by sharded actors.

Re-exports the public client classes (``Counter``, ``Dict``, ``Set``,
``Queue``, ``Lock``, ``Semaphore``, ``Barrier``) and the ``Distributed``
facade that creates them.
"""

from __future__ import annotations

from casty.distributed.barrier import Barrier
from casty.distributed.counter import Counter
from casty.distributed.dict import Dict
from casty.distributed.distributed import Distributed, EntityGateway
from casty.distributed.lock import Lock
from casty.distributed.queue import Queue
from casty.distributed.semaphore import Semaphore
from casty.distributed.set import Set

__all__ = [
    "Barrier",
    "Counter",
    "Dict",
    "Distributed",
    "EntityGateway",
    "Lock",
    "Queue",
    "Semaphore",
    "Set",
]
