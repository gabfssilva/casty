"""Distributed collections (spec 06). Importing this package registers every
collection's shard factory; `ensure` materializes any of them from a wire name."""

from casty.collections._sharded import ensure
from casty.collections.counter import Counter
from casty.collections.map import Map
from casty.collections.multimap import MultiMap
from casty.collections.queue import Queue
from casty.collections.register import Register
from casty.collections.semaphore import Lease, Lock, Semaphore
from casty.collections.set import Set

__all__ = [
    "Counter",
    "Lease",
    "Lock",
    "Map",
    "MultiMap",
    "Queue",
    "Register",
    "Semaphore",
    "Set",
    "ensure",
]
