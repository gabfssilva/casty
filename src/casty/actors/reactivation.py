"""Failover reactivation: the reserved `@active` page constants and the pure
ring-rebuild scan. Pure: no I/O, no clock — the node feeds it store items,
live activations and ring lookups, and executes the decisions."""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Set
from dataclasses import dataclass

# Page keys beginning with `@` are reserved for the framework: a state field is
# a Python identifier, so they can never collide, and restore ignores them.
RESERVED_PREFIX = "@"
ACTIVE_PAGE = "@active"
ACTIVE_DATA = b"\x01"

type Identity = tuple[str, str]  # (wire_name, key)


@dataclass(frozen=True, slots=True)
class Marked:
    """One stored identity holding `@active`, resolved against the current ring."""

    wire_name: str
    key: str
    owner: uuid.UUID
    replicas: tuple[uuid.UUID, ...]


@dataclass(frozen=True, slots=True)
class Sweep:
    pokes: list[Identity]
    discards: list[Identity]
    handoffs: list[Identity]


def sweep(
    *,
    marked: Iterable[Marked],
    placed: Iterable[tuple[str, str, uuid.UUID]],  # live activation + current owner
    active: Set[Identity],
    pending_clears: Set[Identity],
    node_id: uuid.UUID,
) -> Sweep:
    """What this node must do after a ring rebuild.

    Pokes are marked identities it now owns, not activated and not
    pending-clear; discards are live activations whose key it no longer owns;
    handoffs are marked identities whose current replica set excludes it.
    """
    entries = list(marked)
    pokes = [
        (m.wire_name, m.key)
        for m in entries
        if m.owner == node_id
        and (m.wire_name, m.key) not in active
        and (m.wire_name, m.key) not in pending_clears
    ]
    discards = [(wire, key) for wire, key, owner in placed if owner != node_id]
    handoffs = [(m.wire_name, m.key) for m in entries if node_id not in m.replicas]
    return Sweep(pokes=pokes, discards=discards, handoffs=handoffs)
