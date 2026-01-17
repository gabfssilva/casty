"""Core replication logic for distributed actor state.

This module provides the handler for incoming state replication
messages, implementing vector clock-based conflict detection and
three-way merge for conflict resolution.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .version import VectorClock
from .wal import WAL

if TYPE_CHECKING:
    from casty import Actor
    from casty.cluster.messages import ReplicateState, ReplicateAck


def _reconstruct(cls: type, state: dict[str, Any]) -> Any:
    """Reconstruct actor instance from state for merge.

    Creates a new instance of the actor class and applies the given
    state to it. This is used during three-way merge to create the
    base and other instances.

    Args:
        cls: The actor class to instantiate
        state: The state dictionary to apply

    Returns:
        A new actor instance with the given state applied
    """
    instance = object.__new__(cls)
    if hasattr(instance, "__init__"):
        try:
            instance.__init__()
        except TypeError:
            pass
    instance.set_state(state)
    return instance


def _is_mergeable(actor: object) -> bool:
    """Check if an actor implements the Mergeable protocol.

    Internal helper to avoid circular imports.
    """
    return (
        hasattr(actor, "__casty_merge__")
        and hasattr(actor, "get_state")
        and hasattr(actor, "set_state")
    )


async def handle_replicate_state(
    msg: "ReplicateState",
    actor: "Actor",
    wal: WAL,
    node_id: str,
) -> "ReplicateAck":
    """Handle incoming state replication from another node.

    Implements vector clock-based conflict detection:
    1. If their_version dominates my_version: I'm behind, accept their state
    2. If my_version dominates their_version: They're behind, ignore
    3. If concurrent (neither dominates): CONFLICT, need merge
       - Find base in WAL
       - If base found and actor is mergeable: three-way merge
       - Else: full sync (accept their state)

    Args:
        msg: The ReplicateState message containing actor_id, version, and state
        actor: The local actor instance to update
        wal: The Write-Ahead Log for this actor
        node_id: The local node's identifier (for version incrementing)

    Returns:
        ReplicateAck indicating success or failure
    """
    from casty.cluster.messages import ReplicateAck

    their_version = msg.version
    their_state = msg.state
    my_version = wal.current_version

    if their_version.dominates(my_version):
        actor.set_state(their_state)
        wal.sync_to(their_version, their_state)
        return ReplicateAck(
            actor_id=msg.actor_id,
            version=their_version,
            success=True,
        )

    if my_version.dominates(their_version):
        return ReplicateAck(
            actor_id=msg.actor_id,
            version=my_version,
            success=True,
        )

    if their_version.concurrent(my_version):
        base_result = wal.find_base(their_version)

        if base_result and _is_mergeable(actor):
            base_version, base_state = base_result
            actor_cls = type(actor)
            base_instance = _reconstruct(actor_cls, base_state)
            their_instance = _reconstruct(actor_cls, their_state)

            actor.__casty_merge__(base_instance, their_instance)

            merged_version = my_version.merge(their_version).increment(node_id)
            merged_state = actor.get_state()
            wal.append_merged(merged_version, merged_state)

            return ReplicateAck(
                actor_id=msg.actor_id,
                version=merged_version,
                success=True,
            )

        actor.set_state(their_state)
        wal.sync_to(their_version, their_state)
        return ReplicateAck(
            actor_id=msg.actor_id,
            version=their_version,
            success=True,
        )

    return ReplicateAck(
        actor_id=msg.actor_id,
        version=my_version,
        success=True,
    )
