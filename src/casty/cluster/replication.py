"""Sharding-specific replication message.

``ReplicaPromoted`` is sent by the shard coordinator to a replica region
when the previous primary is detected as down.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReplicaPromoted:
    """Internal message: notifies a replica that it has been promoted to primary.

    Sent by the shard coordinator when the previous primary is detected as
    down.  The replica transitions from passive event persistence to active
    command handling.

    Parameters
    ----------
    entity_id : str
        Entity being promoted.
    shard_id : int
        Shard that owns the entity.

    Examples
    --------
    >>> ReplicaPromoted("user-1", shard_id=7)
    ReplicaPromoted(entity_id='user-1', shard_id=7)
    """

    entity_id: str
    shard_id: int
