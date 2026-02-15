from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class ShardEnvelope[M]:
    """Envelope that routes a message to a specific entity within a shard region.

    Wraps a message ``M`` together with an ``entity_id`` used for deterministic
    shard assignment.  The shard proxy computes the target shard from the
    ``entity_id`` and forwards the envelope to the owning node.

    Parameters
    ----------
    entity_id : str
        Logical identifier of the target entity.
    message : M
        The payload message delivered to the entity actor.

    Examples
    --------
    >>> ref.tell(ShardEnvelope("user-42", Deposit(amount=100)))
    """

    entity_id: str
    message: M


def entity_shard(entity_id: str, num_shards: int) -> int:
    """Deterministic shard assignment -- consistent across processes.

    Parameters
    ----------
    entity_id : str
        The entity identifier to hash.
    num_shards : int
        Total number of shards.

    Returns
    -------
    int
        Shard index in ``[0, num_shards)``.

    Examples
    --------
    >>> entity_shard("user-42", 100)
    72
    """
    digest = hashlib.md5(entity_id.encode(), usedforsecurity=False).digest()
    return int.from_bytes(digest[:4], "big") % num_shards
