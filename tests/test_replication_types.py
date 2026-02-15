from __future__ import annotations

from casty.cluster.state import NodeAddress, ShardAllocation
from casty.core.replication import ReplicateEvents, ReplicateEventsAck, ReplicationConfig
from casty.cluster.replication import ReplicaPromoted
from casty.core.journal import PersistedEvent


def test_replication_config_defaults() -> None:
    config = ReplicationConfig()
    assert config.replicas == 0
    assert config.min_acks == 0
    assert config.ack_timeout == 5.0


def test_shard_allocation_with_replicas() -> None:
    primary = NodeAddress(host="10.0.0.1", port=25520)
    replica1 = NodeAddress(host="10.0.0.2", port=25520)
    replica2 = NodeAddress(host="10.0.0.3", port=25520)
    alloc = ShardAllocation(primary=primary, replicas=(replica1, replica2))
    assert alloc.primary == primary
    assert len(alloc.replicas) == 2


def test_shard_allocation_no_replicas() -> None:
    primary = NodeAddress(host="10.0.0.1", port=25520)
    alloc = ShardAllocation(primary=primary, replicas=())
    assert alloc.replicas == ()


def test_replicate_events_message() -> None:
    events = (PersistedEvent(sequence_nr=1, event="deposited", timestamp=1.0),)
    msg = ReplicateEvents(entity_id="acc-1", shard_id=3, events=events)
    assert msg.entity_id == "acc-1"
    assert msg.shard_id == 3
    assert len(msg.events) == 1


def test_replicate_events_ack() -> None:
    ack = ReplicateEventsAck(entity_id="acc-1", sequence_nr=5)
    assert ack.sequence_nr == 5


def test_replica_promoted() -> None:
    msg = ReplicaPromoted(entity_id="acc-1", shard_id=3)
    assert msg.entity_id == "acc-1"
