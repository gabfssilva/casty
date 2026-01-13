"""Actor state persistence and replication for Casty.

This module provides:
- WriteMode: Configurable durability levels
- ReplicationConfig: Configuration for state replication
- BackupStateManager: In-memory backup state management
- Helper functions for actor state serialization
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from casty.cluster.serialize import serialize


class WriteMode(Enum):
    """Write mode for state persistence.

    Each mode trades off latency vs durability:
    - MEMORY_ONLY: Lowest latency, state only in memory (replicated to backups)
    - ASYNC: Low latency, async persistence (eventual durability)
    - SYNC_LOCAL: Medium latency, sync local persist (survives local crash)
    - SYNC_LOCAL_ASYNC_BACKUP: Medium latency, local sync + async replication
    - SYNC_QUORUM: Higher latency, quorum must persist (survives minority failures)
    - SYNC_ALL: Highest latency, all nodes must persist (maximum durability)
    """

    MEMORY_ONLY = "memory_only"
    ASYNC = "async"
    SYNC_LOCAL = "sync_local"
    SYNC_LOCAL_ASYNC_BACKUP = "sync_local_async_backup"
    SYNC_QUORUM = "sync_quorum"
    SYNC_ALL = "sync_all"


@dataclass
class ReplicationConfig:
    """Configuration for state replication and persistence."""

    # Number of in-memory backup copies (besides primary)
    # 0 = no backup, 1 = one backup, 2 = two backups (recommended)
    memory_replicas: int = 2

    # Maximum entries in backup cache per node
    backup_max_entries: int = 100_000

    # Maximum memory for backup cache (MB)
    backup_max_memory_mb: int = 512

    # Write mode for persistence
    write_mode: WriteMode = WriteMode.MEMORY_ONLY

    # For ASYNC mode: batch size before flush
    async_batch_size: int = 100

    # For ASYNC mode: max interval between flushes (seconds)
    async_flush_interval: float = 0.1

    # Timeout for sync operations (seconds)
    sync_timeout: float = 5.0

    # Try to restore from memory backups before storage
    prefer_memory_restore: bool = True

    # Timeout for failover state fetch (seconds)
    failover_timeout: float = 10.0


class ReplicationPresets:
    """Preset configurations for common use cases."""

    @staticmethod
    def development() -> ReplicationConfig:
        """For local development - minimal overhead."""
        return ReplicationConfig(
            memory_replicas=0,
            write_mode=WriteMode.MEMORY_ONLY,
        )

    @staticmethod
    def high_performance() -> ReplicationConfig:
        """For high performance - accepts eventual loss."""
        return ReplicationConfig(
            memory_replicas=1,
            write_mode=WriteMode.ASYNC,
            async_batch_size=500,
            async_flush_interval=0.5,
        )

    @staticmethod
    def balanced() -> ReplicationConfig:
        """Balance between performance and durability (recommended)."""
        return ReplicationConfig(
            memory_replicas=2,
            write_mode=WriteMode.MEMORY_ONLY,
        )

    @staticmethod
    def high_durability() -> ReplicationConfig:
        """For critical data - maximum durability."""
        return ReplicationConfig(
            memory_replicas=2,
            write_mode=WriteMode.SYNC_QUORUM,
        )


@dataclass
class BackupState:
    """State maintained on backup nodes."""

    state: bytes
    version: int
    primary_node: str
    last_updated: float
    access_count: int = 0  # For LRU eviction


class BackupStateManager:
    """Manages backup states in memory.

    Responsibilities:
    - Store states received via Raft
    - Provide state for failover
    - Eviction of old states (LRU)
    """

    def __init__(
        self,
        max_entries: int = 100_000,
        max_memory_mb: int = 512,
    ):
        self._states: dict[str, BackupState] = {}  # key = "type:id"
        self._max_entries = max_entries
        self._max_memory = max_memory_mb * 1024 * 1024
        self._current_memory = 0
        self._lock = asyncio.Lock()

    async def store(
        self,
        entity_type: str,
        entity_id: str,
        state: bytes,
        version: int,
        primary_node: str,
        timestamp: float,
    ) -> None:
        """Store or update backup state."""
        key = f"{entity_type}:{entity_id}"

        async with self._lock:
            existing = self._states.get(key)

            # Only update if version is greater
            if existing and existing.version >= version:
                return

            # Calculate memory delta
            new_size = len(state)
            old_size = len(existing.state) if existing else 0
            delta = new_size - old_size

            # Evict if necessary
            while (
                self._current_memory + delta > self._max_memory
                or len(self._states) >= self._max_entries
            ) and self._states:
                await self._evict_one()

            # Store
            self._states[key] = BackupState(
                state=state,
                version=version,
                primary_node=primary_node,
                last_updated=timestamp,
            )
            self._current_memory += delta

    async def get(
        self,
        entity_type: str,
        entity_id: str,
    ) -> tuple[bytes, int] | None:
        """Get backup state. Returns (state_bytes, version) or None."""
        key = f"{entity_type}:{entity_id}"

        async with self._lock:
            backup = self._states.get(key)
            if backup:
                backup.access_count += 1
                return (backup.state, backup.version)
            return None

    async def delete(self, entity_type: str, entity_id: str) -> None:
        """Remove backup state."""
        key = f"{entity_type}:{entity_id}"

        async with self._lock:
            backup = self._states.pop(key, None)
            if backup:
                self._current_memory -= len(backup.state)

    async def _evict_one(self) -> None:
        """Remove least recently used entry (LRU)."""
        if not self._states:
            return

        # Find entry with lowest access_count and oldest timestamp
        min_key = min(
            self._states.keys(),
            key=lambda k: (
                self._states[k].access_count,
                self._states[k].last_updated,
            ),
        )

        backup = self._states.pop(min_key)
        self._current_memory -= len(backup.state)

    def get_stats(self) -> dict[str, Any]:
        """Return backup manager statistics."""
        return {
            "entry_count": len(self._states),
            "memory_used": self._current_memory,
            "memory_limit": self._max_memory,
            "utilization": self._current_memory / self._max_memory if self._max_memory > 0 else 0,
        }


# Actor state serialization helpers


@runtime_checkable
class StatefulActor(Protocol):
    """Protocol for actors with persistable state.

    Actors implementing this protocol have full control
    over how their state is serialized and restored.
    """

    def __casty_get_state__(self) -> dict[str, Any]:
        """Return the actor's serializable state."""
        ...

    def __casty_set_state__(self, state: dict[str, Any]) -> None:
        """Restore the actor's state."""
        ...


class PersistentActorMixin:
    """Mixin that provides default persistence implementation.

    Default behavior:
    - get_state: returns __dict__ excluding fields with _ prefix
    - set_state: updates __dict__ with received values

    Customization via class attributes:
    - __casty_include_fields__: Set of fields to include (whitelist)
    - __casty_exclude_fields__: Set of fields to exclude (blacklist)
    """

    __casty_include_fields__: set[str] | None = None
    __casty_exclude_fields__: set[str] = set()

    def __casty_get_state__(self) -> dict[str, Any]:
        if self.__casty_include_fields__ is not None:
            # Whitelist mode
            return {
                k: v
                for k, v in self.__dict__.items()
                if k in self.__casty_include_fields__
            }
        else:
            # Blacklist mode (default)
            return {
                k: v
                for k, v in self.__dict__.items()
                if not k.startswith("_") and k not in self.__casty_exclude_fields__
            }

    def __casty_set_state__(self, state: dict[str, Any]) -> None:
        for k, v in state.items():
            setattr(self, k, v)


def persistent(*fields: str):
    """Decorator that marks specific fields for persistence.

    Usage:
        @persistent("balance", "transaction_count")
        class Account(Actor[Deposit | GetBalance]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id  # Not persisted
                self.balance = 0.0          # Persisted
                self.transaction_count = 0  # Persisted
    """

    def decorator(cls):
        cls.__casty_include_fields__ = set(fields)

        if not hasattr(cls, "__casty_get_state__"):

            def get_state(self) -> dict[str, Any]:
                return {k: getattr(self, k) for k in fields if hasattr(self, k)}

            cls.__casty_get_state__ = get_state

        if not hasattr(cls, "__casty_set_state__"):

            def set_state(self, state: dict[str, Any]) -> None:
                for k, v in state.items():
                    if k in fields:
                        setattr(self, k, v)

            cls.__casty_set_state__ = set_state

        return cls

    return decorator


def serialize_actor_state(actor: Any) -> bytes:
    """Serialize actor state to bytes.

    Order of precedence:
    1. If has __casty_get_state__, use it
    2. If is dataclass, use asdict()
    3. Otherwise, use __dict__ (excluding _prefixed)
    """
    if hasattr(actor, "__casty_get_state__"):
        state_dict = actor.__casty_get_state__()
    elif is_dataclass(actor) and not isinstance(actor, type):
        state_dict = asdict(actor)
    else:
        state_dict = {k: v for k, v in actor.__dict__.items() if not k.startswith("_")}

    return serialize(state_dict)


def restore_actor_state(
    actor: Any, state_dict: dict[str, Any]
) -> None:
    """Restore actor state from a dictionary."""
    if hasattr(actor, "__casty_set_state__"):
        actor.__casty_set_state__(state_dict)
    else:
        for k, v in state_dict.items():
            setattr(actor, k, v)
