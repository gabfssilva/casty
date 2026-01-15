"""
StateStore Actor - manages local gossip state replica.

Uses Last-Writer-Wins (LWW) with vector clocks for conflict resolution.
"""

import time
from collections import deque

from casty import Actor, LocalRef, Context

from .config import GossipConfig
from .messages import (
    LocalPut,
    LocalDelete,
    ApplyRemoteUpdates,
    GetState,
    GetDigest,
    GetEntries,
    GetPendingUpdates,
    ComputeDiff,
    StateChanged,
)
from .state import StateEntry, StateDigest, DigestDiff, EntryStatus
from .versioning import VectorClock


class StateStore(Actor):
    """Actor managing local gossip state.

    Uses Last-Writer-Wins (LWW) with vector clocks for conflict resolution.
    Maintains state entries and notifies parent of changes.

    Example:
        store = await ctx.spawn(StateStore, node_id="node-1", parent=gossip_ref)
        await store.send(LocalPut("key", b"value"))
        entry = await store.ask(GetState("key"))
    """

    def __init__(
        self,
        node_id: str,
        parent: LocalRef,
        config: GossipConfig | None = None,
    ):
        self._node_id = node_id
        self._parent = parent
        self._config = config or GossipConfig()

        # State storage: key -> StateEntry
        self._entries: dict[str, StateEntry] = {}

        # Our vector clock
        self._clock = VectorClock()

        # Pending updates for push (bounded queue)
        self._pending_updates: deque[StateEntry] = deque(
            maxlen=self._config.max_push_entries * 10
        )

        # Digest generation counter
        self._digest_generation = 0

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            # Local updates
            case LocalPut(key, value, ttl):
                entry = self._put_local(key, value, ttl)
                self._pending_updates.append(entry)
                await self._notify_change(entry)

            case LocalDelete(key):
                entry = self._delete_local(key)
                if entry:
                    self._pending_updates.append(entry)
                    await self._notify_change(entry)

            # Remote updates
            case ApplyRemoteUpdates(entries, source):
                changed = self._apply_remote(entries)
                for entry in changed:
                    await self._notify_change(entry, source=source)

            # Queries
            case GetState(key):
                if key is None:
                    ctx.reply(dict(self._entries))
                else:
                    ctx.reply(self._entries.get(key))

            case GetDigest():
                ctx.reply(self._create_digest())

            case GetEntries(keys):
                entries = tuple(
                    self._entries[k] for k in keys if k in self._entries
                )
                ctx.reply(entries)

            case GetPendingUpdates(max_count):
                updates = list(self._pending_updates)[:max_count]
                ctx.reply(tuple(updates))

            case ComputeDiff(remote_digest):
                diff = self._compute_diff(remote_digest)
                ctx.reply(diff)

    def _put_local(self, key: str, value: bytes, ttl: float | None) -> StateEntry:
        """Put a value locally, incrementing our clock."""
        self._clock = self._clock.increment(self._node_id)
        entry = StateEntry(
            key=key,
            value=value,
            version=self._clock,
            status=EntryStatus.ACTIVE,
            ttl=ttl,
            origin=self._node_id,
            timestamp=time.time(),
        )
        self._entries[key] = entry
        self._enforce_max_entries()
        return entry

    def _delete_local(self, key: str) -> StateEntry | None:
        """Delete a key locally, creating a tombstone."""
        existing = self._entries.get(key)
        if existing is None:
            return None

        self._clock = self._clock.increment(self._node_id)
        entry = StateEntry(
            key=key,
            value=b"",
            version=self._clock,
            status=EntryStatus.DELETED,
            ttl=self._config.tombstone_ttl,
            origin=self._node_id,
            timestamp=time.time(),
        )
        self._entries[key] = entry
        return entry

    def _apply_remote(self, entries: tuple[StateEntry, ...]) -> list[StateEntry]:
        """Apply remote updates using LWW conflict resolution."""
        changed = []
        for entry in entries:
            existing = self._entries.get(entry.key)
            if existing is None or self._should_apply(entry, existing):
                self._entries[entry.key] = entry
                self._clock = self._clock.merge(entry.version)
                changed.append(entry)

        self._enforce_max_entries()
        return changed

    def _should_apply(self, new: StateEntry, existing: StateEntry) -> bool:
        """Determine if new entry should replace existing.

        Rules:
        1. If new happens-after existing: apply
        2. If concurrent: use node_id as tiebreaker (deterministic)
        3. If new happens-before existing: ignore
        """
        if new.version.happens_before(existing.version):
            return False
        if existing.version.happens_before(new.version):
            return True
        # Concurrent: use deterministic tiebreaker (larger node_id wins)
        return new.origin > existing.origin

    def _create_digest(self) -> StateDigest:
        """Create a digest of current state."""
        self._digest_generation += 1
        entries = tuple(
            (key, entry.version)
            for key, entry in sorted(self._entries.items())
        )
        return StateDigest(
            entries=entries,
            node_id=self._node_id,
            generation=self._digest_generation,
        )

    def _compute_diff(self, remote_digest: StateDigest) -> DigestDiff:
        """Compute difference between our state and remote digest."""
        local_keys = set(self._entries.keys())
        remote_keys = remote_digest.keys()

        # Keys they have that we might need
        keys_i_need = []
        for key, remote_version in remote_digest.entries:
            local_entry = self._entries.get(key)
            if local_entry is None:
                keys_i_need.append(key)
            elif remote_version.happens_before(local_entry.version):
                # We have newer, they need ours
                pass
            elif local_entry.version.happens_before(remote_version):
                # They have newer, we need theirs
                keys_i_need.append(key)
            elif local_entry.version.concurrent_with(remote_version):
                # Concurrent, exchange both
                keys_i_need.append(key)

        # Keys we have that they don't
        keys_peer_needs = []
        for key in local_keys:
            if key not in remote_keys:
                keys_peer_needs.append(key)
            else:
                local_entry = self._entries[key]
                remote_version = remote_digest.get_version(key)
                if remote_version and remote_version.happens_before(local_entry.version):
                    keys_peer_needs.append(key)
                elif remote_version and local_entry.version.concurrent_with(remote_version):
                    keys_peer_needs.append(key)

        return DigestDiff(
            keys_i_need=tuple(keys_i_need),
            keys_peer_needs=tuple(keys_peer_needs),
        )

    def _enforce_max_entries(self) -> None:
        """Remove oldest entries if over max."""
        if len(self._entries) <= self._config.max_entries:
            return

        # Sort by timestamp, remove oldest
        sorted_entries = sorted(
            self._entries.items(),
            key=lambda x: x[1].timestamp,
        )
        to_remove = len(self._entries) - self._config.max_entries
        for key, _ in sorted_entries[:to_remove]:
            del self._entries[key]

    async def _notify_change(self, entry: StateEntry, source: str = "local") -> None:
        """Notify parent of state change."""
        value = entry.value if entry.status == EntryStatus.ACTIVE else None
        await self._parent.send(
            StateChanged(
                key=entry.key,
                value=value,
                version=entry.version,
                source=source,
            ),
            sender=self._ctx.self_ref,
        )
