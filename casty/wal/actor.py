from __future__ import annotations

from typing import Any

from casty import Actor, Context

from .backend import StoreBackend, InMemoryStoreBackend
from .entry import WALEntry, EntryType
from .version import VectorClock
from .messages import (
    Append,
    Snapshot,
    SyncTo,
    AppendMerged,
    Close,
    Recover,
    GetCurrentVersion,
    GetCurrentState,
    GetStateAt,
    FindBase,
)


type WALMessage = (
    Append | Snapshot | SyncTo | AppendMerged | Close |
    Recover | GetCurrentVersion | GetCurrentState | GetStateAt | FindBase
)


class WriteAheadLog(Actor[WALMessage]):
    def __init__(
        self,
        node_id: str,
        backend: StoreBackend | None = None,
        max_entries: int = 1000,
    ):
        self._node_id = node_id
        self._backend = backend or InMemoryStoreBackend()
        self._max_entries = max_entries

        self._base_snapshot: dict[str, Any] = {}
        self._base_version: VectorClock = VectorClock()
        self._entries: list[WALEntry] = []

    @property
    def current_version(self) -> VectorClock:
        if self._entries:
            return self._entries[-1].version
        return self._base_version

    async def on_start(self) -> None:
        await self._backend.initialize()

    async def on_stop(self) -> None:
        await self._backend.close()

    async def receive(self, msg: WALMessage, ctx: Context) -> None:
        match msg:
            case Append(delta):
                version = await self._append(delta)
                await ctx.reply(version)

            case GetCurrentVersion():
                await ctx.reply(self.current_version)

            case GetCurrentState():
                await ctx.reply(self._get_current_state())

            case GetStateAt(version):
                await ctx.reply(self._get_state_at(version))

            case FindBase(their_version):
                result = self._find_base(their_version)
                await ctx.reply(result)

            case Recover():
                result = await self._recover()
                await ctx.reply(result)

            case Snapshot(state):
                await self._snapshot(state)

            case SyncTo(version, state):
                self._sync_to(version, state)

            case AppendMerged(version, state):
                await self._append_merged(version, state)

            case Close():
                await self._backend.close()

    async def _append(self, delta: dict[str, Any]) -> VectorClock:
        new_version = self.current_version.increment(self._node_id)
        entry = WALEntry(version=new_version, delta=delta)

        self._entries.append(entry)
        await self._backend.append(entry)

        if len(self._entries) > self._max_entries:
            await self._compact()

        return new_version

    def _get_current_state(self) -> dict[str, Any]:
        state = dict(self._base_snapshot)
        for entry in self._entries:
            for k, v in entry.delta.items():
                if v is None:
                    state.pop(k, None)
                else:
                    state[k] = v
        return state

    def _get_state_at(self, target_version: VectorClock) -> dict[str, Any] | None:
        if target_version == self._base_version:
            return dict(self._base_snapshot)

        state = dict(self._base_snapshot)
        for entry in self._entries:
            if target_version.dominates(entry.version) or target_version == entry.version:
                for k, v in entry.delta.items():
                    if v is None:
                        state.pop(k, None)
                    else:
                        state[k] = v
            if entry.version == target_version:
                return state

        return None

    def _find_base(self, their_version: VectorClock) -> tuple[VectorClock, dict[str, Any]] | None:
        my_version = self.current_version

        for entry in reversed(self._entries):
            if my_version.dominates(entry.version) and their_version.dominates(entry.version):
                state = self._get_state_at(entry.version)
                if state is not None:
                    return (entry.version, state)

        if my_version.dominates(self._base_version) and their_version.dominates(self._base_version):
            return (self._base_version, dict(self._base_snapshot))

        if self._base_version == VectorClock() and their_version.dominates(self._base_version):
            return (self._base_version, dict(self._base_snapshot))

        return None

    def _sync_to(self, version: VectorClock, state: dict[str, Any]) -> None:
        self._base_snapshot = dict(state)
        self._base_version = version
        self._entries = []

    async def _snapshot(self, state: dict[str, Any]) -> None:
        new_version = self.current_version.increment(self._node_id)
        entry = WALEntry(
            version=new_version,
            delta=state,
            entry_type=EntryType.SNAPSHOT,
        )
        self._entries.append(entry)
        await self._backend.append(entry)

    async def _append_merged(self, version: VectorClock, state: dict[str, Any]) -> None:
        current = self._get_current_state()
        delta = {k: v for k, v in state.items() if current.get(k) != v}

        for k in current:
            if k not in state:
                delta[k] = None

        entry = WALEntry(version=version, delta=delta)
        self._entries.append(entry)
        await self._backend.append(entry)

        if len(self._entries) > self._max_entries:
            await self._compact()

    async def _recover(self) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        last_snapshot: dict[str, Any] | None = None
        deltas_after: list[dict[str, Any]] = []

        async for entry in self._backend.read_all():
            self._entries.append(entry)

            if entry.entry_type == EntryType.SNAPSHOT:
                last_snapshot = entry.delta
                deltas_after.clear()
                self._base_snapshot = entry.delta
                self._base_version = entry.version
            else:
                deltas_after.append(entry.delta)

        return (last_snapshot, deltas_after)

    async def _compact(self) -> None:
        keep = self._max_entries // 2

        if len(self._entries) <= keep:
            return

        cut_point = len(self._entries) - keep
        cut_entry = self._entries[cut_point - 1]

        new_base_state = self._get_state_at(cut_entry.version)
        if new_base_state:
            self._base_snapshot = new_base_state
            self._base_version = cut_entry.version
            self._entries = self._entries[cut_point:]
