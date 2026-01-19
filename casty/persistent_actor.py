from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from .actor import Actor, Context, LocalActorRef
from .wal import (
    WriteAheadLog,
    StoreBackend,
    InMemoryStoreBackend,
    Append,
    Recover,
    GetCurrentVersion as WALGetCurrentVersion,
    GetCurrentState as WALGetCurrentState,
    SyncTo as WALSyncTo,
    VectorClock,
)

if TYPE_CHECKING:
    from .wal.actor import WALMessage


@dataclass(frozen=True)
class StateChanged:
    actor_id: str
    version: VectorClock
    state: dict[str, Any]


@dataclass(frozen=True)
class GetCurrentVersion:
    pass


@dataclass(frozen=True)
class GetState:
    pass


@dataclass(frozen=True)
class SyncState:
    version: VectorClock
    state: dict[str, Any]


class PersistentActor[M](Actor[M | GetCurrentVersion | GetState | SyncState]):
    def __init__(
        self,
        wrapped_actor_cls: type[Actor[M]],
        *args: Any,
        actor_id: str,
        cluster_node_id: str,
        backend: StoreBackend | None = None,
        on_state_change: LocalActorRef[StateChanged] | None = None,
        **kwargs: Any,
    ) -> None:
        self._actor_cls = wrapped_actor_cls
        self._actor_args = args
        self._actor_kwargs = kwargs
        self._actor_id = actor_id
        self._cluster_node_id = cluster_node_id
        self._backend = backend or InMemoryStoreBackend()
        self._on_state_change = on_state_change

        self._wal: LocalActorRef[Any] | None = None
        self._wrapped_instance: Actor[M] | None = None

    async def on_start(self) -> None:
        self._wal = await self._ctx.spawn(
            WriteAheadLog,
            node_id=self._cluster_node_id,
            backend=self._backend,
        )

        self._wrapped_instance = self._actor_cls(
            *self._actor_args,
            **self._actor_kwargs,
        )
        self._wrapped_instance._ctx = self._ctx

        snapshot, deltas = await self._wal.ask(Recover())
        if snapshot or deltas:
            await self._apply_recovery(snapshot, deltas)

    async def receive(
        self,
        msg: M | GetCurrentVersion | GetState | SyncState,
        ctx: Context,
    ) -> None:
        match msg:
            case GetCurrentVersion():
                version = await self._wal.ask(WALGetCurrentVersion())
                await ctx.reply(version)

            case GetState():
                state = self._get_actor_state()
                await ctx.reply(state)

            case SyncState(version, state):
                await self._handle_sync(version, state)
                await ctx.reply(True)

            case _:
                await self._forward_with_persistence(msg, ctx)

    async def _forward_with_persistence(self, msg: M, ctx: Context) -> None:
        state_before = self._get_actor_state()

        await self._wrapped_instance.receive(msg, ctx)

        state_after = self._get_actor_state()
        if state_before != state_after:
            delta = self._compute_delta(state_before, state_after)
            version = await self._wal.ask(Append(delta))

            if self._on_state_change:
                await self._on_state_change.send(StateChanged(
                    actor_id=self._actor_id,
                    version=version,
                    state=state_after,
                ))

    def _get_actor_state(self) -> dict[str, Any]:
        return self._wrapped_instance.get_state()

    def _set_actor_state(self, state: dict[str, Any]) -> None:
        self._wrapped_instance.set_state(state)

    def _compute_delta(self, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
        delta: dict[str, Any] = {}

        for k, v in after.items():
            if before.get(k) != v:
                delta[k] = v

        for k in before:
            if k not in after:
                delta[k] = None

        return delta

    async def _apply_recovery(
        self,
        snapshot: dict[str, Any] | None,
        deltas: list[dict[str, Any]],
    ) -> None:
        state = snapshot or {}

        for delta in deltas:
            for k, v in delta.items():
                if v is None:
                    state.pop(k, None)
                else:
                    state[k] = v

        if state:
            self._set_actor_state(state)

    async def _handle_sync(self, version: VectorClock, state: dict[str, Any]) -> None:
        self._set_actor_state(state)
        await self._wal.send(WALSyncTo(version=version, state=state))
