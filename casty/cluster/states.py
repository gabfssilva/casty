from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from casty import actor, Mailbox
from casty.serializable import serializable


@serializable
@dataclass
class StoreState:
    actor_id: str
    state: dict[str, Any]


@serializable
@dataclass
class StoreAck:
    actor_id: str
    success: bool = True


@serializable
@dataclass
class GetState:
    actor_id: str


@serializable
@dataclass
class DeleteState:
    actor_id: str


class StateBackend(Protocol):
    async def store(self, actor_id: str, state: dict[str, Any]) -> None: ...
    async def get(self, actor_id: str) -> dict[str, Any] | None: ...
    async def delete(self, actor_id: str) -> None: ...


class MemoryBackend:
    def __init__(self):
        self._data: dict[str, dict[str, Any]] = {}

    async def store(self, actor_id: str, state: dict[str, Any]) -> None:
        self._data[actor_id] = state

    async def get(self, actor_id: str) -> dict[str, Any] | None:
        return self._data.get(actor_id)

    async def delete(self, actor_id: str) -> None:
        self._data.pop(actor_id, None)


type StatesMessage = StoreState | GetState | DeleteState


@actor
async def states(backend: StateBackend | None = None, *, mailbox: Mailbox[StatesMessage]):
    if backend is None:
        backend = MemoryBackend()

    async for msg, ctx in mailbox:
        match msg:
            case StoreState(actor_id, state):
                await backend.store(actor_id, state)
                await ctx.reply(StoreAck(actor_id))
            case GetState(actor_id):
                result = await backend.get(actor_id)
                await ctx.reply(result)
            case DeleteState(actor_id):
                await backend.delete(actor_id)


class ReplicationQuorumError(Exception):
    pass
