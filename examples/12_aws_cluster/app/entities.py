from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty import ActorRef, Behavior, Behaviors


# --- Counter entity ---


@dataclass(frozen=True)
class Increment:
    amount: int = 1


@dataclass(frozen=True)
class GetCounter:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetCounter


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    async def setup(_ctx: Any) -> Any:
        value = 0

        async def receive(_ctx: Any, msg: Any) -> Any:
            nonlocal value
            match msg:
                case Increment(amount=amount):
                    value += amount
                case GetCounter(reply_to=reply_to):
                    reply_to.tell(value)
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


# --- KV entity ---


@dataclass(frozen=True)
class Put:
    key: str
    value: str


@dataclass(frozen=True)
class Get:
    key: str
    reply_to: ActorRef[str | None]


type KVMsg = Put | Get


def kv_entity(entity_id: str) -> Behavior[KVMsg]:
    async def setup(_ctx: Any) -> Any:
        store: dict[str, str] = {}

        async def receive(_ctx: Any, msg: Any) -> Any:
            match msg:
                case Put(key=key, value=value):
                    store[key] = value
                case Get(key=key, reply_to=reply_to):
                    reply_to.tell(store.get(key))
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)
