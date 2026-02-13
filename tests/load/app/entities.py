from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty import ActorRef, Behavior, Behaviors


# --- Counter entity (behavior recursion) ---


@dataclass(frozen=True)
class Increment:
    amount: int = 1


@dataclass(frozen=True)
class GetCounter:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetCounter


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    def active(value: int) -> Behavior[CounterMsg]:
        async def receive(_ctx: Any, msg: CounterMsg) -> Behavior[CounterMsg]:
            match msg:
                case Increment(amount=amount):
                    return active(value + amount)
                case GetCounter(reply_to=reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()
            return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return active(0)


# --- KV entity (behavior recursion with frozendict-style updates) ---


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
    def active(store: dict[str, str]) -> Behavior[KVMsg]:
        async def receive(_ctx: Any, msg: KVMsg) -> Behavior[KVMsg]:
            match msg:
                case Put(key=key, value=value):
                    return active({**store, key: value})
                case Get(key=key, reply_to=reply_to):
                    reply_to.tell(store.get(key))
                    return Behaviors.same()
            return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return active({})
