from __future__ import annotations

import asyncio
import random
import socket
import time
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, Behavior, Behaviors


# --- Worker (one per node, addressable by node_id via lookup) ---


@dataclass(frozen=True)
class GetStatus:
    reply_to: ActorRef[WorkerStatus]


@dataclass(frozen=True)
class WorkerStatus:
    node_id: str
    processed: int


type WorkerMsg = GetStatus


def worker_actor() -> Behavior[WorkerMsg]:
    def active(processed: int) -> Behavior[WorkerMsg]:
        async def receive(ctx: Any, msg: WorkerMsg) -> Behavior[WorkerMsg]:
            match msg:
                case GetStatus(reply_to=reply_to):
                    node_id = ctx.self.address.node_id or "unknown"
                    reply_to.tell(WorkerStatus(node_id=node_id, processed=processed))
                    return active(processed + 1)
            return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)


# --- Ping listener (broadcasted) ---


@dataclass(frozen=True)
class Ping:
    seq: int
    from_node: str
    sent_at: float
    reply_to: ActorRef[Pong]


@dataclass(frozen=True)
class Pong:
    seq: int
    from_node: str
    latency_ms: float


type PingMsg = Ping


def ping_listener() -> Behavior[PingMsg]:
    node = socket.gethostname()

    async def receive(_ctx: Any, msg: PingMsg) -> Behavior[PingMsg]:
        match msg:
            case Ping(seq=seq, sent_at=sent_at, reply_to=reply_to):
                delay = random.uniform(1.0, 3.0)
                await asyncio.sleep(delay)
                latency_ms = (time.time() - sent_at) * 1000.0
                reply_to.tell(Pong(seq=seq, from_node=node, latency_ms=latency_ms))
        return Behaviors.same()

    return Behaviors.receive(receive)


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
