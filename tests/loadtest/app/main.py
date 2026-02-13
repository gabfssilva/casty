from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

from casty import Behaviors
from casty.ref import ActorRef
from casty.sharding import ClusteredActorSystem, ShardEnvelope

from .config import AppConfig
from .entities import (
    CounterMsg,
    Get,
    GetCounter,
    Increment,
    KVMsg,
    Put,
    counter_entity,
    kv_entity,
)

logger = logging.getLogger("casty.loadtest.app")

_system: ClusteredActorSystem | None = None
_counter_proxy: ActorRef[ShardEnvelope[CounterMsg]] | None = None
_kv_proxy: ActorRef[ShardEnvelope[KVMsg]] | None = None
_config: AppConfig | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global _system, _counter_proxy, _kv_proxy, _config

    _config = AppConfig.from_env()
    logger.info(
        "Starting node %d at %s:%d (seeds: %s)",
        _config.node_index,
        _config.host_ip,
        _config.casty_port,
        _config.seed_ips,
    )

    async with ClusteredActorSystem(
        name="loadtest-cluster",
        host=_config.host_ip,
        port=_config.casty_port,
        node_id=f"node-{_config.node_index}",
        seed_nodes=_config.seed_nodes,
        bind_host="0.0.0.0",
        required_quorum=_config.node_count // 2 + 1,
    ) as system:
        _system = system

        _counter_proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=20),
            "counters",
        )
        _kv_proxy = system.spawn(
            Behaviors.sharded(entity_factory=kv_entity, num_shards=20),
            "kv",
        )

        logger.info("Node %d ready", _config.node_index)
        yield

    _system = None
    _counter_proxy = None
    _kv_proxy = None


app = FastAPI(title="Casty Load Test Cluster", lifespan=lifespan)


@app.middleware("http")
async def add_node_header(request: Request, call_next):  # noqa: ANN001
    response: Response = await call_next(request)
    if _config:
        response.headers["X-Casty-Node"] = str(_config.node_index)
    return response


class IncrementRequest(BaseModel):
    amount: int = 1


class CounterResponse(BaseModel):
    entity_id: str
    value: int


class PutRequest(BaseModel):
    value: str


class KVResponse(BaseModel):
    entity_id: str
    key: str
    value: str | None


class HealthResponse(BaseModel):
    node: int
    status: str


class ClusterStatusResponse(BaseModel):
    node: int
    members: list[dict[str, Any]]
    unreachable: list[str]


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(
        node=_config.node_index if _config else -1,
        status="ok" if _system else "starting",
    )


@app.get("/cluster/status")
async def cluster_status() -> ClusterStatusResponse:
    if not _system or not _config:
        raise HTTPException(503, "System not ready")
    state = await _system._cluster.get_state(timeout=3.0)  # noqa: SLF001
    return ClusterStatusResponse(
        node=_config.node_index,
        members=[
            {
                "address": f"{m.address.host}:{m.address.port}",
                "status": m.status.name,
            }
            for m in sorted(state.members, key=lambda m: str(m.address))
        ],
        unreachable=[f"{n.host}:{n.port}" for n in state.unreachable],
    )


@app.post("/counter/{entity_id}/increment")
async def counter_increment(entity_id: str, body: IncrementRequest) -> CounterResponse:
    if not _system or not _counter_proxy:
        raise HTTPException(503, "System not ready")
    _counter_proxy.tell(ShardEnvelope(entity_id, Increment(amount=body.amount)))
    try:
        value = await _system.ask(
            _counter_proxy,
            lambda r, eid=entity_id: ShardEnvelope(eid, GetCounter(reply_to=r)),
            timeout=5.0,
        )
        return CounterResponse(entity_id=entity_id, value=value)
    except asyncio.TimeoutError:
        raise HTTPException(504, "Timeout reading counter") from None


@app.get("/counter/{entity_id}")
async def counter_get(entity_id: str) -> CounterResponse:
    if not _system or not _counter_proxy:
        raise HTTPException(503, "System not ready")
    try:
        value = await _system.ask(
            _counter_proxy,
            lambda r, eid=entity_id: ShardEnvelope(eid, GetCounter(reply_to=r)),
            timeout=5.0,
        )
        return CounterResponse(entity_id=entity_id, value=value)
    except asyncio.TimeoutError:
        raise HTTPException(504, "Timeout reading counter") from None


@app.put("/kv/{entity_id}/{key}")
async def kv_put(entity_id: str, key: str, body: PutRequest) -> KVResponse:
    if not _kv_proxy:
        raise HTTPException(503, "System not ready")
    _kv_proxy.tell(ShardEnvelope(entity_id, Put(key=key, value=body.value)))
    return KVResponse(entity_id=entity_id, key=key, value=body.value)


@app.get("/kv/{entity_id}/{key}")
async def kv_get(entity_id: str, key: str) -> KVResponse:
    if not _system or not _kv_proxy:
        raise HTTPException(503, "System not ready")
    try:
        value = await _system.ask(
            _kv_proxy,
            lambda r, eid=entity_id, k=key: ShardEnvelope(eid, Get(key=k, reply_to=r)),
            timeout=5.0,
        )
        return KVResponse(entity_id=entity_id, key=key, value=value)
    except asyncio.TimeoutError:
        raise HTTPException(504, "Timeout reading KV") from None
