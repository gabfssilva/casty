from __future__ import annotations

import asyncio
import os

import pytest

import casty
from tests.chaos import docker_ctl
from tests.chaos.harness import CHAOS_CONFIG, Counter, Workload, eventually, wait_views

pytestmark = pytest.mark.asyncio

ACTORS = int(os.environ.get("CHAOS_ACTORS", "50"))
DURATION = float(os.environ.get("CHAOS_DURATION", "15"))


async def test_baseline_load_with_consistency_check() -> None:
    """Carga pura, sem falha injetada, com os três eixos configuráveis:
    CHAOS_WRITERS (concorrência), CHAOS_ACTORS (atores distintos) e
    CHAOS_RATE (ops/s alvo; 0 = capacidade máxima), por CHAOS_DURATION segundos.

    Consistência ao final: chave sem erro de escrita deve ter valor EXATAMENTE
    igual aos acks; chave com erro fica no intervalo [acked, attempted]."""
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    addrs = [docker_ctl.addr_of(c) for c in all_members]
    await wait_views(addrs, len(all_members), timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    keys = [f"base-{i}" for i in range(ACTORS)]
    workload = Workload(client, keys)
    workload.start()
    await asyncio.sleep(DURATION)
    await workload.stop()

    write_errors = sum(workload.attempted[k] - workload.acked[k] for k in keys)
    print(f"\nbaseline: {workload.report()} | {ACTORS} atores | {write_errors} erros de escrita")

    try:
        for key in keys:
            lo, hi = workload.acked[key], workload.attempted[key]

            async def check(k: str = key, lo: int = lo, hi: int = hi) -> None:
                value = await client.actor(Counter, k).read()
                if lo == hi:  # nenhum erro nesta chave: igualdade exata
                    assert value == lo, f"{k}: {value} != {lo} acks"
                else:
                    assert lo <= value <= hi, f"{k}: {value} fora de [{lo}, {hi}]"

            await eventually(check)
    finally:
        await client.close()
    assert workload.total_acked > 0
