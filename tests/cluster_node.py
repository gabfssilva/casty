"""
Cluster test node — symmetric, like example 11.

Every node is identical: connects to the seed, waits for all nodes,
then the first node (seed) writes the counter and all nodes read it.

Used by docker-compose.test.yml via testcontainers.
"""
from __future__ import annotations

import argparse
import asyncio
import socket
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, Behavior, Behaviors
from casty.internal.retry import Backoff, retry
from casty.sharding import ClusteredActorSystem, ShardEnvelope


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetValue


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    def active(value: int = 0) -> Behavior[CounterMsg]:
        async def receive(_ctx: Any, msg: CounterMsg) -> Behavior[CounterMsg]:
            match msg:
                case Increment(amount=amount):
                    return active(value + amount)
                case GetValue(reply_to=reply_to):
                    reply_to.tell(value)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active()


async def run_node(
    host: str,
    port: int,
    bind_host: str,
    seed: str,
    nodes: int,
    is_seed: bool,
) -> None:
    seed_host, seed_port_str = seed.rsplit(":", maxsplit=1)
    seed_nodes = [(seed_host, int(seed_port_str))]

    async with ClusteredActorSystem(
        name="test-cluster",
        host=host,
        port=port,
        bind_host=bind_host,
        seed_nodes=seed_nodes,
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )

        await system.wait_for(nodes, timeout=30.0)

        entity = "test-counter"

        if is_seed:
            for _ in range(10):
                proxy.tell(ShardEnvelope(entity, Increment(amount=1)))
                await asyncio.sleep(0.02)

        await system.barrier("writes-done", nodes)

        @retry(max_retries=20, delay=0.5, backoff=Backoff.fixed, on=(Exception,))
        async def read_counter() -> int:
            result = await system.ask(
                proxy,
                lambda r: ShardEnvelope(entity, GetValue(reply_to=r)),
                timeout=2.0,
            )
            if result != 10:
                msg = f"Counter not ready: {result}"
                raise ValueError(msg)
            return result

        result = await read_counter()
        print(f"RESULT={result}")

        await system.barrier("reads-done", nodes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="auto")
    parser.add_argument("--port", type=int, default=25520)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--seed", required=True)
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--is-seed", action="store_true")
    args = parser.parse_args()
    host = socket.gethostbyname(socket.gethostname()) if args.host == "auto" else args.host
    asyncio.run(run_node(host, args.port, args.bind_host, args.seed, args.nodes, args.is_seed))
