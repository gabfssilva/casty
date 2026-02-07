"""
Multi-Process Cluster Example
==============================

Run N nodes in separate terminals, each on its own TCP port.
Node 1 is the seed/coordinator.

Usage (local terminals):
    python examples/11_multi_process_cluster/main.py 1      # node 1 on port 25520 (seed)
    python examples/11_multi_process_cluster/main.py 2      # node 2 on port 25521
    python examples/11_multi_process_cluster/main.py 3      # node 3 on port 25522

Usage (Docker Compose):
    cd examples/11_multi_process_cluster
    docker compose up --build --scale node=4
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
from dataclasses import dataclass
from typing import Any

from casty import Behaviors, ActorRef, Behavior
from casty.sharding import ClusteredActorSystem, ShardEnvelope


# --- Counter entity ---


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetValue


def counter_entity(entity_id: str) -> Behavior[CounterMsg]:
    async def setup(_ctx: Any) -> Any:
        value = 0
        print(f"  [entity:{entity_id}] spawned")

        async def receive(_ctx: Any, msg: Any) -> Any:
            nonlocal value
            match msg:
                case Increment(amount=amount):
                    value += amount
                    print(f"  [entity:{entity_id}] incremented by {amount} -> {value}")
                case GetValue(reply_to=reply_to):
                    print(f"  [entity:{entity_id}] get -> {value}")
                    reply_to.tell(value)
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


# --- Interactive mode ---


async def run_interactive(
    system: ClusteredActorSystem,
    proxy: ActorRef[ShardEnvelope[CounterMsg]],
    node_id: int,
) -> None:
    print(f"[node{node_id}] Ready. Commands: inc <entity> <amount> | get <entity> | quit")

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        line = line.strip()
        if not line:
            continue
        if line == "quit":
            break

        parts = line.split()
        if parts[0] == "inc" and len(parts) == 3:
            entity_id = parts[1]
            amount = int(parts[2])
            proxy.tell(ShardEnvelope(entity_id, Increment(amount=amount)))
            print(f"  -> sent Increment({amount}) to {entity_id}")

        elif parts[0] == "get" and len(parts) == 2:
            entity_id = parts[1]
            try:
                result = await system.ask(
                    proxy,
                    lambda r, eid=entity_id: ShardEnvelope(eid, GetValue(reply_to=r)),
                    timeout=3.0,
                )
                print(f"  -> {entity_id} = {result}")
            except asyncio.TimeoutError:
                print(f"  -> timeout getting {entity_id}")
        else:
            print("  Usage: inc <entity> <amount> | get <entity> | quit")


# --- Auto mode (non-interactive, for Docker) ---


async def run_auto_seed(
    system: ClusteredActorSystem,
    proxy: ActorRef[ShardEnvelope[CounterMsg]],
) -> None:
    """Seed node: write to counter, read back, print result, then stay alive for workers."""
    await asyncio.sleep(3.0)  # wait for workers to join

    entity = "demo-counter"
    total = 10
    print(f"[seed] Incrementing '{entity}' {total} times...")
    for _ in range(total):
        proxy.tell(ShardEnvelope(entity, Increment(amount=1)))
        await asyncio.sleep(0.05)

    await asyncio.sleep(1.0)  # let increments propagate

    try:
        result = await system.ask(
            proxy,
            lambda r: ShardEnvelope(entity, GetValue(reply_to=r)),
            timeout=5.0,
        )
        print(f"[seed] Result: {entity} = {result} (expected {total})")
    except asyncio.TimeoutError:
        print(f"[seed] Timeout reading {entity}")

    # Stay alive so workers can query the entity hosted here
    await asyncio.sleep(10.0)
    print("[seed] Done, shutting down.")


async def run_auto_worker(
    system: ClusteredActorSystem,
    proxy: ActorRef[ShardEnvelope[CounterMsg]],
    host: str,
) -> None:
    """Worker node: wait for seed to write, then read the counter."""
    await asyncio.sleep(8.0)  # wait for seed to finish writing

    entity = "demo-counter"
    try:
        result = await system.ask(
            proxy,
            lambda r: ShardEnvelope(entity, GetValue(reply_to=r)),
            timeout=5.0,
        )
        print(f"[worker:{host}] Result: {entity} = {result}")
    except asyncio.TimeoutError:
        print(f"[worker:{host}] Timeout reading {entity}")


# --- Node runner ---


async def run_node(
    node_id: int,
    port: int,
    host: str,
    bind_host: str,
    seed_host: str,
    seed_port: int,
    auto: bool,
) -> None:
    seed_nodes: list[tuple[str, int]] = (
        [] if node_id == 1 else [(seed_host, seed_port)]
    )

    print(f"[node{node_id}] Starting on {host}:{port} (bind={bind_host})...")

    async with ClusteredActorSystem(
        name="demo-cluster",
        host=host,
        port=port,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
    ) as system:
        proxy = system.spawn(
            Behaviors.sharded(entity_factory=counter_entity, num_shards=10),
            "counters",
        )
        await asyncio.sleep(1.0)

        if auto:
            if node_id == 1:
                await run_auto_seed(system, proxy)
            else:
                await run_auto_worker(system, proxy, host)
        else:
            await run_interactive(system, proxy, node_id)

    print(f"[node{node_id}] Shutdown complete.")


# --- CLI ---


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty multi-process cluster example")
    parser.add_argument("node_id", type=int, help="Node number (1 = seed)")
    parser.add_argument("--base-port", type=int, default=25520, help="Base port (default: 25520)")
    parser.add_argument("--port", type=int, default=None, help="Explicit port (overrides base-port + node_id)")
    parser.add_argument(
        "--host",
        default=None,
        help="Identity hostname. Use 'auto' for container IP. Default: 127.0.0.1",
    )
    parser.add_argument(
        "--bind-host",
        default=None,
        help="Address to bind TCP listener. Default: same as --host",
    )
    parser.add_argument(
        "--seed-host",
        default=None,
        help="Hostname of the seed node. Default: same as --host",
    )
    parser.add_argument(
        "--seed-port",
        type=int,
        default=None,
        help="Port of the seed node. Default: same as --base-port",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Non-interactive mode for Docker (seed writes, workers read)",
    )
    args = parser.parse_args()

    if args.node_id < 1:
        parser.error("node_id must be >= 1")

    # Resolve host
    if args.host is None:
        host = "127.0.0.1"
    elif args.host == "auto":
        # Get the container's IP address (resolvable by other containers)
        host = socket.gethostbyname(socket.gethostname())
    else:
        host = args.host

    port: int = args.port if args.port is not None else args.base_port + args.node_id - 1
    bind_host: str = args.bind_host or host
    seed_host: str = args.seed_host or host
    seed_port: int = args.seed_port if args.seed_port is not None else args.base_port

    asyncio.run(run_node(args.node_id, port, host, bind_host, seed_host, seed_port, args.auto))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")
    main()
