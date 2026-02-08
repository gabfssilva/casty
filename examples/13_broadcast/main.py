"""
Cluster Broadcast
=================

Every node spawns a listener actor. One node (the leader) periodically
broadcasts an announcement to ALL nodes via remote refs.  Each listener
receives the message, logs it, and acks back to the sender.

Demonstrates:
  - Cluster formation and leader election
  - Remote lookup via system.lookup(path, node=...)
  - Broadcasting to all cluster members
  - Request-reply across nodes

Usage (local):
    uv run python examples/13_broadcast/main.py --port 25520
    uv run python examples/13_broadcast/main.py --port 25521 --seed 127.0.0.1:25520
    uv run python examples/13_broadcast/main.py --port 25522 --seed 127.0.0.1:25520

Usage (Docker Compose):
    cd examples/13_broadcast
    docker compose up --build
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import ActorRef, Behavior, Behaviors
from casty.cluster_state import MemberStatus
from casty.config import load_config
from casty.sharding import ClusteredActorSystem

log = logging.getLogger(__name__)

RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
MAGENTA = "\033[35m"
LEVEL_COLORS = {
    logging.DEBUG: DIM,
    logging.INFO: GREEN,
    logging.WARNING: YELLOW,
    logging.ERROR: "\033[31m",
    logging.CRITICAL: "\033[1;31m",
}


class ColorFormatter(logging.Formatter):
    def __init__(self) -> None:
        self.node = socket.gethostname()

    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        color = LEVEL_COLORS.get(record.levelno, "")
        level = record.levelname.ljust(7)
        return (
            f"{DIM}{ts}{RESET}  "
            f"{color}{level}{RESET} "
            f"{CYAN}{self.node}{RESET}  "
            f"{record.getMessage()}"
        )


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Announcement:
    text: str
    seq: int
    from_node: str
    reply_to: ActorRef[Ack]


@dataclass(frozen=True)
class Ack:
    seq: int
    from_node: str


type ListenerMsg = Announcement


# ---------------------------------------------------------------------------
# Listener actor — receives broadcasts, sends ack
# ---------------------------------------------------------------------------

LISTENER_PATH = "/listener"


def listener_actor() -> Behavior[ListenerMsg]:
    node = socket.gethostname()

    async def receive(_ctx: Any, msg: ListenerMsg) -> Behavior[ListenerMsg]:
        log.info(
            "%s[#%d]%s from %s%s%s: %s%s%s",
            MAGENTA, msg.seq, RESET,
            CYAN, msg.from_node, RESET,
            YELLOW, msg.text, RESET,
        )
        msg.reply_to.tell(Ack(seq=msg.seq, from_node=node))
        return Behaviors.same()

    return Behaviors.receive(receive)


# ---------------------------------------------------------------------------
# Broadcaster — leader sends announcements to all nodes
# ---------------------------------------------------------------------------


async def broadcast_loop(
    system: ClusteredActorSystem,
    rounds: int,
) -> None:
    node = socket.gethostname()

    for seq in range(1, rounds + 1):
        state = await system.get_cluster_state()
        up_members = [m for m in state.members if m.status == MemberStatus.up]

        log.info(
            "Broadcasting round %s#%d%s to %d nodes...",
            MAGENTA, seq, RESET, len(up_members),
        )

        ack_count = 0
        for member in up_members:
            ref = system.lookup(LISTENER_PATH, node=member.address)
            if ref is None:
                continue

            ack = await system.ask_or_none(
                ref,
                lambda r, s=seq: Announcement(
                    text=f"Hello cluster! (round {s})",
                    seq=s,
                    from_node=node,
                    reply_to=r,
                ),
                timeout=5.0,
            )
            if ack is not None:
                log.info(
                    "  ack from %s%s%s for #%d",
                    CYAN, ack.from_node, RESET, ack.seq,
                )
                ack_count += 1

        log.info(
            "Round #%d: %s%d/%d%s acks received",
            seq, GREEN, ack_count, len(up_members), RESET,
        )

        if seq < rounds:
            await asyncio.sleep(2.0)

    log.info("%sBroadcast complete!%s", GREEN, RESET)


# ---------------------------------------------------------------------------
# Node entry point
# ---------------------------------------------------------------------------


async def run_node(
    host: str,
    port: int,
    bind_host: str,
    seed_nodes: list[tuple[str, int]] | None,
    num_nodes: int,
    rounds: int,
) -> None:
    config = load_config(Path(__file__).parent / "casty.toml")

    log.info("Starting...")

    async with ClusteredActorSystem.from_config(
        config,
        host=host,
        port=port,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
    ) as system:
        # Every node spawns a listener at a well-known path
        system.spawn(listener_actor(), "listener")

        # Wait for all nodes to join
        await system.wait_for(num_nodes)
        log.info("Cluster ready (%d nodes)", num_nodes)

        # Small delay so all listeners are registered
        await asyncio.sleep(1.0)

        # Only the leader broadcasts
        state = await system.get_cluster_state()
        if state.leader == system.self_node:
            log.info("%sI am the leader — starting broadcast%s", YELLOW, RESET)
            await broadcast_loop(system, rounds)
        else:
            log.info("Waiting for broadcasts from leader...")
            # Wait long enough for the leader to finish
            await asyncio.sleep(rounds * 3.0)

        await system.barrier("shutdown", num_nodes)

    log.info("Shutdown")


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty Cluster Broadcast")
    parser.add_argument("--port", type=int, default=25520)
    parser.add_argument(
        "--host", default=None,
        help="Identity hostname. Use 'auto' for container IP. Default: 127.0.0.1",
    )
    parser.add_argument(
        "--bind-host", default=None,
        help="Address to bind TCP listener. Default: same as --host",
    )
    parser.add_argument(
        "--seed", default=None,
        help="Seed node address (host:port) to join an existing cluster",
    )
    parser.add_argument(
        "--nodes", type=int, default=3,
        help="Wait for N nodes before starting (default: 3)",
    )
    parser.add_argument(
        "--rounds", type=int, default=3,
        help="Number of broadcast rounds (default: 3)",
    )
    args = parser.parse_args()

    match args.host:
        case None:
            host = "127.0.0.1"
        case "auto":
            host = socket.gethostbyname(socket.gethostname())
        case h:
            host = h

    bind_host: str = args.bind_host or host

    seed_nodes: list[tuple[str, int]] | None = None
    if args.seed:
        seed_host, seed_port_str = args.seed.rsplit(":", maxsplit=1)
        seed_nodes = [(seed_host, int(seed_port_str))]

    asyncio.run(run_node(host, args.port, bind_host, seed_nodes, args.nodes, args.rounds))


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    main()
