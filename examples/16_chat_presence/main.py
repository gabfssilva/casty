"""
Chat Presence with Service Discovery
=====================================

Every node dynamically spawns "user" actors, registers them via the
cluster receptionist, and monitors presence changes through ``Subscribe``.
Users join with random Brazilian names, chat briefly, then leave —
triggering auto-deregister via the EventStream.

Demonstrates:
  - ``ServiceKey`` — typed registry keys
  - ``Register`` / ``Subscribe`` — service discovery primitives
  - Auto-deregister on actor stop (via ``ActorStopped`` event)
  - Real-time presence diffs across cluster nodes

Usage (local):
    uv run python examples/16_chat_presence/main.py --port 25520
    uv run python examples/16_chat_presence/main.py --port 25521 --seed 127.0.0.1:25520
    uv run python examples/16_chat_presence/main.py --port 25522 --seed 127.0.0.1:25520

Usage (Docker Compose):
    cd examples/16_chat_presence
    docker compose up --build
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import (
    ActorRef,
    Behavior,
    Behaviors,
    DeadLetter,
    Find,
    Listing,
    Register,
    ServiceKey,
    Subscribe,
)
from casty.config import load_config
from casty.sharding import ClusteredActorSystem

log = logging.getLogger(__name__)

RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
MAGENTA = "\033[35m"
LEVEL_COLORS = {
    logging.DEBUG: DIM,
    logging.INFO: GREEN,
    logging.WARNING: YELLOW,
    logging.ERROR: RED,
    logging.CRITICAL: "\033[1;31m",
}

BRAZILIAN_NAMES = [
    "Ana",
    "Beatriz",
    "Camila",
    "Daniela",
    "Eduardo",
    "Fernanda",
    "Gabriel",
    "Helena",
    "Igor",
    "Julia",
    "Karina",
    "Lucas",
    "Mariana",
    "Nicolas",
    "Olivia",
    "Pedro",
    "Rafaela",
    "Sofia",
    "Thiago",
    "Valentina",
    "Arthur",
    "Bianca",
    "Carlos",
    "Diana",
    "Enzo",
    "Flavia",
    "Gustavo",
    "Isabela",
    "João",
    "Leticia",
    "Matheus",
    "Natalia",
    "Otavio",
    "Patricia",
    "Rafael",
    "Sabrina",
    "Tales",
    "Ursula",
    "Vinicius",
    "Yasmin",
]

CHAT_USER_KEY: ServiceKey[ChatMsg] = ServiceKey("chat-user")


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
class ChatMessage:
    text: str
    from_user: str


@dataclass(frozen=True)
class Leave:
    pass


type ChatMsg = ChatMessage | Leave


# ---------------------------------------------------------------------------
# User actor — registered under ServiceKey("chat-user")
# ---------------------------------------------------------------------------


def user_actor(name: str) -> Behavior[ChatMsg]:
    async def receive(_ctx: Any, msg: ChatMsg) -> Behavior[ChatMsg]:
        match msg:
            case ChatMessage(text=text, from_user=sender):
                log.info(
                    "💬 %s%s%s → %s%s%s: %s%s%s",
                    MAGENTA,
                    sender,
                    RESET,
                    CYAN,
                    name,
                    RESET,
                    YELLOW,
                    text,
                    RESET,
                )
                return Behaviors.same()
            case Leave():
                return Behaviors.stopped()
        return Behaviors.unhandled()

    return Behaviors.receive(receive)


# ---------------------------------------------------------------------------
# Presence monitor — subscribes to chat-user key, prints join/leave diffs
# ---------------------------------------------------------------------------


def presence_monitor() -> Behavior[Listing[ChatMsg]]:
    node = socket.gethostname()

    def active(prev_paths: frozenset[str]) -> Behavior[Listing[ChatMsg]]:
        async def receive(
            _ctx: Any,
            msg: Listing[ChatMsg],
        ) -> Behavior[Listing[ChatMsg]]:
            current_paths = frozenset(inst.ref.address.path for inst in msg.instances)
            joined = current_paths - prev_paths
            left = prev_paths - current_paths

            for path in sorted(joined):
                actor_name = path.rsplit("/", maxsplit=1)[-1]
                log.info(
                    "🟢 %s%s%s joined (%s%s%s)",
                    GREEN,
                    actor_name,
                    RESET,
                    CYAN,
                    node,
                    RESET,
                )
            for path in sorted(left):
                actor_name = path.rsplit("/", maxsplit=1)[-1]
                log.info(
                    "🔴 %s%s%s left",
                    RED,
                    actor_name,
                    RESET,
                )

            return active(current_paths)

        return Behaviors.receive(receive)

    return active(frozenset())


# ---------------------------------------------------------------------------
# Spawn loop — periodically creates and removes users
# ---------------------------------------------------------------------------


async def spawn_loop(system: ClusteredActorSystem, num_users: int) -> None:
    node = socket.gethostname()
    names = random.sample(BRAZILIAN_NAMES, min(num_users, len(BRAZILIAN_NAMES)))
    pending: list[asyncio.Task[None]] = []

    async def leave_after(ref: ActorRef[ChatMsg], delay: float) -> None:
        await asyncio.sleep(delay)
        ref.tell(Leave())

    for base_name in names:
        actor_name = f"{base_name}-{node}"

        ref = system.spawn(user_actor(actor_name), actor_name)
        system.receptionist.tell(Register(key=CHAT_USER_KEY, ref=ref))

        listing: Listing[ChatMsg] = await system.ask(
            system.receptionist,
            lambda r: Find(key=CHAT_USER_KEY, reply_to=r),
            timeout=3.0,
        )
        others = [
            inst
            for inst in listing.instances
            if inst.ref.address.path != ref.address.path
        ]
        if others:
            target = random.choice(others)
            greetings = ["oi!", "e aí?", "bom dia!", "tudo bem?", "fala!"]
            target.ref.tell(
                ChatMessage(text=random.choice(greetings), from_user=actor_name)
            )

        delay = random.uniform(3.0, 8.0)
        pending.append(asyncio.create_task(leave_after(ref, delay)))

        await asyncio.sleep(random.uniform(1.0, 3.0))

    await asyncio.gather(*pending)


# ---------------------------------------------------------------------------
# Node entry point
# ---------------------------------------------------------------------------


async def run_node(
    host: str,
    port: int,
    bind_host: str,
    seed_nodes: list[tuple[str, int]] | None,
    num_nodes: int,
    num_users: int,
) -> None:
    config = load_config(Path(__file__).parent / "casty.toml")

    log.info("Starting...")

    node_id = socket.gethostname()

    async with ClusteredActorSystem.from_config(
        config,
        host=host,
        port=port,
        node_id=node_id,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
    ) as system:
        await system.wait_for(num_nodes)
        log.info(
            "Cluster ready (%s%d%s nodes)",
            GREEN,
            num_nodes,
            RESET,
        )

        async def on_dead_letter(event: DeadLetter) -> None:
            match event.message:
                case ChatMessage():
                    recipient = event.intended_ref.address.path.rsplit("/", maxsplit=1)[-1]
                    log.info(
                        "💨 %s%s%s already left the chat",
                        DIM,
                        recipient,
                        RESET,
                    )

        system.event_stream.subscribe(DeadLetter, on_dead_letter)

        monitor = system.spawn(presence_monitor(), "presence-monitor")
        system.receptionist.tell(Subscribe(key=CHAT_USER_KEY, reply_to=monitor))

        await spawn_loop(system, num_users)
        log.info("All users done")

    log.info("Shutdown")


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty Chat Presence")
    parser.add_argument("--port", type=int, default=25520)
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
        "--seed",
        default=None,
        help="Seed node address (host:port) to join an existing cluster",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=3,
        help="Wait for N nodes before starting (default: 3)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=10,
        help="Number of users to spawn per node (default: 10, max: 40)",
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

    asyncio.run(
        run_node(host, args.port, bind_host, seed_nodes, args.nodes, args.users)
    )


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    main()
