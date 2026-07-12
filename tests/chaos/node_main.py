"""Container entrypoint: one casty node. Run as `python -m tests.chaos.node_main`."""

from __future__ import annotations

import asyncio
import os
import socket

import uvloop

import casty
from tests.chaos.harness import CHAOS_CONFIG


def container_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("10.255.255.255", 1))  # no packet sent; picks the default-route iface
        ip = sock.getsockname()[0]
        assert isinstance(ip, str)
        return ip


async def main() -> None:
    port = int(os.environ.get("CASTY_PORT", "9000"))
    seeds = [s for s in os.environ.get("CASTY_SEEDS", "").split(",") if s]
    advertise = f"{container_ip()}:{port}"
    last: Exception | None = None
    for _ in range(5):  # the seed may still be booting during a mass start
        try:
            await casty.start(
                f"0.0.0.0:{port}", advertise=advertise, seeds=seeds, config=CHAOS_CONFIG
            )
            break
        except (casty.CastyError, OSError, TimeoutError) as exc:
            last = exc
            await asyncio.sleep(2.0)
    else:
        raise SystemExit(f"failed to join cluster: {last}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    uvloop.run(main())
