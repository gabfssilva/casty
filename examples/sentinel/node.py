"""One casty node hosting the Sentinel of examples/09_reactivation.py.

Every container runs this under `restart: always`; a container that cannot
join exits and lets Docker retry it. The seed bootstraps a fresh cluster only
when its peers are unreachable (the first boot) — on a restart it rejoins
through them, so there is never a second cluster.

The activate hook prints the hostname of the container the activation landed
on. Kill that container and watch the sentinel come back on another one, no
call sent; the killed container restarts, rejoins the ring, and killing the
new owner moves the activity again:

    docker compose up --build -d
    docker compose logs -f                    # "sentinel is up ... on <host>"
    docker exec <owner> pkill -9 python       # crash: Docker restarts it, the sentinel moves
    docker exec <new owner> pkill -9 python   # and moves again
    docker compose down

(`docker kill` also works, but Docker treats it as a manual stop and will not
restart the container — bring it back with `docker compose up -d`.)
"""

from __future__ import annotations

import asyncio
import os
import socket

import casty


@casty.actor(name="ex.Sentinel", replicas=3, idle_timeout=float("inf"))
class Sentinel:
    activations: int = 0

    @casty.activate
    async def _up(self) -> None:
        self.activations += 1
        print(
            f"sentinel is up (activation #{self.activations}) on {socket.gethostname()}",
            flush=True,
        )

    async def read(self) -> int:
        return self.activations


def container_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("10.255.255.255", 1))  # no packet sent; picks the default-route iface
        ip = sock.getsockname()[0]
        assert isinstance(ip, str)
        return ip


async def main() -> None:
    port = 9000
    peers = [s for s in os.environ.get("CASTY_SEEDS", "").split(",") if s]
    bootstrap = os.environ.get("CASTY_BOOTSTRAP") == "1"
    node = await casty.start(f"0.0.0.0:{port}", advertise=f"{container_ip()}:{port}")
    joined = False
    for _ in range(5):
        try:
            await node.membership.join(peers)
            joined = True
            break
        except (casty.CastyError, OSError, TimeoutError):
            await asyncio.sleep(2.0)
    if not joined and not bootstrap:
        raise SystemExit(1)  # docker's restart policy is the retry loop
    if bootstrap:  # the seed activates the sentinel once the cluster is up
        while len(node.membership.alive_members()) < 3:
            await asyncio.sleep(0.1)
        while True:  # right after a restart the view may still hold this seed's old self
            try:
                await node.actor(Sentinel, "watchdog").read()
                break
            except casty.CastyError:
                await asyncio.sleep(1.0)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
