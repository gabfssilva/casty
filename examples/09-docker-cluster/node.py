"""One node, configured by the environment of its container.

`ADVERTISE` is the address the others dial. The seeds set it to their service name, which is stable; every other node
leaves it out and advertises the IP of its container, since replicas of one compose service share a name.
`docker compose stop` sends SIGTERM, which is an orderly exit: the node hands its keys over before it goes.
"""

import asyncio
import os
import signal
import socket
from datetime import timedelta

from casty import ActorSystem, Cluster

PORT = 7400


async def main() -> None:
    advertise = os.environ.get("ADVERTISE") or f"{socket.gethostbyname(socket.gethostname())}:{PORT}"
    cluster = Cluster(
        bind=f"0.0.0.0:{PORT}",
        advertise=advertise,
        seeds=tuple(os.environ["SEEDS"].split(",")),
        # The defaults suspect a node that is only slow to be visited once the cluster has more than about fifteen
        # members, so the detection is given more room here than a cluster of this size should need.
        suspect_after=timedelta(seconds=60),
    )
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    async with ActorSystem(cluster=cluster) as system:
        print(f"{system.node.address} is up", flush=True)
        await stop.wait()
    print(f"{advertise} left", flush=True)


asyncio.run(main())
