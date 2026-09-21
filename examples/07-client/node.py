"""One node of the cluster, as its own process: `uv run node.py 7421`, then `uv run node.py 7422` in another shell.

It lists no actor type. The cluster tells it the name of each type in use, and it imports `app:poll` from there:
every node runs the same code, and that is all a node has to have.

Ctrl-C (or SIGTERM) is an orderly exit: the node hands its keys to the others before it goes.
"""

import asyncio
import signal
import sys

from casty import ActorSystem, Cluster

SEEDS = ("127.0.0.1:7421",)


async def main(port: int) -> None:
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    async with ActorSystem(cluster=Cluster(bind=f"127.0.0.1:{port}", seeds=SEEDS)) as system:
        print(f"{system.node.address} is up", flush=True)
        await stop.wait()
    print("left", flush=True)


asyncio.run(main(int(sys.argv[1])))
