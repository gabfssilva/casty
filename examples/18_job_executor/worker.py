"""Job Executor — Worker Node
==============================

Spawns a job worker actor discoverable under ``JOB_WORKER_KEY``.
The worker can install pip packages on demand and execute arbitrary
Python functions received via cloudpickle.

Usage (local):
    uv run python examples/18_job_executor/worker.py --port 25520
    uv run python examples/18_job_executor/worker.py --port 25521 --seed 127.0.0.1:25520
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import traceback
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from messages import (
    JOB_WORKER_KEY,
    DepsFailed,
    DepsInstalled,
    InstallDeps,
    JobResult,
    RunJob,
    Shutdown,
)

from casty import Behavior, Behaviors
from casty.cluster.system import ClusteredActorSystem
from casty.remote.extras import CloudPickleSerializer

log = logging.getLogger(__name__)


type WorkerMsg = InstallDeps | RunJob | Shutdown


def job_worker(node_name: str, done: asyncio.Event) -> Behavior[WorkerMsg]:
    async def receive(ctx: Any, msg: WorkerMsg) -> Behavior[WorkerMsg]:
        match msg:
            case InstallDeps(packages=packages, reply_to=reply_to):
                log.info("[%s] installing %s", node_name, ", ".join(packages))
                proc = await asyncio.create_subprocess_exec(
                    "uv", "pip", "install", "--quiet", *packages,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await proc.communicate()
                if proc.returncode == 0:
                    reply_to.tell(DepsInstalled(node=node_name))
                else:
                    reply_to.tell(DepsFailed(node=node_name, error=stderr.decode()))
                return Behaviors.same()

            case RunJob(fn=fn, reply_to=reply_to):
                try:
                    result = await asyncio.to_thread(fn)
                    reply_to.tell(JobResult(node=node_name, value=result))
                except Exception:
                    reply_to.tell(JobResult(node=node_name, error=traceback.format_exc()))
                return Behaviors.same()

            case Shutdown():
                log.info("[%s] shutting down", node_name)
                done.set()
                return Behaviors.stopped()

    return Behaviors.receive(receive)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=25520)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--bind-host", default=None)
    parser.add_argument("--seed", default=None)
    parser.add_argument("--nodes", type=int, default=1)
    args = parser.parse_args()

    host = socket.gethostname() if args.host == "auto" else args.host
    seed_nodes = (
        tuple(
            (s.split(":")[0], int(s.split(":")[1]))
            for s in args.seed.split(",")
        )
        if args.seed
        else ()
    )
    node_name = f"{host}:{args.port}"

    done = asyncio.Event()

    async with ClusteredActorSystem(
        name="job-cluster",
        host=host,
        port=args.port,
        node_id=node_name,
        seed_nodes=seed_nodes,
        bind_host=args.bind_host,
        serializer=CloudPickleSerializer(),
    ) as system:
        system.spawn(
            Behaviors.discoverable(
                job_worker(node_name, done),
                key=JOB_WORKER_KEY,
            ),
            "job-worker",
        )

        await system.wait_for(args.nodes)
        log.info("[%s] cluster ready (%d nodes)", node_name, args.nodes)

        await done.wait()
        log.info("[%s] done", node_name)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("casty.cluster").setLevel(logging.WARNING)
    logging.getLogger("casty.remote").setLevel(logging.WARNING)
    asyncio.run(main())
