"""Job Executor — Client
=========================

External client that discovers job workers in the cluster, installs
numpy on each node, and fans out eigenvalue computations in parallel.

Usage:
    uv run python examples/18_job_executor/client.py --contact node-1:25520 --workers 10
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
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

from casty import ClusterClient, Listing, ServiceInstance
from casty.remote.extras import CloudPickleSerializer

log = logging.getLogger(__name__)

BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
RESET = "\033[0m"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contact", default="127.0.0.1:25520")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    host, port = args.contact.split(":")
    contact_points = [(host, int(port))]

    async with ClusterClient(
        contact_points=contact_points,
        system_name="job-cluster",
        serializer=CloudPickleSerializer(),
    ) as client:
        # 1. Discover workers
        print(f"\n{BOLD}Waiting for {args.workers} workers...{RESET}")
        workers: list[ServiceInstance[Any]] = []
        while len(workers) < args.workers:
            listing: Listing[Any] = client.lookup(JOB_WORKER_KEY)
            workers = sorted(listing.instances, key=lambda w: str(w.node))
            if len(workers) < args.workers:
                await asyncio.sleep(0.5)
        print(f"  found {len(workers)} workers\n")

        # 2. Install numpy on all workers
        print(f"{BOLD}Installing numpy on {len(workers)} workers...{RESET}")
        install_tasks = [
            client.ask(
                w.ref,
                lambda reply_to, _w=w: InstallDeps(
                    packages=("numpy",), reply_to=reply_to
                ),
                timeout=120.0,
            )
            for w in workers
        ]
        install_results = await asyncio.gather(*install_tasks)
        for result in install_results:
            match result:
                case DepsInstalled(node=node):
                    print(f"  {GREEN}✓{RESET} {node}")
                case DepsFailed(node=node, error=error):
                    print(f"  {RED}✗{RESET} {node}: {error}")

        # 3. Submit jobs in parallel
        def make_job(job_id: int):
            def compute():
                import numpy as np

                rng = np.random.default_rng(seed=job_id)
                matrix = rng.standard_normal((200, 200))
                eigenvalues = np.linalg.eigvals(matrix)
                top3 = sorted(np.abs(eigenvalues), reverse=True)[:3]
                return {
                    "job_id": job_id,
                    "matrix_shape": (200, 200),
                    "top_eigenvalues": [round(float(v), 2) for v in top3],
                }

            return compute

        print(f"\n{BOLD}Submitting {len(workers)} jobs in parallel...{RESET}")
        t0 = time.monotonic()

        job_tasks = [
            client.ask(
                workers[i].ref,
                lambda reply_to, _i=i: RunJob(fn=make_job(_i), reply_to=reply_to),
                timeout=120.0,
            )
            for i in range(len(workers))
        ]
        job_results: list[JobResult] = await asyncio.gather(*job_tasks)

        elapsed = time.monotonic() - t0
        for r in job_results:
            match r:
                case JobResult(node=node, value=value) if value is not None:
                    v = value
                    print(
                        f"  {CYAN}job-{v['job_id']}{RESET} → {node}  "
                        f"eigenvalues: {v['top_eigenvalues']}"
                    )
                case JobResult(node=node, error=error):
                    print(f"  {RED}job{RESET} → {node}  ERROR: {error}")

        print(f"\n{BOLD}All {len(workers)} jobs completed in {elapsed:.2f}s{RESET}")

        # 4. Shut down workers
        for w in workers:
            w.ref.tell(Shutdown())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("casty.cluster").setLevel(logging.WARNING)
    logging.getLogger("casty.remote").setLevel(logging.WARNING)
    asyncio.run(main())
