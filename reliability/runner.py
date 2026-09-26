"""The runner of a run on Kubernetes, in the pod `python -m reliability` starts it in.

    python -m reliability.runner

The run comes in `KUBE_PLAN`: its kind, `chaos` or `performance`, and its settings, with the schedule of a chaos run,
as a dump holds them. What it leaves goes to `KUBE_OUTPUT`, the directory the driver copies it back to. `KUBE_IMAGE`
is the image of the pods of the run, this pod's own; `KUBE_CPU` and `KUBE_MEMORY` the size of each pod of a node or a
client; `KUBE_POD` and `KUBE_UID` name this pod, which owns every pod and chaos of the run. Once the run is over, the
runner says `{"finished": <failure>}` and waits for the driver to copy the output and destroy the cluster.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import traceback
from collections.abc import Mapping
from pathlib import Path

from lightkube.models.meta_v1 import OwnerReference

from reliability import performance, records
from reliability.chaos.run import planned, run
from reliability.node import say
from reliability.site import Kubernetes, Shape


async def main(environ: Mapping[str, str], /) -> str | None:
    """Run the plan of `environ`, and answer what failed, if anything did."""
    plan = records.record(json.loads(environ["KUBE_PLAN"]))
    output = Path(environ["KUBE_OUTPUT"])
    owner = OwnerReference(apiVersion="v1", kind="Pod", name=environ["KUBE_POD"], uid=environ["KUBE_UID"])
    shape = Shape(environ.get("KUBE_CPU") or None, environ.get("KUBE_MEMORY") or None)
    image = environ["KUBE_IMAGE"]
    try:
        match records.text(plan["kind"]):
            case "chaos":
                settings, schedule = planned(plan)
                async with Kubernetes.connected(settings.slots, image=image, shape=shape, owner=owner) as site:
                    return (await run(settings, schedule, output, site)).failure
            case "performance":
                measured = performance.Settings.decoded(plan["settings"])
                async with Kubernetes.connected(measured.slots, image=image, shape=shape, owner=owner) as site:
                    return await performance.measure(measured, output, site)
            case other:
                return f"no run of the kind {other!r}"
    except Exception:
        traceback.print_exc()
        return "the run raised: the traceback is above"


if __name__ == "__main__":
    Path(os.environ["KUBE_OUTPUT"]).mkdir(parents=True, exist_ok=True)
    say("finished", asyncio.run(main(os.environ)))
    # The first process of a container drops a signal it has no handler for, unless the signal is blocked: blocked,
    # the SIGTERM that ends the pod waits here to be taken, or the alarm an hour on if no driver ends it.
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGALRM})
    signal.alarm(3600)
    signal.sigwait({signal.SIGTERM, signal.SIGALRM})
