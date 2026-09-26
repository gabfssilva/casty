"""A run on Kubernetes, driven from this machine.

    uv run python -m reliability chaos           # make chaos
    uv run python -m reliability performance     # make performance

plans the run: a chaos run from the `CHAOS_*` variables of `reliability.chaos.run`, or the replay of `CHAOS_REPLAY`; a
performance run from the `PERFORMANCE_*` variables of `reliability.performance`. It brings up the Kubernetes of the
Pulumi stack `KUBE_STACK` (`local`, a kind cluster, unless set); builds the image of this checkout and loads it there;
starts the runner and prints what it says; and copies what the run leaves to `CHAOS_OUTPUT` or `PERFORMANCE_OUTPUT`, or
to a new temporary directory. The runner writes it at that same path in its pod, so the paths the report names are the
ones on this machine. `KUBE_CPU` and `KUBE_MEMORY` size the pod of each node and client.

However the run ends, the image goes and the stack is destroyed: only the output is left. Ctrl-C reaches every command
of the run and ends the one under way, which ends the run; this process is not ended by it, and removes the image and
destroys the stack out of the reach of another. It exits with 1 when the run failed.
"""

from __future__ import annotations

import io
import json
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType

from reliability import performance, records
from reliability.chaos.run import arranged
from reliability.chaos.schedule import encoded

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "reliability" / "infra"
RUNNER = "runner"
"""The pod of the runner, and the account it acts with, which `reliability/infra/__main__.py` creates."""


@dataclass(frozen=True)
class Stack:
    """What the Pulumi stack of a cluster exports."""

    kubeconfig: str
    cluster: str
    namespace: str

    @classmethod
    @contextmanager
    def up(cls, name: str, /) -> Generator[Stack]:
        """Bring the stack `name` up, and destroy it when done, however that comes."""
        try:
            _pulumi("stack", "select", "--create", name)
            _pulumi("up", "--yes", "--skip-preview", "--suppress-outputs", "--stack", name)
            exported = _pulumi("stack", "output", "--json", "--show-secrets", "--stack", name, quiet=True)
            held = records.record(json.loads(exported))
            yield cls(records.text(held["kubeconfig"]), records.text(held["cluster"]), records.text(held["namespace"]))
        finally:
            _pulumi("destroy", "--yes", "--skip-preview", "--stack", name, alone=True)


def main(arguments: Sequence[str], /) -> int:
    plan: dict[str, object]
    match arguments:
        case ["chaos"]:
            settings, schedule = arranged(os.environ)
            plan = {"kind": "chaos", "settings": settings.encoded(), "schedule": [encoded(step) for step in schedule]}
            output = _destination(os.environ.get("CHAOS_OUTPUT"), f"casty-chaos-{settings.seed}-")
        case ["performance"]:
            plan = {"kind": "performance", "settings": performance.Settings.environment(os.environ).encoded()}
            output = _destination(os.environ.get("PERFORMANCE_OUTPUT"), "casty-performance-")
        case _:
            raise SystemExit("usage: python -m reliability {chaos,performance}")
    signal.signal(signal.SIGINT, _interrupted)
    with (
        Stack.up(os.environ.get("KUBE_STACK", "local")) as stack,
        _image(stack) as image,
        tempfile.NamedTemporaryFile("w", suffix=".yaml") as kubeconfig,
    ):
        kubeconfig.write(stack.kubeconfig)
        kubeconfig.flush()
        kubectl = ("kubectl", "--kubeconfig", kubeconfig.name, "--namespace", stack.namespace)
        _run(*kubectl, "apply", "--filename", "-", given=json.dumps(_runner(image, plan, output)))
        _run(*kubectl, "wait", "--for=condition=Ready", f"pod/{RUNNER}", "--timeout=300s")
        finished = _follow(kubectl)
        archive = subprocess.run(
            [*kubectl, "exec", RUNNER, "--", "tar", "cf", "-", "-C", str(output), "."],
            check=True,
            capture_output=True,
        ).stdout
        with tarfile.open(fileobj=io.BytesIO(archive)) as copied:
            copied.extractall(output, filter="data")
    print(f"output: {output}", flush=True)
    return 0 if finished else 1


def _destination(chosen: str | None, prefix: str, /) -> Path:
    """Where a run keeps what it leaves: `chosen`, or a new temporary directory starting with `prefix`."""
    if chosen:
        path = Path(chosen)
        path.mkdir(parents=True, exist_ok=True)
        return path
    return Path(tempfile.mkdtemp(prefix=prefix))


@contextmanager
def _image(stack: Stack, /) -> Generator[str]:
    """Build the image of this checkout, load it into the cluster under a tag of its own, and remove it from this
    machine when done. What the build cached stays, for the next build."""
    with tempfile.TemporaryDirectory() as scratch:
        written = Path(scratch) / "image"
        _run("docker", "build", "--file", "reliability/Dockerfile", "--iidfile", str(written), ".")
        identity = written.read_text().strip()
    image = f"casty-kube:{identity.removeprefix('sha256:')[:12]}"
    _run("docker", "tag", identity, image)
    try:
        _run("kind", "load", "docker-image", image, "--name", stack.cluster)
        yield image
    finally:
        _run("docker", "image", "rm", image, alone=True)


def _runner(image: str, plan: dict[str, object], output: Path, /) -> dict[str, object]:
    def field(path: str) -> dict[str, object]:
        return {"fieldRef": {"fieldPath": path}}

    env: list[dict[str, object]] = [
        {"name": "KUBE_PLAN", "value": json.dumps(plan)},
        {"name": "KUBE_OUTPUT", "value": str(output)},
        {"name": "KUBE_CPU", "value": os.environ.get("KUBE_CPU", "")},
        {"name": "KUBE_MEMORY", "value": os.environ.get("KUBE_MEMORY", "")},
        {"name": "KUBE_IMAGE", "value": image},
        {"name": "KUBE_POD", "valueFrom": field("metadata.name")},
        {"name": "KUBE_UID", "valueFrom": field("metadata.uid")},
    ]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": RUNNER, "labels": {"casty/role": "runner"}},
        "spec": {
            "serviceAccountName": RUNNER,
            "restartPolicy": "Never",
            "enableServiceLinks": False,
            "containers": [{"name": RUNNER, "image": image, "args": ["-m", "reliability.runner"], "env": env}],
        },
    }


def _follow(kubectl: Sequence[str], /) -> bool:
    """Print what the runner says until it says it finished; answer whether the run passed."""
    logs = subprocess.Popen([*kubectl, "logs", "--follow", RUNNER], stdout=subprocess.PIPE, text=True)
    assert logs.stdout is not None
    with logs:
        for line in logs.stdout:
            print(line, end="", flush=True)
            if line.startswith('{"finished"'):
                logs.terminate()
                return records.record(json.loads(line))["finished"] is None
    raise SystemExit("the runner ended without finishing; its log is above")


def _interrupted(signum: int, frame: FrameType | None, /) -> None:
    print("interrupted: the run ends, and what it brought up is destroyed", file=sys.stderr, flush=True)
    # `uv run` passes on the Ctrl-C it gets as well, which says nothing new.
    signal.signal(signal.SIGINT, _heard)


def _heard(signum: int, frame: FrameType | None, /) -> None:
    """A Ctrl-C after the first, whose run is already ending."""


def _run(*command: str, given: str | None = None, alone: bool = False) -> None:
    """Run `command` from the root of the checkout; `alone`, in a session of its own, which Ctrl-C does not reach."""
    subprocess.run(command, cwd=ROOT, input=given, text=True, check=True, start_new_session=alone)


def _pulumi(*arguments: str, quiet: bool = False, alone: bool = False) -> str:
    """Run `pulumi` on the project of `reliability/infra`, whose stacks keep their state in ~/.pulumi, and answer what
    it printed if `quiet`; `alone`, as `_run` does. The secrets of the stacks need a passphrase, even an empty one."""
    environment = {"PULUMI_CONFIG_PASSPHRASE": ""} | dict(os.environ)
    done = subprocess.run(
        ["pulumi", *arguments],
        cwd=INFRA,
        env=environment,
        check=True,
        text=True,
        capture_output=quiet,
        start_new_session=alone,
    )
    return done.stdout if quiet else ""


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
