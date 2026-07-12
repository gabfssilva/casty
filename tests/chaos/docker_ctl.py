"""Fault injection against the compose cluster, via the docker socket mounted
into the driver container."""

from __future__ import annotations

import uuid

import docker
from docker.models.containers import Container

NETWORK = "casty-chaos"
IMAGE = "casty-chaos"
NODE_LABEL = "casty.chaos.node"
SEED_LABEL = "casty.chaos.seed"
SPAWNED_LABEL = "casty.chaos.spawned"
PORT = 9000


def client() -> docker.DockerClient:
    return docker.from_env()


def members(dc: docker.DockerClient) -> list[Container]:
    """Every running cluster member, seed included."""
    containers: list[Container] = dc.containers.list(
        filters={"label": NODE_LABEL, "status": "running"}
    )
    return containers


def killable(dc: docker.DockerClient) -> list[Container]:
    return [c for c in members(dc) if SEED_LABEL not in c.labels]


def seed_addr(dc: docker.DockerClient) -> str:
    seed = dc.containers.list(filters={"label": SEED_LABEL, "status": "running"})[0]
    return addr_of(seed)


def addr_of(container: Container) -> str:
    container.reload()
    ip = container.attrs["NetworkSettings"]["Networks"][NETWORK]["IPAddress"]
    return f"{ip}:{PORT}"


def spawn_node(dc: docker.DockerClient) -> Container:
    return dc.containers.run(
        IMAGE,
        command=["python", "-m", "tests.chaos.node_main"],
        name=f"casty-chaos-spawn-{uuid.uuid4().hex[:8]}",
        network=NETWORK,
        environment={"CASTY_SEEDS": f"seed:{PORT}"},
        cap_add=["NET_ADMIN"],
        labels={NODE_LABEL: "1", SPAWNED_LABEL: "1"},
        detach=True,
    )


def netem_apply(container: Container, spec: str) -> None:
    """e.g. spec="delay 50ms 10ms" or "delay 400ms 100ms loss 10%"."""
    code, output = container.exec_run(
        ["tc", "qdisc", "replace", "dev", "eth0", "root", "netem", *spec.split()]
    )
    assert code == 0, output.decode()


def netem_clear(container: Container) -> None:
    container.exec_run(["tc", "qdisc", "del", "dev", "eth0", "root"])
