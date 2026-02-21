"""
End-to-end Docker cluster tests using testcontainers + Docker Compose.

Each test spins up a compose environment with symmetric nodes,
waits for all containers to exit, and asserts expected output in logs.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)


def _run_cluster(services: list[str]) -> DockerCompose:
    os.environ["NODES"] = str(len(services))
    compose = DockerCompose(
        context=PROJECT_ROOT,
        compose_file_name="tests/docker-compose.test.yml",
        build=True,
        wait=False,
        services=services,
    )
    project = f"casty-test-{uuid.uuid4().hex[:8]}"
    compose.compose_command_property = [
        *compose.compose_command_property,
        "-p",
        project,
    ]
    compose.start()
    return compose


def _wait_all_exited(compose: DockerCompose, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        containers = compose.get_containers(include_all=True)
        if containers and all(c.State == "exited" for c in containers):
            return
        time.sleep(1.0)
    states = {c.Name: c.State for c in compose.get_containers(include_all=True)}
    msg = f"Containers did not exit within {timeout}s: {states}"
    raise TimeoutError(msg)


@pytest.mark.timeout(120)
class TestContaineredCluster:
    def test_single_node(self) -> None:
        """Single seed node writes counter=10 and reads it back."""
        compose = _run_cluster(services=["node-1"])
        try:
            _wait_all_exited(compose)
            stdout, stderr = compose.get_logs()
            logs = stdout + stderr
            assert "RESULT=10" in logs, f"Expected RESULT=10 in logs:\n{logs}"
        finally:
            compose.stop()

    def test_three_nodes(self) -> None:
        """3-node cluster: seed writes, all nodes read."""
        services = [f"node-{i}" for i in range(1, 4)]
        compose = _run_cluster(services=services)
        try:
            _wait_all_exited(compose)
            stdout, stderr = compose.get_logs()
            logs = stdout + stderr
            assert logs.count("RESULT=10") >= 3, f"Expected 3 results:\n{logs}"
        finally:
            compose.stop()

    @pytest.mark.parametrize("n_nodes", [3, 6])
    def test_cluster_scales(self, n_nodes: int) -> None:
        """Cluster works with N nodes."""
        services = [f"node-{i}" for i in range(1, n_nodes + 1)]
        compose = _run_cluster(services=services)
        try:
            _wait_all_exited(compose, timeout=90.0)
            stdout, stderr = compose.get_logs()
            logs = stdout + stderr
            result_count = logs.count("RESULT=10")
            assert result_count >= n_nodes, (
                f"Expected {n_nodes} results, got {result_count}:\n{logs}"
            )
        finally:
            compose.stop()
