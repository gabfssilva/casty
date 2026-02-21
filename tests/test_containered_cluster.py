"""
End-to-end Docker cluster tests.

Each test spins up a compose environment with symmetric nodes,
waits for all containers to exit, and asserts expected output in logs.
"""

from __future__ import annotations

import subprocess
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = "tests/docker-compose.test.yml"


def _run_cluster(services: list[str], *, timeout: float = 120.0) -> str:
    project = f"casty-test-{uuid.uuid4().hex[:8]}"
    env_nodes = str(len(services))

    try:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                COMPOSE_FILE,
                "-p",
                project,
                "up",
                "--build",
                *services,
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=PROJECT_ROOT,
            env={"NODES": env_nodes, "PATH": "/usr/local/bin:/usr/bin:/bin"},
        )
    except subprocess.TimeoutExpired as exc:
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "-p", project, "down"],
            cwd=PROJECT_ROOT,
            capture_output=True,
        )
        msg = f"Cluster did not finish within {timeout}s"
        raise TimeoutError(msg) from exc
    finally:
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "-p", project, "down"],
            cwd=PROJECT_ROOT,
            capture_output=True,
        )

    return result.stdout + result.stderr


@pytest.mark.timeout(180)
class TestContaineredCluster:
    def test_single_node(self) -> None:
        """Single seed node writes counter=10 and reads it back."""
        logs = _run_cluster(["node-1"])
        assert "RESULT=10" in logs, f"Expected RESULT=10 in logs:\n{logs}"

    def test_three_nodes(self) -> None:
        """3-node cluster: seed writes, all nodes read."""
        logs = _run_cluster([f"node-{i}" for i in range(1, 4)])
        assert logs.count("RESULT=10") >= 3, f"Expected 3 results:\n{logs}"

    @pytest.mark.parametrize("n_nodes", [3, 6])
    def test_cluster_scales(self, n_nodes: int) -> None:
        """Cluster works with N nodes."""
        services = [f"node-{i}" for i in range(1, n_nodes + 1)]
        logs = _run_cluster(services)
        result_count = logs.count("RESULT=10")
        assert result_count >= n_nodes, (
            f"Expected {n_nodes} results, got {result_count}:\n{logs}"
        )
