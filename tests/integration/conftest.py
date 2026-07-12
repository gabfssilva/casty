from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from casty.config import TLS
from casty.transport.connection import LocalNode


def make_local(cluster: str = "test-cluster") -> LocalNode:
    return LocalNode(node_id=uuid.uuid4(), cluster_name=cluster)


@dataclass(frozen=True)
class Certs:
    ca: str
    node_tls: TLS


def _openssl(*args: str) -> None:
    subprocess.run(["openssl", *args], check=True, capture_output=True)


@pytest.fixture(scope="session")
def certs(tmp_path_factory: pytest.TempPathFactory) -> Certs:
    d: Path = tmp_path_factory.mktemp("certs")
    ca_key, ca_crt = str(d / "ca.key"), str(d / "ca.crt")
    node_key, node_csr, node_crt = str(d / "node.key"), str(d / "node.csr"), str(d / "node.crt")
    _openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-keyout", ca_key, "-out", ca_crt, "-subj", "/CN=casty-test-ca",
    )
    _openssl(
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", node_key, "-out", node_csr, "-subj", "/CN=casty-test-node",
    )
    _openssl(
        "x509", "-req", "-in", node_csr, "-CA", ca_crt, "-CAkey", ca_key,
        "-CAcreateserial", "-days", "1", "-out", node_crt,
    )
    return Certs(ca=ca_crt, node_tls=TLS(cert=node_crt, key=node_key, ca=ca_crt))
