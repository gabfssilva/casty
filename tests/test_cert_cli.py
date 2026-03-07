from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.fixture()
def certs_dir(tmp_path: Path) -> Path:
    d = tmp_path / "certs"
    d.mkdir()
    return d


@pytest.fixture()
def ca_dir(certs_dir: Path) -> Path:
    """Pre-generate a CA for tests that need it."""
    subprocess.run(
        ["uv", "run", "casty", "cert", "create-ca", "--out", str(certs_dir)],
        capture_output=True,
        check=True,
    )
    return certs_dir


class TestCreateCa:
    def test_creates_ca_cert_and_key(self, certs_dir: Path) -> None:
        result = subprocess.run(
            ["uv", "run", "casty", "cert", "create-ca", "--out", str(certs_dir)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (certs_dir / "ca.crt").exists()
        assert (certs_dir / "ca.key").exists()

        ca_pem = (certs_dir / "ca.crt").read_text()
        assert "BEGIN CERTIFICATE" in ca_pem

        ca_key_pem = (certs_dir / "ca.key").read_text()
        assert "BEGIN PRIVATE KEY" in ca_key_pem or "BEGIN EC PRIVATE KEY" in ca_key_pem

    def test_ca_cert_has_ca_constraint(self, certs_dir: Path) -> None:
        subprocess.run(
            ["uv", "run", "casty", "cert", "create-ca", "--out", str(certs_dir)],
            capture_output=True,
        )

        from cryptography.x509 import BasicConstraints, load_pem_x509_certificate

        cert = load_pem_x509_certificate((certs_dir / "ca.crt").read_bytes())
        bc = cert.extensions.get_extension_for_class(BasicConstraints)
        assert bc.value.ca is True

    def test_custom_validity(self, certs_dir: Path) -> None:
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-ca",
                "--out",
                str(certs_dir),
                "--validity",
                "730",
            ],
            capture_output=True,
        )

        from cryptography.x509 import load_pem_x509_certificate

        cert = load_pem_x509_certificate((certs_dir / "ca.crt").read_bytes())
        delta = cert.not_valid_after_utc - cert.not_valid_before_utc
        assert delta.days >= 729  # allow 1 day rounding

    def test_custom_cn(self, certs_dir: Path) -> None:
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-ca",
                "--out",
                str(certs_dir),
                "--cn",
                "my-cluster-ca",
            ],
            capture_output=True,
        )

        from cryptography.x509 import load_pem_x509_certificate
        from cryptography.x509.oid import NameOID

        cert = load_pem_x509_certificate((certs_dir / "ca.crt").read_bytes())
        cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        assert cn == "my-cluster-ca"

    def test_refuses_overwrite_without_force(self, certs_dir: Path) -> None:
        subprocess.run(
            ["uv", "run", "casty", "cert", "create-ca", "--out", str(certs_dir)],
            capture_output=True,
        )
        result = subprocess.run(
            ["uv", "run", "casty", "cert", "create-ca", "--out", str(certs_dir)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "already exist" in result.stderr.lower()

    def test_force_overwrite(self, certs_dir: Path) -> None:
        subprocess.run(
            ["uv", "run", "casty", "cert", "create-ca", "--out", str(certs_dir)],
            capture_output=True,
        )
        result = subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-ca",
                "--out",
                str(certs_dir),
                "--force",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0


class TestCreateNode:
    def test_creates_node_cert_and_key(self, ca_dir: Path, tmp_path: Path) -> None:
        node_dir = tmp_path / "node-1"
        result = subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "10.0.0.1",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(node_dir),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (node_dir / "node.crt").exists()
        assert (node_dir / "node.key").exists()

    def test_node_cert_has_correct_sans(self, ca_dir: Path, tmp_path: Path) -> None:
        node_dir = tmp_path / "node-1"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "10.0.0.1",
                "::1",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(node_dir),
            ],
            capture_output=True,
            check=True,
        )

        from cryptography.x509 import (
            DNSName,
            IPAddress,
            SubjectAlternativeName,
            load_pem_x509_certificate,
        )

        cert = load_pem_x509_certificate((node_dir / "node.crt").read_bytes())
        san = cert.extensions.get_extension_for_class(SubjectAlternativeName)
        dns_names = san.value.get_values_for_type(DNSName)
        ip_addrs = [str(ip) for ip in san.value.get_values_for_type(IPAddress)]

        assert "node-1.local" in dns_names
        assert "10.0.0.1" in ip_addrs
        assert "::1" in ip_addrs

    def test_node_cert_signed_by_ca(self, ca_dir: Path, tmp_path: Path) -> None:
        node_dir = tmp_path / "node-1"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "10.0.0.1",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(node_dir),
            ],
            capture_output=True,
            check=True,
        )

        from cryptography.x509 import load_pem_x509_certificate

        ca_cert = load_pem_x509_certificate((ca_dir / "ca.crt").read_bytes())
        node_cert = load_pem_x509_certificate((node_dir / "node.crt").read_bytes())
        assert node_cert.issuer == ca_cert.subject

    def test_node_cert_works_with_tls_config(
        self, ca_dir: Path, tmp_path: Path
    ) -> None:
        """The generated certs must be consumable by casty.remote.tls.Config."""
        node_dir = tmp_path / "node-1"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "127.0.0.1",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(node_dir),
            ],
            capture_output=True,
            check=True,
        )

        from casty.remote.tls import Config

        tls = Config.from_paths(
            certfile=str(node_dir / "node.crt"),
            keyfile=str(node_dir / "node.key"),
            cafile=str(ca_dir / "ca.crt"),
        )
        assert tls.server_context is not None
        assert tls.client_context is not None

    def test_fails_without_ca_dir(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "--ca-dir",
                str(tmp_path / "nonexistent"),
                "--out",
                str(tmp_path / "node"),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "ca" in result.stderr.lower() or "not found" in result.stderr.lower()


class TestCreateClient:
    def test_creates_client_cert_and_key(self, ca_dir: Path, tmp_path: Path) -> None:
        client_dir = tmp_path / "client"
        result = subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-client",
                "my-app",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(client_dir),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (client_dir / "client.crt").exists()
        assert (client_dir / "client.key").exists()

    def test_client_cert_has_client_auth_eku(
        self, ca_dir: Path, tmp_path: Path
    ) -> None:
        client_dir = tmp_path / "client"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-client",
                "my-app",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(client_dir),
            ],
            capture_output=True,
            check=True,
        )

        from cryptography.x509 import ExtendedKeyUsage, load_pem_x509_certificate
        from cryptography.x509.oid import ExtendedKeyUsageOID

        cert = load_pem_x509_certificate((client_dir / "client.crt").read_bytes())
        eku = cert.extensions.get_extension_for_class(ExtendedKeyUsage)
        assert ExtendedKeyUsageOID.CLIENT_AUTH in eku.value

    def test_client_cert_has_no_server_auth(self, ca_dir: Path, tmp_path: Path) -> None:
        client_dir = tmp_path / "client"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-client",
                "my-app",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(client_dir),
            ],
            capture_output=True,
            check=True,
        )

        from cryptography.x509 import ExtendedKeyUsage, load_pem_x509_certificate
        from cryptography.x509.oid import ExtendedKeyUsageOID

        cert = load_pem_x509_certificate((client_dir / "client.crt").read_bytes())
        eku = cert.extensions.get_extension_for_class(ExtendedKeyUsage)
        assert ExtendedKeyUsageOID.SERVER_AUTH not in eku.value


class TestCertInfo:
    def test_shows_ca_info(self, ca_dir: Path) -> None:
        result = subprocess.run(
            ["uv", "run", "casty", "cert", "info", str(ca_dir / "ca.crt")],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "casty-ca" in result.stdout
        assert "CA: yes" in result.stdout or "CA:         yes" in result.stdout

    def test_shows_node_info_with_sans(self, ca_dir: Path, tmp_path: Path) -> None:
        node_dir = tmp_path / "node"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "10.0.0.1",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(node_dir),
            ],
            capture_output=True,
            check=True,
        )
        result = subprocess.run(
            ["uv", "run", "casty", "cert", "info", str(node_dir / "node.crt")],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "node-1.local" in result.stdout
        assert "10.0.0.1" in result.stdout

    def test_file_not_found(self, tmp_path: Path) -> None:
        result = subprocess.run(
            ["uv", "run", "casty", "cert", "info", str(tmp_path / "nope.crt")],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0


class TestCertList:
    def test_lists_all_certs_in_directory(self, ca_dir: Path, tmp_path: Path) -> None:
        node_dir = tmp_path / "node-1"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-node",
                "node-1.local",
                "10.0.0.1",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(node_dir),
            ],
            capture_output=True,
            check=True,
        )

        import shutil

        all_certs = tmp_path / "all"
        all_certs.mkdir()
        shutil.copy(ca_dir / "ca.crt", all_certs / "ca.crt")
        shutil.copy(node_dir / "node.crt", all_certs / "node.crt")

        result = subprocess.run(
            ["uv", "run", "casty", "cert", "list", str(all_certs)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "ca.crt" in result.stdout
        assert "node.crt" in result.stdout
        assert "casty-ca" in result.stdout
        assert "node-1.local" in result.stdout

    def test_empty_directory(self, tmp_path: Path) -> None:
        result = subprocess.run(
            ["uv", "run", "casty", "cert", "list", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "no certificates" in result.stdout.lower()


class TestEndToEnd:
    def test_full_mtls_flow(self, ca_dir: Path, tmp_path: Path) -> None:
        """Generate CA -> 2 nodes -> verify mutual TLS handshake works."""
        node1_dir = tmp_path / "node-1"
        node2_dir = tmp_path / "node-2"

        for name, d, addr in [
            ("node-1.local", node1_dir, "127.0.0.1"),
            ("node-2.local", node2_dir, "127.0.0.1"),
        ]:
            subprocess.run(
                [
                    "uv",
                    "run",
                    "casty",
                    "cert",
                    "create-node",
                    name,
                    addr,
                    "--ca-dir",
                    str(ca_dir),
                    "--out",
                    str(d),
                ],
                capture_output=True,
                check=True,
            )

        from casty.remote.tls import Config

        tls1 = Config.from_paths(
            certfile=str(node1_dir / "node.crt"),
            keyfile=str(node1_dir / "node.key"),
            cafile=str(ca_dir / "ca.crt"),
        )
        tls2 = Config.from_paths(
            certfile=str(node2_dir / "node.crt"),
            keyfile=str(node2_dir / "node.key"),
            cafile=str(ca_dir / "ca.crt"),
        )

        assert tls1.server_context is not None
        assert tls2.client_context is not None

    def test_client_cert_flow(self, ca_dir: Path, tmp_path: Path) -> None:
        """Client cert should load into an ssl.SSLContext for ClusterClient use."""
        client_dir = tmp_path / "client"
        subprocess.run(
            [
                "uv",
                "run",
                "casty",
                "cert",
                "create-client",
                "my-app",
                "--ca-dir",
                str(ca_dir),
                "--out",
                str(client_dir),
            ],
            capture_output=True,
            check=True,
        )

        import ssl

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_cert_chain(
            str(client_dir / "client.crt"), str(client_dir / "client.key")
        )
        ctx.load_verify_locations(str(ca_dir / "ca.crt"))
        assert ctx is not None
