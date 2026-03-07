"""Certificate generation commands for Casty mTLS.

Requires the ``casty[cert]`` optional extra (``cryptography`` library).
"""

from __future__ import annotations

import argparse
import datetime
import ipaddress
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from argparse import _SubParsersAction  # pyright: ignore[reportPrivateUsage]


def _require_cryptography() -> None:
    try:
        import cryptography  # noqa: F401  # pyright: ignore[reportUnusedImport]
    except ImportError:
        print(
            "Error: cryptography is required for cert commands.\n"
            "Install it with: pip install casty[cert]",
            file=sys.stderr,
        )
        sys.exit(1)


def register(sub: _SubParsersAction[argparse.ArgumentParser]) -> None:
    # create-ca
    ca = sub.add_parser("create-ca", help="Create a new certificate authority")
    ca.add_argument(
        "--out", required=True, help="Output directory for ca.crt and ca.key"
    )
    ca.add_argument(
        "--validity", type=int, default=365, help="Validity in days (default: 365)"
    )
    ca.add_argument(
        "--cn", default="casty-ca", help="CA common name (default: casty-ca)"
    )
    ca.add_argument("--force", action="store_true", help="Overwrite existing files")
    ca.set_defaults(func=cmd_create_ca)

    # create-node
    node = sub.add_parser(
        "create-node", help="Create a node certificate signed by the CA"
    )
    node.add_argument(
        "addresses", nargs="+", help="DNS names and/or IP addresses for SANs"
    )
    node.add_argument(
        "--ca-dir", required=True, help="Directory containing ca.crt and ca.key"
    )
    node.add_argument(
        "--out", required=True, help="Output directory for node.crt and node.key"
    )
    node.add_argument(
        "--validity", type=int, default=90, help="Validity in days (default: 90)"
    )
    node.add_argument("--cn", default=None, help="Common name (default: first address)")
    node.add_argument("--force", action="store_true", help="Overwrite existing files")
    node.set_defaults(func=cmd_create_node)

    # create-client
    client = sub.add_parser(
        "create-client", help="Create a client certificate signed by the CA"
    )
    client.add_argument("name", help="Client identity name (used as CN)")
    client.add_argument(
        "--ca-dir", required=True, help="Directory containing ca.crt and ca.key"
    )
    client.add_argument(
        "--out", required=True, help="Output directory for client.crt and client.key"
    )
    client.add_argument(
        "--validity", type=int, default=90, help="Validity in days (default: 90)"
    )
    client.add_argument("--force", action="store_true", help="Overwrite existing files")
    client.set_defaults(func=cmd_create_client)

    # info
    info = sub.add_parser("info", help="Show certificate details")
    info.add_argument("cert_path", help="Path to a PEM certificate file")
    info.set_defaults(func=cmd_info)

    # list
    ls = sub.add_parser("list", help="List certificates in a directory")
    ls.add_argument("directory", help="Directory to scan for .crt/.pem files")
    ls.set_defaults(func=cmd_list)


def cmd_create_ca(args: argparse.Namespace) -> None:
    _require_cryptography()

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    ca_crt = out / "ca.crt"
    ca_key_path = out / "ca.key"

    if not args.force and (ca_crt.exists() or ca_key_path.exists()):
        print(
            f"Error: files already exist in {out}. Use --force to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)

    ca_key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.UTC)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, args.cn)])

    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=args.validity))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                key_cert_sign=True,
                crl_sign=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    ca_crt.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    ca_key_path.write_bytes(
        ca_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    print(f"CA certificate: {ca_crt}")
    print(f"CA private key: {ca_key_path}")
    print(f"Validity: {args.validity} days")
    print(f"CN: {args.cn}")


def cmd_create_node(args: argparse.Namespace) -> None:
    _require_cryptography()

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_dir = Path(args.ca_dir)
    ca_crt_path = ca_dir / "ca.crt"
    ca_key_path = ca_dir / "ca.key"

    if not ca_crt_path.exists() or not ca_key_path.exists():
        print(f"Error: CA files not found in {ca_dir}", file=sys.stderr)
        sys.exit(1)

    ca_cert = x509.load_pem_x509_certificate(ca_crt_path.read_bytes())
    ca_private_key = serialization.load_pem_private_key(
        ca_key_path.read_bytes(), password=None
    )
    assert isinstance(ca_private_key, ec.EllipticCurvePrivateKey)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    node_crt = out / "node.crt"
    node_key_path = out / "node.key"

    if not args.force and (node_crt.exists() or node_key_path.exists()):
        print(
            f"Error: files already exist in {out}. Use --force to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)

    node_key = ec.generate_private_key(ec.SECP256R1())
    cn = args.cn or args.addresses[0]
    now = datetime.datetime.now(datetime.UTC)

    sans: list[x509.GeneralName] = []
    for addr in args.addresses:
        try:
            ip = ipaddress.ip_address(addr)
            sans.append(x509.IPAddress(ip))
        except ValueError:
            sans.append(x509.DNSName(addr))

    node_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
        .issuer_name(ca_cert.subject)
        .public_key(node_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=args.validity))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=False,
                crl_sign=False,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    ExtendedKeyUsageOID.SERVER_AUTH,
                    ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .sign(ca_private_key, hashes.SHA256())
    )

    node_crt.write_bytes(node_cert.public_bytes(serialization.Encoding.PEM))
    node_key_path.write_bytes(
        node_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    san_display = ", ".join(args.addresses)
    print(f"Node certificate: {node_crt}")
    print(f"Node private key: {node_key_path}")
    print(f"SANs: {san_display}")
    print(f"Validity: {args.validity} days")
    print(f"Signed by: {ca_crt_path}")


def cmd_create_client(args: argparse.Namespace) -> None:
    _require_cryptography()

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_dir = Path(args.ca_dir)
    ca_crt_path = ca_dir / "ca.crt"
    ca_key_path = ca_dir / "ca.key"

    if not ca_crt_path.exists() or not ca_key_path.exists():
        print(f"Error: CA files not found in {ca_dir}", file=sys.stderr)
        sys.exit(1)

    ca_cert = x509.load_pem_x509_certificate(ca_crt_path.read_bytes())
    ca_private_key = serialization.load_pem_private_key(
        ca_key_path.read_bytes(), password=None
    )
    assert isinstance(ca_private_key, ec.EllipticCurvePrivateKey)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    client_crt = out / "client.crt"
    client_key_path = out / "client.key"

    if not args.force and (client_crt.exists() or client_key_path.exists()):
        print(
            f"Error: files already exist in {out}. Use --force to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)

    client_key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.UTC)

    client_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, args.name)]))
        .issuer_name(ca_cert.subject)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=args.validity))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=False,
                crl_sign=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .sign(ca_private_key, hashes.SHA256())
    )

    client_crt.write_bytes(client_cert.public_bytes(serialization.Encoding.PEM))
    client_key_path.write_bytes(
        client_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    print(f"Client certificate: {client_crt}")
    print(f"Client private key: {client_key_path}")
    print(f"CN: {args.name}")
    print(f"Validity: {args.validity} days")
    print(f"Signed by: {ca_crt_path}")


def cmd_info(args: argparse.Namespace) -> None:
    _require_cryptography()

    from cryptography.x509 import (
        BasicConstraints,
        DNSName,
        IPAddress,
        SubjectAlternativeName,
        load_pem_x509_certificate,
    )
    from cryptography.x509.oid import NameOID

    path = Path(args.cert_path)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    cert = load_pem_x509_certificate(path.read_bytes())

    cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    issuer_cn = cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME)

    try:
        bc = cert.extensions.get_extension_for_class(BasicConstraints)
        is_ca = bc.value.ca
    except Exception:
        is_ca = False

    sans: list[str] = []
    try:
        san_ext = cert.extensions.get_extension_for_class(SubjectAlternativeName)
        for name in san_ext.value.get_values_for_type(DNSName):
            sans.append(f"DNS:{name}")
        for ip in san_ext.value.get_values_for_type(IPAddress):
            sans.append(f"IP:{ip}")
    except Exception:
        pass

    days_remaining = (
        cert.not_valid_after_utc - datetime.datetime.now(datetime.UTC)
    ).days
    algo = cert.signature_hash_algorithm
    algo_name = algo.name.upper() if algo else "unknown"
    key = cert.public_key()
    key_info = type(key).__name__.replace("PublicKey", "").replace("_", "")

    print(f"Certificate:  {path}")
    print(f"  Subject:    CN={cn[0].value if cn else 'N/A'}")
    print(f"  Issuer:     CN={issuer_cn[0].value if issuer_cn else 'N/A'}")
    print(f"  CA:         {'yes' if is_ca else 'no'}")
    print(
        f"  Valid:      {cert.not_valid_before_utc:%Y-%m-%d} -> {cert.not_valid_after_utc:%Y-%m-%d} ({days_remaining} days remaining)"
    )
    if sans:
        print(f"  SANs:       {', '.join(sans)}")
    print(f"  Algorithm:  {key_info} + {algo_name}")


def cmd_list(args: argparse.Namespace) -> None:
    _require_cryptography()

    from cryptography.x509 import (
        BasicConstraints,
        DNSName,
        IPAddress,
        SubjectAlternativeName,
        load_pem_x509_certificate,
    )
    from cryptography.x509.oid import NameOID

    directory = Path(args.directory)
    cert_files = sorted(directory.glob("*.crt")) + sorted(directory.glob("*.pem"))

    if not cert_files:
        print(f"No certificates found in {directory}")
        return

    for cert_path in cert_files:
        try:
            cert = load_pem_x509_certificate(cert_path.read_bytes())
        except Exception:
            continue

        cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn_str = cn[0].value if cn else "N/A"

        try:
            bc = cert.extensions.get_extension_for_class(BasicConstraints)
            is_ca = bc.value.ca
        except Exception:
            is_ca = False

        days_remaining = (
            cert.not_valid_after_utc - datetime.datetime.now(datetime.UTC)
        ).days

        sans: list[str] = []
        try:
            san_ext = cert.extensions.get_extension_for_class(SubjectAlternativeName)
            for name in san_ext.value.get_values_for_type(DNSName):
                sans.append(name)
            for ip in san_ext.value.get_values_for_type(IPAddress):
                sans.append(str(ip))
        except Exception:
            pass

        kind = "CA" if is_ca else "node/client"
        san_str = f"  SANs: {', '.join(sans)}" if sans else ""
        expiry_warning = " [EXPIRED]" if days_remaining < 0 else ""

        print(
            f"  {cert_path.name:<20} CN={cn_str:<20} {kind:<12} {days_remaining:>4}d remaining{expiry_warning}{san_str}"
        )
