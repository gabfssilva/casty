"""Casty CLI — certificate management and utilities."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from argparse import _SubParsersAction  # pyright: ignore[reportPrivateUsage]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="casty", description="Casty CLI utilities")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    # casty cert ...
    cert_parser = subparsers.add_parser("cert", help="TLS certificate management")
    cert_sub = cert_parser.add_subparsers(dest="cert_command")
    cert_sub.required = True

    _register_cert_commands(cert_sub)

    args = parser.parse_args(argv)
    args.func(args)


def _register_cert_commands(sub: _SubParsersAction[argparse.ArgumentParser]) -> None:
    from casty.cli.cert import register

    register(sub)


if __name__ == "__main__":
    main()
