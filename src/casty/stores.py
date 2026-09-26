"""The stores casty carries, written in Rust.

Each is a `casty.Store`, which the nodes of a cluster call from their own threads, without the event loop.
"""

from __future__ import annotations

from importlib import import_module

SQL = import_module("casty._casty").SQL

__all__ = ["SQL"]
