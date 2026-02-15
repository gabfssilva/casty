"""Root conftest — registers CLI options available to all test suites."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--benchmark-save",
        action="store_true",
        default=False,
        help="Save benchmark results as baseline for this machine",
    )
    parser.addoption(
        "--benchmark-compare",
        action="store_true",
        default=False,
        help="Compare against saved baseline and fail on regression",
    )
