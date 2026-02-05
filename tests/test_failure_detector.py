from __future__ import annotations

import time
from unittest.mock import patch

from casty.failure_detector import PhiAccrualFailureDetector


def test_no_heartbeat_returns_zero_phi() -> None:
    detector = PhiAccrualFailureDetector()
    assert detector.phi("node-1") == 0.0


def test_recent_heartbeat_is_available() -> None:
    detector = PhiAccrualFailureDetector(threshold=8.0)
    detector.heartbeat("node-1")
    assert detector.is_available("node-1")


def test_stale_heartbeat_becomes_unavailable() -> None:
    detector = PhiAccrualFailureDetector(
        threshold=3.0, first_heartbeat_estimate_ms=100.0
    )
    detector.heartbeat("node-1")
    with patch("casty.failure_detector.time") as mock_time:
        mock_time.monotonic.return_value = time.monotonic() + 10.0
        assert not detector.is_available("node-1")


def test_phi_increases_with_elapsed_time() -> None:
    detector = PhiAccrualFailureDetector(first_heartbeat_estimate_ms=1000.0)
    detector.heartbeat("node-1")
    base = time.monotonic()
    with patch("casty.failure_detector.time") as mock_time:
        mock_time.monotonic.return_value = base + 1.0
        phi_1s = detector.phi("node-1")
        mock_time.monotonic.return_value = base + 5.0
        phi_5s = detector.phi("node-1")
    assert phi_5s > phi_1s


def test_regular_heartbeats_stay_available() -> None:
    detector = PhiAccrualFailureDetector(threshold=8.0)
    base = time.monotonic()
    for i in range(10):
        with patch("casty.failure_detector.time") as mock_time:
            mock_time.monotonic.return_value = base + i * 1.0
            detector.heartbeat("node-1")
    with patch("casty.failure_detector.time") as mock_time:
        mock_time.monotonic.return_value = base + 10.5
        assert detector.is_available("node-1")


def test_max_sample_size_respected() -> None:
    detector = PhiAccrualFailureDetector(max_sample_size=5)
    base = time.monotonic()
    for i in range(20):
        with patch("casty.failure_detector.time") as mock_time:
            mock_time.monotonic.return_value = base + i * 1.0
            detector.heartbeat("node-1")
    assert len(detector._history["node-1"]) <= 5


def test_multiple_nodes_independent() -> None:
    detector = PhiAccrualFailureDetector(threshold=3.0)
    detector.heartbeat("node-1")
    detector.heartbeat("node-2")
    assert detector.is_available("node-1")
    assert detector.is_available("node-2")
