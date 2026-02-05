from __future__ import annotations

import math
import statistics
import time
from collections import deque


class PhiAccrualFailureDetector:
    """Phi Accrual Failure Detector (Hayashibara et al.)

    Outputs a continuous suspicion level (phi) instead of binary alive/dead.
    phi = -log10(1 - CDF(elapsed_time)) where CDF is normal distribution
    fitted to heartbeat interval history.
    """

    def __init__(
        self,
        *,
        threshold: float = 8.0,
        max_sample_size: int = 200,
        min_std_deviation_ms: float = 100.0,
        acceptable_heartbeat_pause_ms: float = 0.0,
        first_heartbeat_estimate_ms: float = 1000.0,
    ) -> None:
        self._threshold = threshold
        self._max_sample_size = max_sample_size
        self._min_std_deviation_ms = min_std_deviation_ms
        self._acceptable_heartbeat_pause_ms = acceptable_heartbeat_pause_ms
        self._first_heartbeat_estimate_ms = first_heartbeat_estimate_ms
        self._last_heartbeat: dict[str, float] = {}
        self._history: dict[str, deque[float]] = {}

    def heartbeat(self, node: str) -> None:
        """Record arrival of a heartbeat from a node."""
        now = time.monotonic()
        if node in self._last_heartbeat:
            interval_ms = (now - self._last_heartbeat[node]) * 1000.0
            if node not in self._history:
                self._history[node] = deque(maxlen=self._max_sample_size)
            self._history[node].append(interval_ms)
        self._last_heartbeat[node] = now

    def phi(self, node: str) -> float:
        """Calculate phi (suspicion level) for a node. Higher = more suspect."""
        if node not in self._last_heartbeat:
            return 0.0

        elapsed_ms = (time.monotonic() - self._last_heartbeat[node]) * 1000.0
        history = self._history.get(node)

        if not history or len(history) < 2:
            mean = self._first_heartbeat_estimate_ms
            std = mean / 4.0
        else:
            mean = statistics.mean(history)
            std = max(statistics.stdev(history), self._min_std_deviation_ms)

        mean += self._acceptable_heartbeat_pause_ms

        y = (elapsed_ms - mean) / std
        p = 0.5 * (1.0 + math.erf(y / math.sqrt(2.0)))

        if p >= 1.0:
            return float("inf")
        if p <= 0.0:
            return 0.0

        return -math.log10(1.0 - p)

    @property
    def tracked_nodes(self) -> frozenset[str]:
        """Return the set of node keys that have received at least one heartbeat."""
        return frozenset(self._last_heartbeat.keys())

    def is_available(self, node: str) -> bool:
        """Check if a node is considered available (phi below threshold)."""
        return self.phi(node) < self._threshold
