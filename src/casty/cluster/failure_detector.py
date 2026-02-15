"""Phi accrual failure detection for cluster heartbeat monitoring.

Implements the phi accrual failure detector described by Hayashibara et al.,
which outputs a continuous suspicion level rather than a binary alive/dead
decision.  This allows each consumer to choose its own threshold.
"""

from __future__ import annotations

import math
import statistics
import time
from collections import deque


class PhiAccrualFailureDetector:
    """Phi accrual failure detector (Hayashibara et al.).

    Outputs a continuous suspicion level (phi) instead of a binary alive/dead
    signal.  ``phi = -log10(1 - CDF(elapsed))`` where CDF is the normal
    distribution fitted to the observed heartbeat interval history.

    Parameters
    ----------
    threshold : float
        Phi value above which a node is considered unreachable.  Higher values
        tolerate more jitter but detect failures more slowly.
    max_sample_size : int
        Maximum number of heartbeat intervals to keep per node.
    min_std_deviation_ms : float
        Floor for the standard deviation estimate, preventing overly
        aggressive detection when intervals are very stable.
    acceptable_heartbeat_pause_ms : float
        Additional grace period added to the mean estimate, accounting for
        expected pauses (e.g. GC).
    first_heartbeat_estimate_ms : float
        Assumed mean interval before enough samples have been collected.

    Examples
    --------
    >>> fd = PhiAccrualFailureDetector(threshold=8.0)
    >>> fd.heartbeat("node-1")
    >>> fd.is_available("node-1")
    True
    >>> fd.phi("unknown-node")
    0.0
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
        """Record arrival of a heartbeat from a node.

        Parameters
        ----------
        node : str
            Identifier of the node that sent the heartbeat.
        """
        now = time.monotonic()
        if node in self._last_heartbeat:
            interval_ms = (now - self._last_heartbeat[node]) * 1000.0
            if node not in self._history:
                self._history[node] = deque(maxlen=self._max_sample_size)
            self._history[node].append(interval_ms)
        self._last_heartbeat[node] = now

    def phi(self, node: str) -> float:
        """Calculate the suspicion level for *node*.

        Parameters
        ----------
        node : str
            Identifier of the node to evaluate.

        Returns
        -------
        float
            The phi value.  ``0.0`` if *node* has never sent a heartbeat;
            ``inf`` if the node is almost certainly down.
        """
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
        """Return the set of node keys that have received at least one heartbeat.

        Returns
        -------
        frozenset[str]
            Node identifiers currently being tracked.
        """
        return frozenset(self._last_heartbeat.keys())

    def remove(self, node: str) -> None:
        """Stop tracking a node entirely.

        Called when a node is marked ``down`` so it no longer accumulates
        stale history in the failure detector.

        Parameters
        ----------
        node : str
            Identifier of the node to remove.
        """
        self._last_heartbeat.pop(node, None)
        self._history.pop(node, None)

    def is_available(self, node: str) -> bool:
        """Check if a node is considered available (phi below threshold).

        Parameters
        ----------
        node : str
            Identifier of the node to check.

        Returns
        -------
        bool
            ``True`` if ``phi(node) < threshold``.
        """
        return self.phi(node) < self._threshold
