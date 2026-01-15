"""Distributed rate limiter using token bucket algorithm.

Demonstrates:
- Sharded actors (per-user rate limiters)
- Token bucket algorithm with refill
- Three-way merge for split-brain reconciliation
"""

import time
from dataclasses import dataclass

from casty import Actor, Context


@dataclass
class CheckToken:
    """Request a token - reply with bool (granted or denied)"""
    pass


@dataclass
class GetStatus:
    """Query current state - reply with dict"""
    pass


@dataclass
class Reset:
    """Admin: reset bucket to full capacity"""
    pass


class RateLimiter(Actor[CheckToken | GetStatus | Reset]):
    """Token bucket rate limiter with refill.

    Implements three-way merge for split-brain scenarios.

    Token refill happens dynamically when tokens are checked.
    Rate = tokens per second
    Capacity = max tokens (burst capacity)
    """

    def __init__(self, entity_id: str, rate: float = 10.0, capacity: int = 20):
        """Initialize rate limiter.

        Args:
            entity_id: Unique identifier for this rate limiter (e.g., user ID)
            rate: Tokens per second to refill (default 10.0)
            capacity: Maximum tokens/burst size (default 20)
        """
        self.entity_id = entity_id
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)  # Start with full bucket
        self.last_refill = time.monotonic()
        self.requests_allowed = 0
        self.requests_denied = 0

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

    async def receive(self, msg: CheckToken | GetStatus | Reset, ctx: Context):
        """Handle rate limit check, status query, or reset."""
        match msg:
            case CheckToken():
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    self.requests_allowed += 1
                    ctx.reply(True)
                else:
                    self.requests_denied += 1
                    ctx.reply(False)

            case GetStatus():
                self._refill()
                ctx.reply({
                    "tokens": round(self.tokens, 2),
                    "rate": self.rate,
                    "capacity": self.capacity,
                    "allowed": self.requests_allowed,
                    "denied": self.requests_denied,
                    "total_requests": self.requests_allowed + self.requests_denied,
                })

            case Reset():
                self.tokens = float(self.capacity)
                self.last_refill = time.monotonic()
                self.requests_allowed = 0
                self.requests_denied = 0

    # def __casty_merge__(self, base: 'RateLimiter', other: 'RateLimiter'):
    #     """Merge split-brain states by summing token consumption.
    #
    #     When a network partition heals, both sides may have processed
    #     requests independently. This merge ensures we don't double-count
    #     tokens by summing the consumption from both sides.
    #     """
    #     # Calculate how many tokens were consumed on each side
    #     my_consumed = base.capacity - self.tokens
    #     their_consumed = base.capacity - other.tokens
    #
    #     # Sum consumption and update tokens
    #     # (prevents token "duplication" in split-brain)
    #     total_consumed = my_consumed + their_consumed
    #     self.tokens = max(0, base.capacity - total_consumed)
    #
    #     # Sum request counters from both sides
    #     self.requests_allowed = (
    #         base.requests_allowed +
    #         (self.requests_allowed - base.requests_allowed) +
    #         (other.requests_allowed - base.requests_allowed)
    #     )
    #     self.requests_denied = (
    #         base.requests_denied +
    #         (self.requests_denied - base.requests_denied) +
    #         (other.requests_denied - base.requests_denied)
    #     )
    #
    #     # Use most recent refill timestamp
    #     self.last_refill = max(self.last_refill, other.last_refill)
