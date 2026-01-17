"""Circuit Breaker pattern: Fault tolerance for unreliable services.

Demonstrates:
- States: closed (normal) -> open (failing) -> half-open (testing)
- Counting consecutive failures
- Open state rejects requests immediately
- Using ctx.schedule() for half-open transition
- Reset on successful call

Run with:
    uv run python examples/patterns/03-circuit-breaker.py
"""

import asyncio
import random
from dataclasses import dataclass
from enum import Enum

from casty import Actor, ActorSystem, Context


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class Call:
    """Request to call the protected service."""
    request_id: int


@dataclass
class CallResult:
    """Result of a call attempt."""
    request_id: int
    success: bool
    message: str


@dataclass
class GetStatus:
    """Get circuit breaker status."""
    pass


@dataclass
class CircuitStatus:
    """Status response."""
    state: str
    failure_count: int
    success_count: int
    total_calls: int
    rejected_calls: int


@dataclass
class _TryHalfOpen:
    """Internal message to transition to half-open state."""
    pass


class CircuitBreaker(Actor[Call | GetStatus | _TryHalfOpen]):
    """Circuit breaker that protects calls to an unreliable service.

    Configuration:
    - failure_threshold: Number of consecutive failures before opening
    - reset_timeout: Seconds before transitioning from open to half-open
    - success_threshold: Successes needed in half-open to close
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        reset_timeout: float = 5.0,
        success_threshold: int = 2,
        failure_rate: float = 0.4,
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.success_threshold = success_threshold
        self.failure_rate = failure_rate

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_successes = 0
        self.total_calls = 0
        self.rejected_calls = 0

    async def receive(self, msg: Call | GetStatus | _TryHalfOpen, ctx: Context) -> None:
        match msg:
            case Call(request_id):
                self.total_calls += 1
                await self._handle_call(request_id, ctx)

            case GetStatus():
                await ctx.reply(CircuitStatus(
                    state=self.state.value,
                    failure_count=self.failure_count,
                    success_count=self.success_count,
                    total_calls=self.total_calls,
                    rejected_calls=self.rejected_calls,
                ))

            case _TryHalfOpen():
                if self.state == CircuitState.OPEN:
                    print(f"[Circuit] Transitioning from OPEN -> HALF_OPEN (testing)")
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_successes = 0

    async def _handle_call(self, request_id: int, ctx: Context) -> None:
        match self.state:
            case CircuitState.OPEN:
                self.rejected_calls += 1
                print(f"[Circuit] Call #{request_id} REJECTED (circuit OPEN)")
                await ctx.reply(CallResult(
                    request_id=request_id,
                    success=False,
                    message="Circuit breaker is OPEN - request rejected",
                ))

            case CircuitState.HALF_OPEN:
                success = await self._call_service(request_id)

                if success:
                    self.half_open_successes += 1
                    self.success_count += 1
                    print(f"[Circuit] Call #{request_id} SUCCESS in HALF_OPEN ({self.half_open_successes}/{self.success_threshold})")

                    if self.half_open_successes >= self.success_threshold:
                        print(f"[Circuit] Transitioning from HALF_OPEN -> CLOSED (service recovered)")
                        self.state = CircuitState.CLOSED
                        self.failure_count = 0

                    await ctx.reply(CallResult(request_id, True, "Success"))
                else:
                    print(f"[Circuit] Call #{request_id} FAILED in HALF_OPEN -> back to OPEN")
                    self.failure_count += 1
                    self.state = CircuitState.OPEN
                    await ctx.schedule(self.reset_timeout, _TryHalfOpen())
                    await ctx.reply(CallResult(request_id, False, "Failed in half-open"))

            case CircuitState.CLOSED:
                success = await self._call_service(request_id)

                if success:
                    self.success_count += 1
                    self.failure_count = 0
                    print(f"[Circuit] Call #{request_id} SUCCESS (circuit CLOSED)")
                    await ctx.reply(CallResult(request_id, True, "Success"))
                else:
                    self.failure_count += 1
                    print(f"[Circuit] Call #{request_id} FAILED ({self.failure_count}/{self.failure_threshold})")

                    if self.failure_count >= self.failure_threshold:
                        print(f"[Circuit] Transitioning from CLOSED -> OPEN (too many failures)")
                        self.state = CircuitState.OPEN
                        await ctx.schedule(self.reset_timeout, _TryHalfOpen())

                    await ctx.reply(CallResult(request_id, False, "Service call failed"))

    async def _call_service(self, request_id: int) -> bool:
        """Simulate calling an unreliable external service."""
        await asyncio.sleep(0.05)
        return random.random() > self.failure_rate


async def main():
    print("=" * 60)
    print("Circuit Breaker Pattern Example")
    print("=" * 60)
    print()
    print("Configuration:")
    print("  - Failure threshold: 3 (opens after 3 consecutive failures)")
    print("  - Reset timeout: 2s (time before trying half-open)")
    print("  - Success threshold: 2 (successes needed to close)")
    print("  - Failure rate: 70% (simulated unreliable service)")
    print()

    async with ActorSystem() as system:
        circuit = await system.spawn(
            CircuitBreaker,
            failure_threshold=3,
            reset_timeout=2.0,
            success_threshold=2,
            failure_rate=0.7,
        )

        print("Phase 1: Making calls until circuit opens...")
        print("-" * 40)

        for i in range(10):
            result: CallResult = await circuit.ask(Call(request_id=i))
            await asyncio.sleep(0.1)

        status: CircuitStatus = await circuit.ask(GetStatus())
        print()
        print(f"Status after Phase 1: {status.state}")
        print()

        if status.state == "open":
            print("Phase 2: Circuit is OPEN, waiting for reset timeout...")
            print("-" * 40)

            for i in range(10, 13):
                result = await circuit.ask(Call(request_id=i))
                await asyncio.sleep(0.1)

            print()
            print(f"Waiting {2.5}s for half-open transition...")
            await asyncio.sleep(2.5)

            print()
            print("Phase 3: Circuit should be HALF_OPEN, testing...")
            print("-" * 40)

            for i in range(13, 20):
                result = await circuit.ask(Call(request_id=i))
                await asyncio.sleep(0.2)

        final_status: CircuitStatus = await circuit.ask(GetStatus())
        print()
        print("=" * 60)
        print("Final Statistics:")
        print(f"  State: {final_status.state}")
        print(f"  Total calls: {final_status.total_calls}")
        print(f"  Successful: {final_status.success_count}")
        print(f"  Rejected (circuit open): {final_status.rejected_calls}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
