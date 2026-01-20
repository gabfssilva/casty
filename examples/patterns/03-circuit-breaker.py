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

from casty import actor, ActorSystem, Mailbox


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class Call:
    request_id: int


@dataclass
class CallResult:
    request_id: int
    success: bool
    message: str


@dataclass
class GetStatus:
    pass


@dataclass
class CircuitStatus:
    state: str
    failure_count: int
    success_count: int
    total_calls: int
    rejected_calls: int


@dataclass
class _TryHalfOpen:
    pass


@actor
async def circuit_breaker(
    failure_threshold: int = 3,
    reset_timeout: float = 5.0,
    success_threshold: int = 2,
    failure_rate: float = 0.4,
    *,
    mailbox: Mailbox[Call | GetStatus | _TryHalfOpen],
):
    state = CircuitState.CLOSED
    failure_count = 0
    success_count = 0
    half_open_successes = 0
    total_calls = 0
    rejected_calls = 0

    async def call_service(request_id: int) -> bool:
        await asyncio.sleep(0.05)
        return random.random() > failure_rate

    async for msg, ctx in mailbox:
        match msg:
            case Call(request_id):
                total_calls += 1

                if state == CircuitState.OPEN:
                    rejected_calls += 1
                    print(f"[Circuit] Call #{request_id} REJECTED (circuit OPEN)")
                    await ctx.reply(CallResult(
                        request_id=request_id,
                        success=False,
                        message="Circuit breaker is OPEN - request rejected",
                    ))

                elif state == CircuitState.HALF_OPEN:
                    success = await call_service(request_id)

                    if success:
                        half_open_successes += 1
                        success_count += 1
                        print(f"[Circuit] Call #{request_id} SUCCESS in HALF_OPEN ({half_open_successes}/{success_threshold})")

                        if half_open_successes >= success_threshold:
                            print(f"[Circuit] Transitioning from HALF_OPEN -> CLOSED (service recovered)")
                            state = CircuitState.CLOSED
                            failure_count = 0

                        await ctx.reply(CallResult(request_id, True, "Success"))
                    else:
                        print(f"[Circuit] Call #{request_id} FAILED in HALF_OPEN -> back to OPEN")
                        failure_count += 1
                        state = CircuitState.OPEN
                        await ctx.schedule(_TryHalfOpen(), delay=reset_timeout)
                        await ctx.reply(CallResult(request_id, False, "Failed in half-open"))

                else:  # CLOSED
                    success = await call_service(request_id)

                    if success:
                        success_count += 1
                        failure_count = 0
                        print(f"[Circuit] Call #{request_id} SUCCESS (circuit CLOSED)")
                        await ctx.reply(CallResult(request_id, True, "Success"))
                    else:
                        failure_count += 1
                        print(f"[Circuit] Call #{request_id} FAILED ({failure_count}/{failure_threshold})")

                        if failure_count >= failure_threshold:
                            print(f"[Circuit] Transitioning from CLOSED -> OPEN (too many failures)")
                            state = CircuitState.OPEN
                            await ctx.schedule(_TryHalfOpen(), delay=reset_timeout)

                        await ctx.reply(CallResult(request_id, False, "Service call failed"))

            case GetStatus():
                await ctx.reply(CircuitStatus(
                    state=state.value,
                    failure_count=failure_count,
                    success_count=success_count,
                    total_calls=total_calls,
                    rejected_calls=rejected_calls,
                ))

            case _TryHalfOpen():
                if state == CircuitState.OPEN:
                    print(f"[Circuit] Transitioning from OPEN -> HALF_OPEN (testing)")
                    state = CircuitState.HALF_OPEN
                    half_open_successes = 0


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
        circuit = await system.actor(
            circuit_breaker(
                failure_threshold=3,
                reset_timeout=2.0,
                success_threshold=2,
                failure_rate=0.7,
            ),
            name="circuit-breaker",
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
