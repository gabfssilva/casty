"""Stateful companion — immutable utilities via fold.

A service caller uses a **circuit breaker companion** to protect against
cascading failures.  The companion is a frozen dataclass whose methods
return ``tuple[result, new_self]`` — a constant fold.  The actor carries
it as part of its behavior-recursion state, so no mutation ever happens.

    caller(breaker)
        │
        ├── Call ──▶ breaker.allow?() ──▶ success/failure ──▶ breaker.record() ──▶ caller(new_breaker)
        │
        └── GetStatus ──▶ breaker.status() ──▶ Behaviors.same()

Circuit breaker states:

    closed ──(failures >= threshold)──▶ open ──(timeout expires)──▶ half_open
       ▲                                                              │
       └────────────────(success)─────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


class CircuitState(Enum):
    closed = "closed"
    open = "open"
    half_open = "half_open"


@dataclass(frozen=True)
class CircuitBreaker:
    failure_threshold: int
    reset_timeout: float
    state: CircuitState = CircuitState.closed
    failure_count: int = 0
    last_failure_time: float = 0.0

    def allow(self, now: float) -> tuple[bool, CircuitBreaker]:
        match self.state:
            case CircuitState.closed:
                return True, self

            case CircuitState.open if now - self.last_failure_time >= self.reset_timeout:
                new = CircuitBreaker(
                    failure_threshold=self.failure_threshold,
                    reset_timeout=self.reset_timeout,
                    state=CircuitState.half_open,
                    failure_count=self.failure_count,
                    last_failure_time=self.last_failure_time,
                )
                return True, new

            case CircuitState.open:
                return False, self

            case CircuitState.half_open:
                return True, self

    def record_success(self) -> tuple[None, CircuitBreaker]:
        return None, CircuitBreaker(
            failure_threshold=self.failure_threshold,
            reset_timeout=self.reset_timeout,
            state=CircuitState.closed,
            failure_count=0,
            last_failure_time=0.0,
        )

    def record_failure(self, now: float) -> tuple[None, CircuitBreaker]:
        new_count = self.failure_count + 1
        new_state = (
            CircuitState.open if new_count >= self.failure_threshold else self.state
        )
        return None, CircuitBreaker(
            failure_threshold=self.failure_threshold,
            reset_timeout=self.reset_timeout,
            state=new_state,
            failure_count=new_count,
            last_failure_time=now,
        )

    def status(self) -> tuple[CircuitState, CircuitBreaker]:
        return self.state, self


@dataclass(frozen=True)
class CallResult:
    success: bool
    message: str


@dataclass(frozen=True)
class Call:
    should_fail: bool
    reply_to: ActorRef[CallResult]


@dataclass(frozen=True)
class GetStatus:
    reply_to: ActorRef[CircuitState]


type CallerMsg = Call | GetStatus


def service_caller(breaker: CircuitBreaker) -> Behavior[CallerMsg]:
    async def receive(
        _ctx: ActorContext[CallerMsg], msg: CallerMsg
    ) -> Behavior[CallerMsg]:
        match msg:
            case Call(should_fail=should_fail, reply_to=reply_to):
                now = time.monotonic()
                allowed, new_breaker = breaker.allow(now)

                if not allowed:
                    reply_to.tell(CallResult(success=False, message="circuit open"))
                    return service_caller(new_breaker)

                if should_fail:
                    _, new_breaker = new_breaker.record_failure(now)
                    reply_to.tell(CallResult(success=False, message="service error"))
                else:
                    _, new_breaker = new_breaker.record_success()
                    reply_to.tell(CallResult(success=True, message="ok"))

                return service_caller(new_breaker)

            case GetStatus(reply_to=reply_to):
                state, _ = breaker.status()
                reply_to.tell(state)
                return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(
            service_caller(
                CircuitBreaker(failure_threshold=3, reset_timeout=1.0)
            ),
            "caller",
        )

        print("── Sending 3 failures to trip the breaker ──")
        for i in range(3):
            result: CallResult = await system.ask(
                ref, lambda r: Call(should_fail=True, reply_to=r), timeout=5.0
            )
            print(f"  Call {i + 1}: {result.message}")

        status: CircuitState = await system.ask(
            ref, lambda r: GetStatus(reply_to=r), timeout=5.0
        )
        print(f"  Circuit state: {status.value}")

        print("\n── Calling while circuit is open ──")
        result = await system.ask(
            ref, lambda r: Call(should_fail=False, reply_to=r), timeout=5.0
        )
        print(f"  Call: {result.message}")

        print("\n── Waiting for reset timeout (1s) ──")
        await asyncio.sleep(1.1)

        print("\n── Calling after timeout (half-open) ──")
        result = await system.ask(
            ref, lambda r: Call(should_fail=False, reply_to=r), timeout=5.0
        )
        print(f"  Call: {result.message}")

        status = await system.ask(
            ref, lambda r: GetStatus(reply_to=r), timeout=5.0
        )
        print(f"  Circuit state: {status.value}")


asyncio.run(main())
