"""Supervision: what happens to an actor's state when a handler raises.

The supervisor is a plain function `(cls, key, exc, ctx) -> Directive`,
global on `casty.start` and overridable per class:

- KEEP  (default): state survives; the caller gets ActorFailedError.
- RESET: the activation is discarded; next call starts fresh.
- STOP:  graceful deactivation (deactivate hook runs); next call reactivates.

Infrastructure failures (timeouts, unreachable owner, no quorum) are NOT the
supervisor's business — they surface as typed exceptions on the caller.

Run: uv run python examples/04_supervision.py
"""

import asyncio
import contextlib

import casty


@casty.actor
class Register:
    total: int = 0

    async def add(self, amount: int) -> int:
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        self.total += amount
        return self.total


def three_strikes(
    cls: type, key: str, exc: Exception, ctx: casty.FailureContext
) -> casty.Directive:
    """Keep the state on isolated failures; discard it after three in a row."""
    print(f"  [supervisor] {cls.__name__}/{key} failed ({exc}); strike {ctx.failures}")
    return casty.KEEP if ctx.failures < 3 else casty.RESET


async def main() -> None:
    node = await casty.start("127.0.0.1:7121", supervisor=three_strikes)

    register = node.actor(Register, "drawer-1")
    print(f"total: {await register.add(10)}")

    for _ in range(2):
        try:
            await register.add(-1)
        except casty.ActorFailedError as exc:
            print(f"caller saw: {exc}")
    print(f"total after 2 strikes (KEEP): {await register.add(5)}")  # 15: state kept

    # the successful add above reset the strike count; fail 3 times in a row
    for _ in range(3):
        with contextlib.suppress(casty.ActorFailedError):
            await register.add(-1)
    print(f"total after 3rd strike (RESET): {await register.add(1)}")  # 1: state gone

    await node.close()


if __name__ == "__main__":
    asyncio.run(main())
