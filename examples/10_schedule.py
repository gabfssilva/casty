"""Schedules: deferred and periodic self-calls (spec 04 §10).

- `ctx.schedule(fn, every=...)` delivers ticks to the actor's own mailbox, so
  a tick runs like any handler: one at a time, state committed if it mutated.
- `after=` alone is a one-shot; fixed-delay means the next tick only arms
  after the previous one finished — a slow handler never piles ticks up.
- Schedules die with the activation and never keep an actor alive. To survive
  failover, store the decision as state and re-arm in the activate hook
  (see examples/09_reactivation.py for the eligibility rule).

Run: uv run python examples/10_schedule.py
"""

import asyncio

import casty


@casty.actor(idle_timeout=float("inf"))
class Monitor:
    checks: int = 0
    interval: float | None = None  # durable decision; the schedule is its transient effect

    @casty.activate
    async def _up(self) -> None:
        if self.interval is not None:  # re-arm after a failover reactivation
            casty.context().schedule(self._check, every=self.interval)

    async def watch(self, every: float) -> None:
        self.interval = every
        casty.context().schedule(self._check, every=every)
        casty.context().schedule(self._remind, "first check-in", after=0.5, name="hello")

    async def status(self) -> str:
        return f"{casty.context().key}: {self.checks} checks"

    async def stop_watching(self) -> None:
        self.interval = None
        casty.context().deactivate()  # schedules die with the activation

    async def _check(self) -> None:
        self.checks += 1
        print(f"  [tick]     {casty.context().key}: check #{self.checks}")

    async def _remind(self, note: str) -> None:  # one-shot, with an argument
        print(f"  [one-shot] {casty.context().key}: {note}")


async def main() -> None:
    async with casty.start("127.0.0.1:7110") as system:
        monitor = system.actor(Monitor, "web-1")
        await monitor.watch(every=0.3)

        await asyncio.sleep(1.2)
        print(await monitor.status())

        await monitor.stop_watching()
        await asyncio.sleep(0.5)  # silence: no orphan ticks after deactivation
        print(await monitor.status())  # fresh activation, nothing scheduled


if __name__ == "__main__":
    asyncio.run(main())
