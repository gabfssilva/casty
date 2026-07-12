"""Actor lifecycle: transient state, hooks, idle deactivation.

- `casty.transient()` marks fields that are NOT part of the actor's state
  snapshot (connections, caches); rebuild them in the activate hook.
- `@casty.activate` runs on every activation; `@casty.deactivate` on graceful
  deactivation (idle timeout, `ctx.deactivate()`, node shutdown).
- `casty.context()` gives the handler its own key, and `ctx.deactivate()`
  schedules deactivation after the current handler.

Run: uv run python examples/02_lifecycle.py
"""

import asyncio

import casty


@casty.actor(idle_timeout=1.0)
class Session:
    visits: int = 0
    cache: dict[str, str] = casty.transient(factory=dict)

    @casty.activate
    async def _open(self) -> None:
        self.cache = {"motd": "welcome back"}
        print(f"  [activate]   {casty.context().key}")

    @casty.deactivate
    async def _close(self) -> None:
        print(f"  [deactivate] {casty.context().key}")

    async def visit(self) -> str:
        self.visits += 1
        return f"{casty.context().key}: visit {self.visits}, motd={self.cache['motd']!r}"

    async def logout(self) -> None:
        casty.context().deactivate()


async def main() -> None:
    async with casty.start("127.0.0.1:7102") as node:
        alice = node.actor(Session, "alice")
        print(await alice.visit())
        print(await alice.visit())

        print("explicit logout:")
        await alice.logout()
        await asyncio.sleep(0.1)
        print(await alice.visit())  # fresh activation

        print("idle timeout (1s):")
        await asyncio.sleep(1.5)
        print(await alice.visit())  # reactivated again


if __name__ == "__main__":
    asyncio.run(main())
