"""Failover reactivation: a replicated actor with `idle_timeout=float("inf")`
never stops on its own. Once a first call activates it, the cluster keeps it
active somewhere — its node leaving (or dying) just moves the activation to
the new owner of the key, which reactivates it *without waiting for a call*.

Deactivation stays deliberate: only `ctx.deactivate()` or a STOP directive
ends the activity for good.

Run: uv run python examples/09_reactivation.py
"""

import asyncio

import casty


@casty.actor(replicas=3, idle_timeout=float("inf"))
class Sentinel:
    activations: int = 0

    @casty.activate
    async def _up(self) -> None:
        self.activations += 1
        print(f"  sentinel {casty.context().key!r} is up (activation #{self.activations})")

    async def read(self) -> int:
        return self.activations

    async def retire(self) -> None:
        casty.context().deactivate()


async def main() -> None:
    nodes = [
        await casty.start("127.0.0.1:7131"),
        await casty.start("127.0.0.1:7132", seeds=["127.0.0.1:7131"]),
        await casty.start("127.0.0.1:7133", seeds=["127.0.0.1:7131"]),
    ]
    while any(len(n.membership.alive_members()) < 3 for n in nodes):
        await asyncio.sleep(0.05)

    print("first call activates the sentinel:")
    await nodes[0].actor(Sentinel, "watchdog").read()

    ring = nodes[0]._ring  # examples-only peek: which node owns the key right now
    assert ring is not None
    owner_id = ring.owner(f"{Sentinel.__module__}.Sentinel/watchdog")
    owner = next(n for n in nodes if n.node_id == owner_id)
    nodes.remove(owner)

    print("owner leaves; the new owner reactivates it, no call sent:")
    await owner.close()
    await asyncio.sleep(1.0)

    print(f"activations seen by the state: {await nodes[0].actor(Sentinel, 'watchdog').read()}")

    print("retire() is the deliberate end — the cluster stops resurrecting it:")
    await nodes[0].actor(Sentinel, "watchdog").retire()
    await asyncio.sleep(1.0)

    for node in nodes:
        await node.close()


if __name__ == "__main__":
    asyncio.run(main())
