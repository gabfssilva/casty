"""Inspection: spy subscriptions, the inline interceptor and state_of (spec 11).

Observation is passive and best-effort: events are born on the node that runs
the handler, matched there against the subscription's globs, and every buffer
in between drops on overflow (surfacing the loss in-band as `Lag`) — a spy can
never slow an actor down. The interceptor is the opposite trade: called inline
in the worker, guaranteed and ordered, so it must return fast and never block.

Run: uv run python examples/11_inspection.py
"""

import asyncio

import casty
from casty import inspection


@casty.actor
class Order:
    total: float = 0.0

    async def add(self, amount: float) -> float:
        self.total += amount
        return self.total


def metrics(event: inspection.SpyEvent) -> None:
    match event:
        case inspection.Completed(actor=actor, method=method, duration=duration):
            print(f"  [interceptor] {actor}.{method} took {duration * 1000:.2f}ms")
        case _:
            pass


async def main() -> None:
    node = await casty.start("127.0.0.1:7101", interceptor=metrics)
    peer = await casty.start("127.0.0.1:7102", seeds=["127.0.0.1:7101"])
    while not all(len(n.membership.alive_members()) == 2 for n in (node, peer)):
        await asyncio.sleep(0.05)  # let the view converge so the ring is stable
    try:
        # cluster scope: dials every member, so it sees keys owned anywhere
        async with node.spy(Order, key="order-*", scope="cluster", payloads=True) as events:
            # subscribing has no join-time sync (spec 11 §7): give the dials a
            # moment to land before emitting, or early events are simply missed
            await asyncio.sleep(0.5)
            for i in range(3):
                await node.actor(Order, f"order-{i}").add(10.0 * (i + 1))

            seen = 0
            async for event in events:
                match event:
                    case inspection.Received(key=key, method=method) as received:
                        print(f"{key}: {method}{tuple(inspection.decode(received))}")
                    case inspection.Completed(key=key) as completed:
                        print(f"{key}: -> {inspection.decode(completed)}")
                        seen += 1
                    case inspection.Lag(dropped=dropped):
                        print(f"(lost {dropped} events)")
                    case _:
                        pass
                if seen == 3:
                    break

        # a point-in-time read of the live state, without touching the mailbox
        print("state_of order-0:", await node.state_of(Order, "order-0"))
        print("state_of order-9:", await node.state_of(Order, "order-9"))  # never activated
    finally:
        await peer.close()
        await node.close()


asyncio.run(main())
