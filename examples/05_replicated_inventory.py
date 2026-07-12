"""Replication: state that survives losing a node.

`replicas=3, write=MAJORITY` means every mutation is committed on 2 of 3
nodes before the caller gets an answer, with an HLC per key. When a node
goes away, the next activation handshakes with a quorum of replicas and
adopts the newest committed snapshot — nothing is lost.

`casty.Map` is the same machinery as sugar: a distributed map partitioned
into shard actors, each replicated like any other actor.

Run: uv run python examples/05_replicated_inventory.py
"""

import asyncio

import casty


@casty.message
class Reservation:
    order_id: str
    qty: int


@casty.actor(replicas=3, write=casty.MAJORITY)
class StockItem:
    on_hand: int = 0
    reservations: list[Reservation] = casty.transient(factory=list)  # rebuilt, not replicated
    committed: int = 0

    async def restock(self, qty: int) -> int:
        self.on_hand += qty
        return self.on_hand

    async def reserve(self, order_id: str, qty: int) -> bool:
        if qty > self.on_hand - self.committed:
            return False
        self.committed += qty
        self.reservations.append(Reservation(order_id=order_id, qty=qty))
        return True

    async def available(self) -> int:
        return self.on_hand - self.committed


async def main() -> None:
    systems = [await casty.start("127.0.0.1:7131")]
    for port in (7132, 7133):
        systems.append(await casty.start(f"127.0.0.1:{port}", seeds=["127.0.0.1:7131"]))
    while any(len(s.membership.alive_members()) < 3 for s in systems):
        await asyncio.sleep(0.05)
    print("cluster up: 3 members")

    sku = systems[0].actor(StockItem, "sku-1234")
    await sku.restock(100)
    assert await sku.reserve("order-1", 30)
    assert not await sku.reserve("order-2", 90)  # only 70 available
    print(f"available before failure: {await sku.available()}")

    # take a node down; its ranges hand off and replicas keep the state
    leaving, *rest = systems
    await leaving.close()
    print("node 1 stopped (graceful: drain + handoff + leave)")

    survivor_view = rest[0].actor(StockItem, "sku-1234")
    print(f"available after failure:  {await survivor_view.available()}")
    assert await survivor_view.available() == 70

    # the distributed map rides the same replication
    prices: casty.Map[str, float] = rest[0].map("prices", replicas=3, write=casty.MAJORITY)
    await prices.put("sku-1234", 49.90)
    await prices.put("sku-9999", 12.50)
    print(f"price of sku-1234: {await prices.get('sku-1234')}")
    print(f"catalog size: {await prices.size()}")

    for system in rest:
        await system.close()


if __name__ == "__main__":
    asyncio.run(main())
