"""Services: concurrent RPC over actors (spec 08).

An actor serializes: one handler at a time, and an `await` on a slow I/O holds the
worker for the whole wait. A `@casty.service` is a generated actor whose handler does
not wait for the work — it fires the method as a task and returns the mailbox — so N
calls progress together. It has no state: state is kept by the actor the method calls,
and calls from the same `(actor, key)` stay serialized there.

The service is the concurrent door; the actor, the serial guardian.

Run: uv run python examples/07_service.py
"""

import asyncio
import time

import casty


@casty.actor
class Inventory:
    stock: int = 10

    async def reserve(self, qty: int) -> bool:
        if qty > self.stock:
            return False
        self.stock -= qty  # serial per sku: never sells what it does not have
        return True

    async def left(self) -> int:
        return self.stock


@casty.service(concurrency=32)
class Checkout:

    async def buy(self, sku: str, qty: int) -> bool:
        context = casty.context()
        await asyncio.sleep(0.2)  # I/O: payment gateway
        return await context.actor(Inventory, sku).reserve(qty)


async def main() -> None:
    async with casty.local() as system:
        checkout = system.service(Checkout)

        started = time.perf_counter()
        results = await asyncio.gather(*[checkout.buy("sku-1", 1) for _ in range(12)])
        elapsed = time.perf_counter() - started

        sold = sum(results)
        print(f"{len(results)} orders in {elapsed:.2f}s (serial would be {0.2 * 12:.1f}s)")
        print(f"sold: {sold}, refused: {len(results) - sold}")
        print(f"stock: {await system.actor(Inventory, 'sku-1').left()}")


if __name__ == "__main__":
    asyncio.run(main())
