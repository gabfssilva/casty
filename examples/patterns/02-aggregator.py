"""Aggregator pattern: Scatter-gather for collecting responses from multiple actors.

Demonstrates:
- Fan-out requests to multiple actors (price suppliers)
- Collecting responses with a timeout
- Returning aggregated result (lowest price)
- Handling partial responses gracefully

Run with:
    uv run python examples/patterns/02-aggregator.py
"""

import asyncio
import random
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context, LocalActorRef


@dataclass
class GetPrice:
    """Request a price quote for an item."""
    item: str


@dataclass
class PriceQuote:
    """Price quote response from a supplier."""
    supplier: str
    item: str
    price: float


@dataclass
class FindBestPrice:
    """Request to find the best price across all suppliers."""
    item: str
    timeout: float = 1.0


@dataclass
class BestPriceResult:
    """Result of the aggregation."""
    item: str
    best_price: float | None
    best_supplier: str | None
    quotes_received: int
    total_suppliers: int


class Supplier(Actor[GetPrice]):
    """Simulates a price supplier with variable response time."""

    def __init__(self, supplier_name: str, base_price: float, delay: float):
        self.supplier_name = supplier_name
        self.base_price = base_price
        self.delay = delay

    async def receive(self, msg: GetPrice, ctx: Context) -> None:
        await asyncio.sleep(self.delay)

        price = self.base_price + random.uniform(-10, 10)
        price = round(price, 2)

        print(f"  [{self.supplier_name}] Quote for '{msg.item}': ${price:.2f}")
        await ctx.reply(PriceQuote(self.supplier_name, msg.item, price))


@dataclass
class _SupplierResponse:
    """Internal message for collecting supplier responses."""
    quote: PriceQuote


@dataclass
class _Timeout:
    """Internal message for aggregation timeout."""
    request_id: int


class Aggregator(Actor[FindBestPrice | _SupplierResponse | _Timeout]):
    """Aggregates price quotes from multiple suppliers."""

    def __init__(self, suppliers: list[LocalActorRef[GetPrice]]):
        self.suppliers = suppliers
        self.pending_requests: dict[int, dict] = {}
        self.request_counter = 0

    async def receive(
        self,
        msg: FindBestPrice | _SupplierResponse | _Timeout,
        ctx: Context,
    ) -> None:
        match msg:
            case FindBestPrice(item, timeout):
                self.request_counter += 1
                request_id = self.request_counter

                self.pending_requests[request_id] = {
                    "item": item,
                    "quotes": [],
                    "sender": ctx.sender,
                    "total": len(self.suppliers),
                }

                print(f"[Aggregator] Requesting quotes for '{item}' from {len(self.suppliers)} suppliers")

                await ctx.schedule(timeout, _Timeout(request_id))

                for supplier in self.suppliers:
                    ctx.detach(self._request_quote(supplier, item)).then(
                        lambda q: _SupplierResponse(q)
                    )

            case _SupplierResponse(quote):
                for req_id, request in list(self.pending_requests.items()):
                    if request["item"] == quote.item:
                        request["quotes"].append(quote)

                        if len(request["quotes"]) == request["total"]:
                            await self._complete_request(req_id)
                        break

            case _Timeout(request_id):
                if request_id in self.pending_requests:
                    print(f"[Aggregator] Timeout reached, completing with partial results")
                    await self._complete_request(request_id)

    async def _request_quote(
        self,
        supplier: LocalActorRef[GetPrice],
        item: str,
    ) -> PriceQuote:
        """Request a quote from a single supplier."""
        return await supplier.ask(GetPrice(item), timeout=5.0)

    async def _complete_request(self, request_id: int) -> None:
        """Complete a request and send the aggregated result."""
        request = self.pending_requests.pop(request_id, None)
        if not request:
            return

        quotes: list[PriceQuote] = request["quotes"]
        sender = request["sender"]

        if quotes:
            best = min(quotes, key=lambda q: q.price)
            result = BestPriceResult(
                item=request["item"],
                best_price=best.price,
                best_supplier=best.supplier,
                quotes_received=len(quotes),
                total_suppliers=request["total"],
            )
        else:
            result = BestPriceResult(
                item=request["item"],
                best_price=None,
                best_supplier=None,
                quotes_received=0,
                total_suppliers=request["total"],
            )

        print(f"[Aggregator] Aggregation complete: {len(quotes)}/{request['total']} quotes received")

        if sender:
            await sender.send(result)


async def main():
    print("=" * 60)
    print("Aggregator Pattern Example (Scatter-Gather)")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        supplier1 = await system.actor(Supplier, name="supplier-fastmart", supplier_name="FastMart", base_price=100.0, delay=0.1)
        supplier2 = await system.actor(Supplier, name="supplier-budgetstore", supplier_name="BudgetStore", base_price=85.0, delay=0.2)
        supplier3 = await system.actor(Supplier, name="supplier-slowshop", supplier_name="SlowShop", base_price=80.0, delay=2.0)
        supplier4 = await system.actor(Supplier, name="supplier-quickdeal", supplier_name="QuickDeal", base_price=95.0, delay=0.15)

        suppliers = [supplier1, supplier2, supplier3, supplier4]

        aggregator = await system.actor(Aggregator, name="aggregator", suppliers=suppliers)

        print("[Main] Requesting best price for 'laptop' (1s timeout)...")
        print("       Note: SlowShop (2s delay) will likely timeout")
        print()

        result: BestPriceResult = await aggregator.ask(
            FindBestPrice(item="laptop", timeout=1.0),
            timeout=5.0,
        )

        print()
        print("=" * 60)
        print("Result:")
        print(f"  Item: {result.item}")
        print(f"  Best price: ${result.best_price:.2f}" if result.best_price else "  Best price: N/A")
        print(f"  Best supplier: {result.best_supplier}" if result.best_supplier else "  Best supplier: N/A")
        print(f"  Quotes received: {result.quotes_received}/{result.total_suppliers}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
