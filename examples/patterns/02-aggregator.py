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

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class GetPrice:
    item: str


@dataclass
class PriceQuote:
    supplier: str
    item: str
    price: float


@dataclass
class FindBestPrice:
    item: str
    timeout: float = 1.0


@dataclass
class BestPriceResult:
    item: str
    best_price: float | None
    best_supplier: str | None
    quotes_received: int
    total_suppliers: int


@dataclass
class _SupplierResponse:
    quote: PriceQuote


@dataclass
class _Timeout:
    request_id: int


@actor
async def supplier(supplier_name: str, base_price: float, delay: float, *, mailbox: Mailbox[GetPrice]):
    async for msg, ctx in mailbox:
        match msg:
            case GetPrice(item):
                await asyncio.sleep(delay)
                price = base_price + random.uniform(-10, 10)
                price = round(price, 2)
                print(f"  [{supplier_name}] Quote for '{item}': ${price:.2f}")
                await ctx.reply(PriceQuote(supplier_name, item, price))


@actor
async def aggregator(suppliers: list[LocalActorRef[GetPrice]], *, mailbox: Mailbox[FindBestPrice | _SupplierResponse | _Timeout]):
    pending_requests: dict[int, dict] = {}
    request_counter = 0

    async for msg, ctx in mailbox:
        match msg:
            case FindBestPrice(item, timeout):
                request_counter += 1
                request_id = request_counter

                pending_requests[request_id] = {
                    "item": item,
                    "quotes": [],
                    "reply_to": ctx.reply_to,
                    "total": len(suppliers),
                }

                print(f"[Aggregator] Requesting quotes for '{item}' from {len(suppliers)} suppliers")

                await ctx.schedule(_Timeout(request_id), delay=timeout)

                for s in suppliers:
                    asyncio.create_task(_fetch_quote(s, item, request_id, pending_requests, ctx._self_ref))

            case _SupplierResponse(quote):
                for req_id, request in list(pending_requests.items()):
                    if request["item"] == quote.item:
                        request["quotes"].append(quote)
                        if len(request["quotes"]) == request["total"]:
                            await _complete_request(req_id, pending_requests)
                        break

            case _Timeout(request_id):
                if request_id in pending_requests:
                    print(f"[Aggregator] Timeout reached, completing with partial results")
                    await _complete_request(request_id, pending_requests)


async def _fetch_quote(s, item, request_id, pending_requests, self_ref):
    try:
        quote = await s.ask(GetPrice(item), timeout=5.0)
        await self_ref.send(_SupplierResponse(quote))
    except asyncio.TimeoutError:
        pass


async def _complete_request(request_id: int, pending_requests: dict) -> None:
    request = pending_requests.pop(request_id, None)
    if not request:
        return

    quotes: list[PriceQuote] = request["quotes"]
    reply_to = request["reply_to"]

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

    if reply_to and not reply_to.done():
        reply_to.set_result(result)


async def main():
    print("=" * 60)
    print("Aggregator Pattern Example (Scatter-Gather)")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        supplier1 = await system.actor(supplier(supplier_name="FastMart", base_price=100.0, delay=0.1), name="supplier-fastmart")
        supplier2 = await system.actor(supplier(supplier_name="BudgetStore", base_price=85.0, delay=0.2), name="supplier-budgetstore")
        supplier3 = await system.actor(supplier(supplier_name="SlowShop", base_price=80.0, delay=2.0), name="supplier-slowshop")
        supplier4 = await system.actor(supplier(supplier_name="QuickDeal", base_price=95.0, delay=0.15), name="supplier-quickdeal")

        suppliers_list = [supplier1, supplier2, supplier3, supplier4]

        agg = await system.actor(aggregator(suppliers=suppliers_list), name="aggregator")

        print("[Main] Requesting best price for 'laptop' (1s timeout)...")
        print("       Note: SlowShop (2s delay) will likely timeout")
        print()

        result: BestPriceResult = await agg.ask(
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
