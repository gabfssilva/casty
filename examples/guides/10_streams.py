"""Reactive Streams — demand-based backpressure between actors.

A ``stream_producer`` buffers elements pushed via ``SinkRef`` until a
``stream_consumer`` subscribes and requests demand.  The consumer
exposes the stream as an ``async for`` iterator.  Demand is
replenished automatically as elements are consumed.

    caller ──await sink.put(elem)──▶ stream_producer ──StreamElement──▶ queue
                                           ▲                              │
                                           └──── StreamDemand(n) ◀── stream_consumer ◀── async for
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty import ActorSystem
from casty.core.streams import (
    GetSink,
    GetSource,
    SinkRef,
    SourceRef,
    stream_consumer,
    stream_producer,
)


@dataclass(frozen=True)
class PriceUpdate:
    symbol: str
    price: float


async def main() -> None:
    async with ActorSystem() as system:
        producer = system.spawn(stream_producer(buffer_size=16), "price-producer")
        consumer = system.spawn(
            stream_consumer(producer, initial_demand=2), "price-consumer"
        )
        await asyncio.sleep(0.05)

        sink: SinkRef[PriceUpdate] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=5.0
        )

        source: SourceRef[PriceUpdate] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=5.0
        )

        prices = (142.50, 143.10, 141.80, 144.20, 145.00)

        async def consume() -> int:
            count = 0
            async for update in source:
                count += 1
                print(f"  #{count} {update.symbol}: ${update.price:.2f}")
            return count

        consume_task = asyncio.create_task(consume())

        print("── Pushing 5 prices into producer ──")
        print("── Consuming via async for (initial_demand=2) ──")
        for price in prices:
            await sink.put(PriceUpdate(symbol="AAPL", price=price))
        await sink.complete()

        count = await consume_task
        print(f"\n── Stream complete: {count} prices received ──")

        print("\n── Early break: take 5 from 20 ──")

        producer2 = system.spawn(stream_producer(buffer_size=32), "num-producer")
        consumer2 = system.spawn(
            stream_consumer(producer2, initial_demand=4), "num-consumer"
        )
        await asyncio.sleep(0.05)

        sink2: SinkRef[int] = await system.ask(
            producer2, lambda r: GetSink(reply_to=r), timeout=5.0
        )

        source2: SourceRef[int] = await system.ask(
            consumer2, lambda r: GetSource(reply_to=r), timeout=5.0
        )

        taken: list[int] = []

        async def take_five() -> None:
            async for value in source2:
                taken.append(value)
                if len(taken) >= 5:
                    break

        take_task = asyncio.create_task(take_five())

        for i in range(20):
            await sink2.put(i)

        await take_task

        print(f"  Took: {taken}")
        print("  Stream cancelled — producer stopped via StreamCancel")


asyncio.run(main())
