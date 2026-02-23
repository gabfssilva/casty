from __future__ import annotations

import asyncio
from typing import Any

from casty import ActorSystem, Behaviors, DeadLetter, ServiceKey
from casty.cluster.system import ClusteredActorSystem
from casty.core.event_stream import Subscribe as ESSubscribe
from casty.core.ref import ActorRef, LocalActorRef
from casty.core.streams import (
    CompleteStream,
    GetSink,
    GetSource,
    Push,
    SinkRef,
    SourceRef,
    StreamCompleted,
    StreamDemand,
    StreamElement,
    StreamProducerMsg,
    Subscribe,
    stream_consumer,
    stream_producer,
)


async def test_sink_full_flow() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(buffer_size=16), "producer")
        consumer = system.spawn(stream_consumer(producer), "consumer")
        await asyncio.sleep(0.05)

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        results: list[int] = []

        async def consume() -> None:
            async for item in source:
                results.append(item)

        consume_task = asyncio.create_task(consume())

        await sink.put(1)
        await sink.put(2)
        await sink.put(3)
        await sink.complete()

        await asyncio.wait_for(consume_task, timeout=2.0)
        assert results == [1, 2, 3]


async def test_sink_backpressure() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(buffer_size=2), "producer")
        await asyncio.sleep(0.05)

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        await sink.put(10)
        await sink.put(20)

        put_done = asyncio.Event()

        async def blocked_put() -> None:
            await sink.put(30)
            put_done.set()

        task = asyncio.create_task(blocked_put())
        await asyncio.sleep(0.1)
        assert not put_done.is_set(), "put should block when buffer is full"

        consumer = system.spawn(
            stream_consumer(producer, initial_demand=16), "consumer"
        )
        await asyncio.sleep(0.05)

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        await asyncio.wait_for(task, timeout=2.0)
        assert put_done.is_set()

        await sink.complete()

        results: list[int] = []
        async for item in source:
            results.append(item)

        assert results == [10, 20, 30]


async def test_full_flow() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(stream_consumer(producer), "consumer")
        await asyncio.sleep(0.05)

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        results: list[int] = []

        async def consume() -> None:
            async for item in source:
                results.append(item)

        consume_task = asyncio.create_task(consume())

        await sink.put(1)
        await sink.put(2)
        await sink.put(3)
        await sink.complete()

        await asyncio.wait_for(consume_task, timeout=2.0)
        assert results == [1, 2, 3]


async def test_push_before_subscribe() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        await asyncio.sleep(0.05)

        sink: SinkRef[str] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        await sink.put("a")
        await sink.put("b")
        await sink.put("c")
        await asyncio.sleep(0.05)

        consumer = system.spawn(stream_consumer(producer), "consumer")
        await asyncio.sleep(0.05)

        await sink.complete()

        source: SourceRef[str] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        results: list[str] = []
        async for item in source:
            results.append(item)

        assert results == ["a", "b", "c"]


async def test_empty_completion() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(stream_consumer(producer), "consumer")
        await asyncio.sleep(0.05)

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )
        await sink.complete()

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        results: list[int] = []
        async for item in source:
            results.append(item)

        assert results == []


async def test_cancellation_on_break() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(
            stream_consumer(producer, initial_demand=16), "consumer"
        )
        await asyncio.sleep(0.05)

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        for i in range(10):
            await sink.put(i)

        results: list[int] = []
        async for item in source:
            results.append(item)
            if len(results) >= 3:
                break

        assert results == [0, 1, 2]


async def test_timeout_on_silence() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(stream_consumer(producer, timeout=0.2), "consumer")
        await asyncio.sleep(0.05)

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )
        await sink.put(42)

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        results: list[int] = []
        async for item in source:
            results.append(item)

        assert results == [42]


async def test_backpressure_demand_limits_inflight() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(stream_consumer(producer, initial_demand=2), "consumer")
        await asyncio.sleep(0.05)

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        sink: SinkRef[int] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        results: list[int] = []

        async def consume() -> None:
            async for item in source:
                results.append(item)

        consume_task = asyncio.create_task(consume())

        for i in range(10):
            await sink.put(i)
        await sink.complete()

        await asyncio.wait_for(consume_task, timeout=2.0)
        assert results == list(range(10))


async def test_backpressure_zero_demand_buffers() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        await asyncio.sleep(0.05)

        queue: asyncio.Queue[Any] = asyncio.Queue()
        subscriber: ActorRef[StreamElement[int] | StreamCompleted] = LocalActorRef(
            id="_test_sub", _deliver=queue.put_nowait
        )

        producer.tell(Subscribe(consumer=subscriber, demand=0))
        await asyncio.sleep(0.05)

        for i in range(5):
            producer.tell(Push(i))
        await asyncio.sleep(0.05)
        assert queue.empty()

        producer.tell(StreamDemand(n=2))
        await asyncio.sleep(0.05)
        assert queue.qsize() == 2

        producer.tell(StreamDemand(n=3))
        await asyncio.sleep(0.05)
        assert queue.qsize() == 5


async def test_completion_waits_for_buffer_drain() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        await asyncio.sleep(0.05)

        queue: asyncio.Queue[Any] = asyncio.Queue()
        subscriber: ActorRef[StreamElement[int] | StreamCompleted] = LocalActorRef(
            id="_test_sub", _deliver=queue.put_nowait
        )

        producer.tell(Subscribe(consumer=subscriber, demand=1))
        await asyncio.sleep(0.05)

        producer.tell(Push(10))
        producer.tell(Push(20))
        producer.tell(Push(30))
        producer.tell(CompleteStream())
        await asyncio.sleep(0.05)

        received: list[Any] = []
        while not queue.empty():
            received.append(queue.get_nowait())
        assert len(received) == 1
        assert received[0] == StreamElement(element=10)

        producer.tell(StreamDemand(n=2))
        await asyncio.sleep(0.05)

        while not queue.empty():
            received.append(queue.get_nowait())

        assert len(received) == 4
        assert received[1] == StreamElement(element=20)
        assert received[2] == StreamElement(element=30)
        assert received[3] == StreamCompleted()


async def test_multiple_pushes_preserve_order() -> None:
    async with ActorSystem("test") as system:
        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(stream_consumer(producer), "consumer")
        await asyncio.sleep(0.05)

        source: SourceRef[str] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        sink: SinkRef[str] = await system.ask(
            producer, lambda r: GetSink(reply_to=r), timeout=1.0
        )

        results: list[str] = []

        async def consume() -> None:
            async for item in source:
                results.append(item)

        consume_task = asyncio.create_task(consume())

        await sink.put("first")
        await sink.put("second")
        await sink.put("third")
        await sink.complete()

        await asyncio.wait_for(consume_task, timeout=2.0)
        assert results == ["first", "second", "third"]


async def test_cross_node_stream() -> None:
    async with ClusteredActorSystem(
        name="cluster", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            key: ServiceKey[StreamProducerMsg[int]] = ServiceKey(name="producer")
            producer_a = system_a.spawn(
                Behaviors.discoverable(stream_producer(), key=key), "producer"
            )
            await asyncio.sleep(2.0)

            sink: SinkRef[int] = await system_a.ask(
                producer_a, lambda r: GetSink(reply_to=r), timeout=5.0
            )

            listing = await system_b.lookup(key)
            assert len(listing.instances) >= 1
            remote_producer = next(iter(listing.instances)).ref

            consumer_b = system_b.spawn(
                stream_consumer(remote_producer, timeout=5.0), "consumer"
            )
            await asyncio.sleep(0.5)

            source: SourceRef[int] = await system_b.ask(
                consumer_b, lambda r: GetSource(reply_to=r), timeout=5.0
            )

            results: list[int] = []

            async def consume() -> None:
                async for item in source:
                    results.append(item)

            consume_task = asyncio.create_task(consume())

            await sink.put(1)
            await sink.put(2)
            await sink.put(3)
            await sink.complete()

            await asyncio.wait_for(consume_task, timeout=5.0)
            assert results == [1, 2, 3]


async def _run_stream_cycle(
    source_system: ClusteredActorSystem,
    target_system: ClusteredActorSystem,
    key_name: str,
    actor_suffix: str,
    data: list[int],
) -> list[int]:
    """Spawn a discoverable producer on *source_system*, look it up from
    *target_system*, stream *data* through, and return the consumed results."""
    key: ServiceKey[StreamProducerMsg[int]] = ServiceKey(name=key_name)
    producer = source_system.spawn(
        Behaviors.discoverable(stream_producer(), key=key),
        f"producer-{actor_suffix}",
    )
    await asyncio.sleep(2.0)

    sink: SinkRef[int] = await source_system.ask(
        producer, lambda r: GetSink(reply_to=r), timeout=5.0
    )

    listing = await target_system.lookup(key)
    assert len(listing.instances) >= 1
    remote_producer = next(iter(listing.instances)).ref

    consumer = target_system.spawn(
        stream_consumer(remote_producer, timeout=5.0),
        f"consumer-{actor_suffix}",
    )
    await asyncio.sleep(0.5)

    source: SourceRef[int] = await target_system.ask(
        consumer, lambda r: GetSource(reply_to=r), timeout=5.0
    )

    results: list[int] = []

    async def consume() -> None:
        async for item in source:
            results.append(item)

    consume_task = asyncio.create_task(consume())

    for item in data:
        await sink.put(item)
    await sink.complete()

    await asyncio.wait_for(consume_task, timeout=5.0)
    return results


async def test_cross_node_consecutive_output() -> None:
    async with ClusteredActorSystem(
        name="cluster", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            r1 = await _run_stream_cycle(
                system_a, system_b, "out-1", "out-1", [1, 2, 3]
            )
            assert r1 == [1, 2, 3]

            r2 = await _run_stream_cycle(
                system_a, system_b, "out-2", "out-2", [4, 5, 6]
            )
            assert r2 == [4, 5, 6]


async def test_cross_node_consecutive_input() -> None:
    async with ClusteredActorSystem(
        name="cluster", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            r1 = await _run_stream_cycle(system_b, system_a, "in-1", "in-1", [1, 2, 3])
            assert r1 == [1, 2, 3]

            r2 = await _run_stream_cycle(system_b, system_a, "in-2", "in-2", [4, 5, 6])
            assert r2 == [4, 5, 6]


async def test_cross_node_consecutive_bidirectional() -> None:
    async with ClusteredActorSystem(
        name="cluster", host="127.0.0.1", port=0, node_id="node-1"
    ) as system_a:
        port_a = system_a.self_node.port

        async with ClusteredActorSystem(
            name="cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-2",
            seed_nodes=[("127.0.0.1", port_a)],
        ) as system_b:
            await system_a.wait_for(2, timeout=10.0)

            r1 = await _run_stream_cycle(system_a, system_b, "bi-1", "bi-1", [1, 2, 3])
            assert r1 == [1, 2, 3]

            r2 = await _run_stream_cycle(system_b, system_a, "bi-2", "bi-2", [4, 5, 6])
            assert r2 == [4, 5, 6]


async def test_dead_letters_after_cancel() -> None:
    dead: list[DeadLetter] = []

    async with ActorSystem("test") as system:
        observer = system.spawn(
            Behaviors.receive(lambda ctx, msg: (dead.append(msg), Behaviors.same())[1]),
            "dead-observer",
        )
        system.event_stream.tell(ESSubscribe(event_type=DeadLetter, handler=observer))
        await asyncio.sleep(0.05)

        producer = system.spawn(stream_producer(), "producer")
        consumer = system.spawn(stream_consumer(producer, initial_demand=4), "consumer")
        await asyncio.sleep(0.05)

        source: SourceRef[int] = await system.ask(
            consumer, lambda r: GetSource(reply_to=r), timeout=1.0
        )

        producer.tell(Push(1))

        async for item in source:
            break

        await asyncio.sleep(0.1)

        consumer.tell(StreamDemand(n=1))
        await asyncio.sleep(0.1)

        assert len(dead) >= 1
