"""Streaming behaviors for reactive producer-consumer patterns.

Provides ``stream_producer`` and ``stream_consumer`` behaviors implementing
demand-based backpressure (Reactive Streams). The producer buffers elements
until a consumer subscribes and requests demand. The consumer provides a
``SourceRef`` async iterator for consumption.

Input-side backpressure is available via ``SinkRef`` — the mirror of
``SourceRef``. A caller obtains a ``SinkRef`` via ``GetSink`` and pushes
elements with ``await sink.put(elem)``, which blocks when the internal
buffer is full.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty.core.behavior import Behavior
from casty.behaviors import Behaviors

if TYPE_CHECKING:
    from casty.core.context import ActorContext
    from casty.core.ref import ActorRef


@dataclass(frozen=True)
class Push[E]:
    element: E


@dataclass(frozen=True)
class CompleteStream:
    pass


@dataclass(frozen=True)
class GetSource[E]:
    reply_to: ActorRef[SourceRef[E]]


@dataclass(frozen=True)
class GetSink[E]:
    reply_to: ActorRef[SinkRef[E]]


@dataclass(frozen=True)
class Subscribe[E]:
    consumer: ActorRef[StreamElement[E] | StreamCompleted]
    demand: int = 0


@dataclass(frozen=True)
class StreamDemand:
    n: int


@dataclass(frozen=True)
class StreamCancel:
    pass


@dataclass(frozen=True)
class InputReady:
    pass


@dataclass(frozen=True)
class StreamElement[E]:
    element: E


@dataclass(frozen=True)
class StreamCompleted:
    pass


type StreamProducerMsg[E] = (
    Push[E]
    | CompleteStream
    | Subscribe[E]
    | StreamDemand
    | StreamCancel
    | GetSink[E]
    | InputReady
)

type StreamConsumerMsg[E] = (
    GetSource[E] | StreamDemand | StreamCancel | StreamElement[E] | StreamCompleted
)


class SourceRef[E]:
    """Async iterator over a stream with automatic demand replenishment.

    Parameters
    ----------
    queue
        Internal queue fed by the consumer actor's receive handler.
    cancel
        Callback to send ``StreamCancel`` to the consumer actor.
    timeout
        Inactivity timeout in seconds before the stream ends.
    request_demand
        Callback to send ``StreamDemand`` to the consumer actor.
    """

    def __init__(
        self,
        queue: asyncio.Queue[StreamElement[E] | StreamCompleted],
        cancel: Callable[[], None],
        timeout: float,
        request_demand: Callable[[int], None],
    ) -> None:
        self._queue = queue
        self._cancel = cancel
        self._timeout = timeout
        self._request_demand = request_demand

    async def __aiter__(self) -> AsyncGenerator[E, None]:
        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        self._queue.get(), timeout=self._timeout
                    )
                except asyncio.TimeoutError:
                    return

                match item:
                    case StreamElement(element=element):
                        self._request_demand(1)
                        yield element
                    case StreamCompleted():
                        return
        finally:
            self._cancel()


class SinkRef[E]:
    """Input-side handle for pushing elements into a ``stream_producer``.

    Obtained via ``GetSink``. Backed by the producer's internal
    ``asyncio.Queue`` — ``put`` blocks when the buffer is full,
    providing input-side backpressure.

    Parameters
    ----------
    queue
        The producer's internal bounded queue.
    producer
        Ref to the producer actor, used to nudge it after each put.
    """

    def __init__(
        self,
        queue: asyncio.Queue[E],
        producer: ActorRef[StreamProducerMsg[E]],
    ) -> None:
        self._queue = queue
        self._producer = producer

    async def put(self, element: E) -> None:
        """Push an element, blocking if the buffer is full."""
        await self._queue.put(element)
        self._producer.tell(InputReady())

    async def complete(self) -> None:
        """Signal that no more elements will be pushed."""
        self._producer.tell(CompleteStream())


def stream_producer[E](*, buffer_size: int = 0) -> Behavior[StreamProducerMsg[E]]:
    """Buffered, demand-gated element source.

    Parameters
    ----------
    buffer_size
        Maximum number of buffered elements. ``0`` means unbounded
        (backward compatible with the original tuple-based buffer).

    Story: ``idle`` → ``subscribed`` → stopped.
    """

    async def setup(
        ctx: ActorContext[StreamProducerMsg[E]],
    ) -> Behavior[StreamProducerMsg[E]]:
        queue: asyncio.Queue[E] = asyncio.Queue(maxsize=buffer_size)
        return idle(queue, completed=False)

    def drain(
        queue: asyncio.Queue[E],
        consumer: ActorRef[StreamElement[E] | StreamCompleted],
        demand: int,
        completed: bool,
    ) -> Behavior[StreamProducerMsg[E]] | None:
        """Drain the queue up to *demand* elements. Return new behavior or None."""
        sent = 0
        while demand > 0 and not queue.empty():
            element = queue.get_nowait()
            consumer.tell(StreamElement(element=element))
            demand -= 1
            sent += 1
        if queue.empty() and completed:
            consumer.tell(StreamCompleted())
            return Behaviors.stopped()
        if sent > 0:
            return subscribed(consumer, demand, queue, completed)
        return None

    def idle(
        queue: asyncio.Queue[E],
        completed: bool,
    ) -> Behavior[StreamProducerMsg[E]]:
        async def receive(
            ctx: ActorContext[StreamProducerMsg[E]],
            msg: StreamProducerMsg[E],
        ) -> Behavior[StreamProducerMsg[E]]:
            match msg:
                case Push(element=element):
                    queue.put_nowait(element)
                    return idle(queue, completed)
                case CompleteStream():
                    return idle(queue, completed=True)
                case GetSink(reply_to=reply_to):
                    reply_to.tell(SinkRef(queue=queue, producer=ctx.self))
                    return Behaviors.same()
                case InputReady():
                    return Behaviors.same()
                case Subscribe(consumer=consumer, demand=demand):
                    result = drain(queue, consumer, demand, completed)
                    if result is not None:
                        return result
                    return subscribed(consumer, demand, queue, completed)
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    def subscribed(
        consumer: ActorRef[StreamElement[E] | StreamCompleted],
        demand: int,
        queue: asyncio.Queue[E],
        completed: bool,
    ) -> Behavior[StreamProducerMsg[E]]:
        async def receive(
            ctx: ActorContext[StreamProducerMsg[E]],
            msg: StreamProducerMsg[E],
        ) -> Behavior[StreamProducerMsg[E]]:
            match msg:
                case Push(element=element):
                    if demand > 0:
                        consumer.tell(StreamElement(element=element))
                        return subscribed(consumer, demand - 1, queue, completed)
                    queue.put_nowait(element)
                    return Behaviors.same()
                case GetSink(reply_to=reply_to):
                    reply_to.tell(SinkRef(queue=queue, producer=ctx.self))
                    return Behaviors.same()
                case InputReady():
                    result = drain(queue, consumer, demand, completed)
                    if result is not None:
                        return result
                    return Behaviors.same()
                case StreamDemand(n=n):
                    new_demand = demand + n
                    result = drain(queue, consumer, new_demand, completed)
                    if result is not None:
                        return result
                    return subscribed(consumer, new_demand, queue, completed)
                case CompleteStream():
                    if queue.empty():
                        consumer.tell(StreamCompleted())
                        return Behaviors.stopped()
                    return subscribed(consumer, demand, queue, completed=True)
                case StreamCancel():
                    return Behaviors.stopped()
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


def stream_consumer[E](
    producer: ActorRef[StreamProducerMsg[E]],
    *,
    timeout: float = 30.0,
    initial_demand: int = 16,
) -> Behavior[StreamConsumerMsg[E]]:
    """Subscription mediator between a ``stream_producer`` and a ``SourceRef``.

    Story: ``waiting`` → ``active`` → stopped.
    """

    async def setup(
        ctx: ActorContext[StreamConsumerMsg[E]],
    ) -> Behavior[StreamConsumerMsg[E]]:
        queue: asyncio.Queue[StreamElement[E] | StreamCompleted] = asyncio.Queue()
        producer.tell(Subscribe(consumer=ctx.self, demand=0))
        return waiting(queue)

    def waiting(
        queue: asyncio.Queue[StreamElement[E] | StreamCompleted],
    ) -> Behavior[StreamConsumerMsg[E]]:
        async def receive(
            ctx: ActorContext[StreamConsumerMsg[E]],
            msg: StreamConsumerMsg[E],
        ) -> Behavior[StreamConsumerMsg[E]]:
            match msg:
                case GetSource(reply_to=reply_to):
                    source: SourceRef[E] = SourceRef(
                        queue=queue,
                        cancel=lambda: ctx.self.tell(StreamCancel()),
                        timeout=timeout,
                        request_demand=lambda n: ctx.self.tell(StreamDemand(n=n)),
                    )
                    reply_to.tell(source)
                    producer.tell(StreamDemand(n=initial_demand))
                    return active(queue)
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    def active(
        queue: asyncio.Queue[StreamElement[E] | StreamCompleted],
    ) -> Behavior[StreamConsumerMsg[E]]:
        async def receive(
            _ctx: ActorContext[StreamConsumerMsg[E]],
            msg: StreamConsumerMsg[E],
        ) -> Behavior[StreamConsumerMsg[E]]:
            match msg:
                case StreamElement():
                    queue.put_nowait(msg)
                    return Behaviors.same()
                case StreamCompleted():
                    queue.put_nowait(msg)
                    return Behaviors.same()
                case StreamDemand(n=n):
                    producer.tell(StreamDemand(n=n))
                    return Behaviors.same()
                case StreamCancel():
                    producer.tell(StreamCancel())
                    return Behaviors.stopped()
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)
