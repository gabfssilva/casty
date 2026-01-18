"""Ticker actor for periodic message emission.

The Ticker sends messages at configurable intervals to subscribed listeners.
Each subscription can have its own message and interval.

Usage via ctx.tick():
    class MyActor(Actor[Ping]):
        async def on_start(self):
            # Send Ping() to self every 0.5 seconds
            self.tick_id = await self._ctx.tick(Ping(), interval=0.5)

        async def receive(self, msg: Ping, ctx: Context):
            print("tick!")

        async def on_stop(self):
            await self._ctx.cancel_tick(self.tick_id)

Direct usage:
    ticker = await system.spawn(Ticker)
    sub_id = await ticker.ask(Subscribe(collector, Ping(), interval=0.5))
    # collector receives Ping() every 0.5s
    await ticker.send(Unsubscribe(sub_id))
"""

import uuid
from dataclasses import dataclass
from typing import Any

from casty import Actor, Context, LocalActorRef


@dataclass(frozen=True, slots=True)
class Subscribe:
    """Subscribe a listener to receive a message at the specified interval."""

    listener: LocalActorRef[Any]
    message: Any
    interval: float


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    """Unsubscribe by subscription ID."""

    subscription_id: str


@dataclass(frozen=True, slots=True)
class _ScheduledTick:
    """Internal message for scheduled tick delivery."""

    subscription_id: str


@dataclass
class _Subscription:
    """Internal subscription state."""

    listener: LocalActorRef[Any]
    message: Any
    interval: float
    task_id: str | None


type TickerMessage = Subscribe | Unsubscribe | _ScheduledTick


class Ticker(Actor[TickerMessage]):
    """Actor that sends periodic messages to subscribed listeners.

    Each subscription specifies a listener, message, and interval.
    The Ticker manages independent schedules for each subscription.

    Example:
        ticker = await system.spawn(Ticker)
        sub_id = await ticker.ask(Subscribe(actor1, Ping(), interval=0.5))
        # actor1 receives Ping() every 0.5s
        await ticker.send(Unsubscribe(sub_id))
    """

    def __init__(self):
        self.subscriptions: dict[str, _Subscription] = {}

    async def receive(self, msg: TickerMessage, ctx: Context[TickerMessage]) -> None:
        match msg:
            case Subscribe(listener, message, interval):
                subscription_id = str(uuid.uuid4())

                self.subscriptions[subscription_id] = _Subscription(
                    listener=listener,
                    message=message,
                    interval=interval,
                    task_id=None,
                )

                task_id = await ctx.schedule(interval, _ScheduledTick(subscription_id))
                self.subscriptions[subscription_id].task_id = task_id

                await ctx.reply(subscription_id)

            case Unsubscribe(subscription_id):
                sub = self.subscriptions.pop(subscription_id, None)
                if sub and sub.task_id:
                    await ctx.cancel_schedule(sub.task_id)

            case _ScheduledTick(subscription_id):
                sub = self.subscriptions.get(subscription_id)
                if sub is None:
                    return

                await sub.listener.send(sub.message)

                sub.task_id = await ctx.schedule(sub.interval, _ScheduledTick(subscription_id))

    async def on_stop(self) -> None:
        self.subscriptions.clear()
