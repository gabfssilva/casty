"""Shopping cart — state machines + scheduler integration.

Four behaviors model the cart lifecycle:

    empty_cart  ──AddItem──▶  active_cart  ──Checkout──▶  checked_out
                                   │
                                   ├──CartExpired──▶  expired
                                   │
                                   └──RemoveItem (last)──▶  empty_cart

A scheduler fires ``CartExpired`` after 5 seconds of inactivity,
demonstrating one-shot timers with ``ScheduleOnce`` / ``CancelSchedule``.
"""

import asyncio
from dataclasses import dataclass

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    CancelSchedule,
    ScheduleOnce,
)
from casty.scheduler import scheduler


# ── Messages ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Item:
    name: str
    price: float


@dataclass(frozen=True)
class Receipt:
    items: tuple[Item, ...]
    total: float


@dataclass(frozen=True)
class AddItem:
    item: Item


@dataclass(frozen=True)
class RemoveItem:
    name: str


@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[float]


@dataclass(frozen=True)
class Checkout:
    reply_to: ActorRef[Receipt]


@dataclass(frozen=True)
class CartExpired:
    pass


type CartMsg = AddItem | RemoveItem | GetTotal | Checkout | CartExpired


# ── Behaviors ────────────────────────────────────────────────────────

type SchedulerRef = ActorRef[ScheduleOnce | CancelSchedule]


def empty_cart(scheduler_ref: SchedulerRef) -> Behavior[CartMsg]:
    async def receive(
        ctx: ActorContext[CartMsg], msg: CartMsg
    ) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                scheduler_ref.tell(
                    ScheduleOnce(
                        key="cart-timeout",
                        target=ctx.self,
                        message=CartExpired(),
                        delay=5.0,
                    )
                )
                return active_cart(scheduler_ref, {item.name: item})
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def active_cart(
    scheduler_ref: SchedulerRef, items: dict[str, Item]
) -> Behavior[CartMsg]:
    async def receive(
        ctx: ActorContext[CartMsg], msg: CartMsg
    ) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                return active_cart(scheduler_ref, {**items, item.name: item})

            case RemoveItem(name) if name in items:
                remaining = {k: v for k, v in items.items() if k != name}
                if remaining:
                    return active_cart(scheduler_ref, remaining)
                scheduler_ref.tell(CancelSchedule(key="cart-timeout"))
                return empty_cart(scheduler_ref)

            case GetTotal(reply_to):
                reply_to.tell(sum(item.price for item in items.values()))
                return Behaviors.same()

            case Checkout(reply_to):
                scheduler_ref.tell(CancelSchedule(key="cart-timeout"))
                receipt = Receipt(
                    items=tuple(items.values()),
                    total=sum(item.price for item in items.values()),
                )
                reply_to.tell(receipt)
                return checked_out()

            case CartExpired():
                print("Cart expired — items discarded")
                return expired()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def checked_out() -> Behavior[CartMsg]:
    async def receive(
        ctx: ActorContext[CartMsg], msg: CartMsg
    ) -> Behavior[CartMsg]:
        return Behaviors.same()

    return Behaviors.receive(receive)


def expired() -> Behavior[CartMsg]:
    async def receive(
        ctx: ActorContext[CartMsg], msg: CartMsg
    ) -> Behavior[CartMsg]:
        return Behaviors.same()

    return Behaviors.receive(receive)


# ── Setup wrapper ────────────────────────────────────────────────────


def shopping_cart() -> Behavior[CartMsg]:
    async def setup(ctx: ActorContext[CartMsg]) -> Behavior[CartMsg]:
        scheduler_ref = ctx.spawn(scheduler(), "scheduler")
        return empty_cart(scheduler_ref)

    return Behaviors.setup(setup)


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    async with ActorSystem() as system:
        # Cart 1: successful checkout
        cart1 = system.spawn(shopping_cart(), "cart-1")
        cart1.tell(AddItem(Item("keyboard", 75.0)))
        cart1.tell(AddItem(Item("mouse", 25.0)))
        await asyncio.sleep(0.1)

        total: float = await system.ask(
            cart1, lambda r: GetTotal(reply_to=r), timeout=5.0
        )
        print(f"Total: ${total:.2f}")

        receipt: Receipt = await system.ask(
            cart1, lambda r: Checkout(reply_to=r), timeout=5.0
        )
        print(f"Receipt: {len(receipt.items)} items, ${receipt.total:.2f}")

        # Cart 2: abandonment / expiration
        cart2 = system.spawn(shopping_cart(), "cart-2")
        cart2.tell(AddItem(Item("monitor", 300.0)))
        print("Waiting for cart-2 to expire...")
        await asyncio.sleep(6.0)


asyncio.run(main())
