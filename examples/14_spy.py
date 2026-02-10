"""Spy — observing every message an actor receives."""

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, SpyEvent, Terminated


# ---------------------------------------------------------------------------
# Example 1: Basic spy on a single actor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountMsg = Deposit | GetBalance


def account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return account(balance + amount)
            case GetBalance(reply_to=reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)


def log_observer() -> Behavior[SpyEvent[Any]]:
    async def receive(
        ctx: ActorContext[SpyEvent[Any]], event: SpyEvent[Any]
    ) -> Behavior[SpyEvent[Any]]:
        match event.event:
            case Terminated():
                print(f"[spy] {event.actor_path} terminated at t={event.timestamp:.4f}")
            case msg:
                print(f"[spy] {event.actor_path} received {msg} at t={event.timestamp:.4f}")
        return Behaviors.same()

    return Behaviors.receive(receive)


async def basic_spy() -> None:
    print("=== Basic spy ===\n")
    async with ActorSystem("bank") as system:
        observer = system.spawn(log_observer(), "observer")

        ref = system.spawn(
            Behaviors.spy(account(), observer),
            "account",
        )

        ref.tell(Deposit(100))
        ref.tell(Deposit(50))
        balance = await system.ask(ref, lambda r: GetBalance(reply_to=r), timeout=2.0)
        print(f"Balance: {balance}")

        await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# Example 2: spy_children — observe an entire actor subtree
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProcessOrder:
    order_id: str
    item: str


@dataclass(frozen=True)
class Pack:
    order_id: str
    item: str


@dataclass(frozen=True)
class Ship:
    order_id: str


type ManagerMsg = ProcessOrder
type PackerMsg = Pack
type ShipperMsg = Ship


def shipper() -> Behavior[ShipperMsg]:
    async def receive(ctx: ActorContext[ShipperMsg], msg: ShipperMsg) -> Behavior[ShipperMsg]:
        match msg:
            case Ship(order_id):
                ctx.log.info("Shipped order %s", order_id)
                return Behaviors.same()

    return Behaviors.receive(receive)


def packer() -> Behavior[PackerMsg]:
    async def setup(ctx: ActorContext[PackerMsg]) -> Behavior[PackerMsg]:
        shipper_ref = ctx.spawn(shipper(), "shipper")

        async def receive(ctx: ActorContext[PackerMsg], msg: PackerMsg) -> Behavior[PackerMsg]:
            match msg:
                case Pack(order_id, item):
                    ctx.log.info("Packing %s for order %s", item, order_id)
                    shipper_ref.tell(Ship(order_id=order_id))
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


def order_manager() -> Behavior[ManagerMsg]:
    async def setup(ctx: ActorContext[ManagerMsg]) -> Behavior[ManagerMsg]:
        packer_ref = ctx.spawn(packer(), "packer")

        async def receive(ctx: ActorContext[ManagerMsg], msg: ManagerMsg) -> Behavior[ManagerMsg]:
            match msg:
                case ProcessOrder(order_id, item):
                    packer_ref.tell(Pack(order_id=order_id, item=item))
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


async def spy_children() -> None:
    print("\n=== spy_children ===\n")
    async with ActorSystem("warehouse") as system:
        observer = system.spawn(log_observer(), "observer")

        manager = system.spawn(
            Behaviors.spy(order_manager(), observer, spy_children=True),
            "manager",
        )

        manager.tell(ProcessOrder(order_id="ORD-001", item="Widget"))
        manager.tell(ProcessOrder(order_id="ORD-002", item="Gadget"))

        await asyncio.sleep(0.2)


# ---------------------------------------------------------------------------

async def main() -> None:
    await basic_spy()
    await spy_children()


asyncio.run(main())
