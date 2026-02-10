"""Spy — observing every message an actor receives."""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, SpyEvent, Terminated


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


def log_observer() -> Behavior[SpyEvent[AccountMsg]]:
    async def receive(
        ctx: ActorContext[SpyEvent[AccountMsg]], event: SpyEvent[AccountMsg]
    ) -> Behavior[SpyEvent[AccountMsg]]:
        match event.event:
            case Terminated():
                print(f"[spy] {event.actor_path} terminated at t={event.timestamp:.4f}")
            case msg:
                print(f"[spy] {event.actor_path} received {msg} at t={event.timestamp:.4f}")
        return Behaviors.same()

    return Behaviors.receive(receive)


async def main() -> None:
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


asyncio.run(main())
