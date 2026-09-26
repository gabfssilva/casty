"""Incorrect use of the public API. Each line ending in `# error` must be reported by pyright, and no other line.

Input for pyright, never executed.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, assert_never

from casty import ActorSystem, Askable, Client, Collections, Context, Opaque, Ref, actor
from casty.collections import SemaphoreState, semaphore


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class Withdraw(Askable[bool]):
    amount: int


type AccountMsg = Deposit | Withdraw


@dataclass(frozen=True)
class Account:
    balance: int = 0


@dataclass(frozen=True)
class Offset:
    value: int = 0


@actor
async def order(ctx: Context[Account, Deposit]) -> None:
    async for _ in ctx.inbox:
        pass


@actor(initial=Account())
async def account(ctx: Context[Account, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(amount):
                await ctx.state.set(Account(ctx.state.value.balance + amount))
            case Withdraw(amount, reply_to=reply_to):
                reply_to.tell("no")  # error
            case _:
                assert_never(msg)


def ref_without_initial(system: ActorSystem) -> Ref[Deposit]:
    return system.ref(order, "o-1")  # error


def ref_with_initial_of_another_type(system: ActorSystem) -> Ref[Deposit]:
    return system.ref(order, "o-1", initial=Offset())  # error


@actor(initial=Account())  # error
async def defaulted_with_initial_of_another_type(ctx: Context[Offset]) -> None:
    await ctx.state.set(Offset())


def tell_outside_the_message_type(system: ActorSystem) -> None:
    system.ref(account, "a").tell(Offset())  # error


async def ask_with_wrong_arguments(system: ActorSystem) -> bool:
    return await system.ref(account, "a").ask(Withdraw("30"))  # error


async def ask_with_wrong_keyword_arguments(system: ActorSystem) -> bool:
    return await system.ref(account, "a").ask(Withdraw(amount="30"))  # error


async def ask_without_required_arguments(system: ActorSystem) -> bool:
    return await system.ref(account, "a").ask(Withdraw())  # error


async def ask_into_the_wrong_type(system: ActorSystem) -> str:
    return await system.ref(account, "a").ask(Withdraw(30))  # error


def widen_a_ref(deposits: Ref[Deposit]) -> Ref[AccountMsg]:
    return deposits  # error


async def save_and_become_outside_the_state_type(ctx: Context[Account, Deposit]) -> None:
    await ctx.state.set(Offset())  # error
    await ctx.state.update(lambda _: Offset())  # error
    await ctx.become(order, Offset())  # error
    await ctx.become(account)  # error


def hand_over_what_the_actor_does_not_take(ctx: Context[Account, Deposit]) -> None:
    bank = ctx.system.ref(account, "a")
    ctx.to_self(bank.ask(Withdraw(1)))  # error
    ctx.to_self(bank.ask(Withdraw(1)), lambda ok: ok)  # error
    ctx.to_self(bank.ask(Withdraw(1)), lambda _: Deposit(1), failed=lambda error: error)  # error


async def ask_what_is_not_askable(system: ActorSystem) -> None:
    await system.ref(account, "a").ask(Deposit(1))  # error
    await system.ref(account, "a").ask(Withdraw)  # error


def ask_through_the_context_what_does_not_fit(ctx: Context[Account, Deposit]) -> None:
    bank = ctx.system.ref(account, "a")
    ctx.ask(bank, Deposit(1), lambda _: Deposit(1))  # error
    ctx.ask(bank, semaphore.Get(), lambda _: Deposit(1))  # error
    ctx.ask(bank, Withdraw(1), lambda ok: ok)  # error
    ctx.ask(bank, Withdraw(1), lambda ok: Deposit(ok.upper()))  # error
    ctx.ask(bank, Withdraw(1), lambda _: Deposit(1), failed=lambda error: error)  # error
    ctx.ask(bank, Withdraw(1))  # error


async def schedule_what_the_actor_does_not_take(ctx: Context[Account, Deposit]) -> None:
    await ctx.schedule("poll", timedelta(seconds=1), None, Account(1))  # error
    await ctx.schedule("poll", 1.0, None, Deposit(1))  # error
    await ctx.schedule("poll", timedelta(seconds=1), None, lambda: Deposit(1))  # error
    await ctx.schedule(timedelta(seconds=1), None, Deposit(1))  # error


@actor  # error
async def body_without_context(ctx: Account) -> None:
    pass


async def incomplete_match(ctx: Context[Account, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit():
                pass
            case _:
                assert_never(msg)  # error


@actor(write="quorum")  # error
async def unknown_write_level(ctx: Context[Account]) -> None:
    pass


async def activations_of_a_client(client: Client) -> None:
    client.activations()  # error
    await client.release(account, "a")  # error


async def invalid_collection_types(system: ActorSystem) -> None:
    collections = Collections(system)
    entries = collections.dict("accounts", key=str, value=Account)
    await entries.put(1, Account())  # error
    await entries.put("one", Offset())  # error
    await entries.get(1)  # error
    queue = collections.queue("accounts", value=Account)
    await queue.offer(Offset())  # error
    register = collections.register("account", value=Account)
    await register.compare_and_set(Offset(), Account())  # error
    await collections.counter("visits").add("one")  # error
    await collections.set("names", value=str).add(1)  # error
    await collections.multimap("names", key=str, value=int).put("one", "two")  # error
    system.ref(semaphore.actor, "pool")  # error


async def answers_the_semaphore_cannot_tell(ctx: Context[Account, Deposit]) -> None:
    pool = ctx.system.ref(semaphore.actor, "pool", initial=SemaphoreState(capacity=1))
    pool.tell(semaphore.Acquire(reply_to=ctx.self))  # error


def packed(numbers: list[int]) -> bytes:
    return bytes(numbers)


def unpacked(data: bytes) -> list[int]:
    return list(data)


type Numbers = Annotated[list[int], Opaque(encode=packed, decode=unpacked)]


@dataclass(frozen=True)
class Sketch:
    strokes: Numbers


def opaque_field_read_as_its_bytes(sketch: Sketch) -> bytes:
    return sketch.strokes  # error
