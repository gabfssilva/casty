from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from casty import ActorContext, ActorRef, Behavior, Behaviors, ActorSystem


# --- Shared messages for fetcher tests ---


@dataclass(frozen=True)
class FetchUser:
    user_id: str


@dataclass(frozen=True)
class UserFound:
    name: str


@dataclass(frozen=True)
class UserFetchFailed:
    error: str


@dataclass(frozen=True)
class GetResult:
    reply_to: ActorRef[str]


type FetcherMsg = FetchUser | UserFound | UserFetchFailed | GetResult


async def fake_fetch(user_id: str) -> str:
    await asyncio.sleep(0.01)
    return f"User-{user_id}"


async def fake_fetch_failing(user_id: str) -> str:
    await asyncio.sleep(0.01)
    msg = f"not found: {user_id}"
    raise ValueError(msg)


def fetcher_behavior(result: str = "") -> Behavior[FetcherMsg]:
    async def receive(
        ctx: ActorContext[FetcherMsg], msg: FetcherMsg
    ) -> Behavior[FetcherMsg]:
        match msg:
            case FetchUser(user_id=uid):
                ctx.pipe_to_self(
                    fake_fetch(uid),
                    lambda name: UserFound(name=name),
                )
                return Behaviors.same()
            case UserFound(name=name):
                return fetcher_behavior(result=name)
            case GetResult(reply_to=reply_to):
                reply_to.tell(result)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


def fetcher_with_failure(result: str = "") -> Behavior[FetcherMsg]:
    async def receive(
        ctx: ActorContext[FetcherMsg], msg: FetcherMsg
    ) -> Behavior[FetcherMsg]:
        match msg:
            case FetchUser(user_id=uid):
                ctx.pipe_to_self(
                    fake_fetch_failing(uid),
                    lambda name: UserFound(name=name),
                    on_failure=lambda exc: UserFetchFailed(error=str(exc)),
                )
                return Behaviors.same()
            case UserFetchFailed(error=error):
                return fetcher_with_failure(result=f"FAILED:{error}")
            case UserFound(name=name):
                return fetcher_with_failure(result=name)
            case GetResult(reply_to=reply_to):
                reply_to.tell(result)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


def fetcher_no_failure_handler(result: str = "") -> Behavior[FetcherMsg]:
    async def receive(
        ctx: ActorContext[FetcherMsg], msg: FetcherMsg
    ) -> Behavior[FetcherMsg]:
        match msg:
            case FetchUser(user_id=uid):
                ctx.pipe_to_self(
                    fake_fetch_failing(uid),
                    lambda name: UserFound(name=name),
                )
                return Behaviors.same()
            case GetResult(reply_to=reply_to):
                reply_to.tell(result)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


# --- Messages for non-blocking test ---


@dataclass(frozen=True)
class StartSlow:
    pass


@dataclass(frozen=True)
class QuickMsg:
    value: int


@dataclass(frozen=True)
class SlowDone:
    value: str


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[tuple[list[int], str]]


type NonBlockMsg = StartSlow | QuickMsg | SlowDone | GetState


async def slow_coro() -> str:
    await asyncio.sleep(0.2)
    return "done"


def non_blocking_actor(
    quick_msgs: tuple[int, ...] = (), slow_result: str = ""
) -> Behavior[NonBlockMsg]:
    async def receive(
        ctx: ActorContext[NonBlockMsg], msg: NonBlockMsg
    ) -> Behavior[NonBlockMsg]:
        match msg:
            case StartSlow():
                ctx.pipe_to_self(
                    slow_coro(),
                    lambda v: SlowDone(value=v),
                )
                return Behaviors.same()
            case QuickMsg(value=v):
                return non_blocking_actor((*quick_msgs, v), slow_result)
            case SlowDone(value=v):
                return non_blocking_actor(quick_msgs, v)
            case GetState(reply_to=reply_to):
                reply_to.tell((list(quick_msgs), slow_result))
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


# --- Messages for concurrent test ---


@dataclass(frozen=True)
class StartAll:
    pass


@dataclass(frozen=True)
class ComputeResult:
    value: int


@dataclass(frozen=True)
class GetResults:
    reply_to: ActorRef[tuple[int, ...]]


type ConcurrentMsg = StartAll | ComputeResult | GetResults


async def compute(n: int) -> int:
    await asyncio.sleep(0.01 * n)
    return n * 10


def concurrent_actor(results: tuple[int, ...] = ()) -> Behavior[ConcurrentMsg]:
    async def receive(
        ctx: ActorContext[ConcurrentMsg], msg: ConcurrentMsg
    ) -> Behavior[ConcurrentMsg]:
        match msg:
            case StartAll():
                for i in range(1, 4):
                    ctx.pipe_to_self(
                        compute(i),
                        lambda v: ComputeResult(value=v),
                    )
                return Behaviors.same()
            case ComputeResult(value=v):
                return concurrent_actor((*results, v))
            case GetResults(reply_to=reply_to):
                reply_to.tell(results)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


# --- Tests ---


async def test_pipe_to_self_sends_result_to_actor() -> None:
    async with ActorSystem("pipe-ok") as system:
        ref = system.spawn(fetcher_behavior(), "fetcher")

        ref.tell(FetchUser(user_id="42"))
        await asyncio.sleep(0.1)

        result: str = await system.ask(
            ref, lambda r: GetResult(reply_to=r), timeout=2.0
        )

        assert result == "User-42"


async def test_pipe_to_self_on_failure() -> None:
    async with ActorSystem("pipe-fail") as system:
        ref = system.spawn(fetcher_with_failure(), "fetcher")

        ref.tell(FetchUser(user_id="99"))
        await asyncio.sleep(0.1)

        result: str = await system.ask(
            ref, lambda r: GetResult(reply_to=r), timeout=2.0
        )

        assert result == "FAILED:not found: 99"


async def test_pipe_to_self_without_on_failure_logs_warning(
    caplog: Any,
) -> None:
    async with ActorSystem("pipe-log") as system:
        ref = system.spawn(fetcher_no_failure_handler(), "fetcher")

        with caplog.at_level(logging.WARNING):
            ref.tell(FetchUser(user_id="bad"))
            await asyncio.sleep(0.1)

        assert any("pipe_to_self failed" in r.message for r in caplog.records)

        result: str = await system.ask(
            ref, lambda r: GetResult(reply_to=r), timeout=2.0
        )
        assert result == ""


async def test_pipe_to_self_does_not_block_mailbox() -> None:
    async with ActorSystem("pipe-noblock") as system:
        ref: ActorRef[NonBlockMsg] = system.spawn(non_blocking_actor(), "actor")

        ref.tell(StartSlow())
        await asyncio.sleep(0.01)

        ref.tell(QuickMsg(value=1))
        ref.tell(QuickMsg(value=2))
        ref.tell(QuickMsg(value=3))
        await asyncio.sleep(0.05)

        state: tuple[list[int], str] = await system.ask(
            ref, lambda r: GetState(reply_to=r), timeout=2.0
        )
        quick, slow = state
        assert quick == [1, 2, 3]
        assert slow == ""

        await asyncio.sleep(0.3)

        state = await system.ask(ref, lambda r: GetState(reply_to=r), timeout=2.0)
        quick, slow = state
        assert quick == [1, 2, 3]
        assert slow == "done"


async def test_pipe_to_self_multiple_concurrent() -> None:
    async with ActorSystem("pipe-multi") as system:
        ref: ActorRef[ConcurrentMsg] = system.spawn(concurrent_actor(), "actor")

        ref.tell(StartAll())
        await asyncio.sleep(0.2)

        results: tuple[int, ...] = await system.ask(
            ref, lambda r: GetResults(reply_to=r), timeout=2.0
        )

        assert sorted(results) == [10, 20, 30]
