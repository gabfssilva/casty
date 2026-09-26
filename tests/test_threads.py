"""The threads of the transport: what they have for the event loop, they hand to it, and they never run Python."""

import asyncio
import os
import sys
import threading
from collections.abc import Generator
from contextlib import contextmanager
from types import CodeType
from unittest.mock import patch

from casty import ActorSystem, Cluster
from tests.app import Append, Deposit, account, durable_ledger
from tests.cluster import Harness
from tests.support import Records, eventually


@contextmanager
def _running_python() -> Generator[set[int]]:
    """The threads that start a Python function while this is open."""
    monitoring = sys.monitoring
    tool = next(tool for tool in range(6) if monitoring.get_tool(tool) is None)
    threads: set[int] = set()

    def started(code: CodeType, offset: int, /) -> None:
        threads.add(threading.get_ident())

    monitoring.use_tool_id(tool, "casty threads")
    monitoring.register_callback(tool, monitoring.events.PY_START, started)
    monitoring.set_events(tool, monitoring.events.PY_START)
    try:
        yield threads
    finally:
        monitoring.set_events(tool, monitoring.events.NO_EVENTS)
        monitoring.register_callback(tool, monitoring.events.PY_START, None)
        monitoring.free_tool_id(tool)


def _descriptors() -> int:
    return len(os.listdir("/dev/fd"))


def describe_the_threads_of_the_transport() -> None:
    async def it_leaves_every_call_into_python_to_the_loop() -> None:
        with _running_python() as running:
            async with Harness.start(2, store=Records()) as harness:
                client = await harness.client()
                assert await client.ref(account, "a").ask(Deposit(5)) == 5
                assert await harness.nodes[1].system.ref(durable_ledger, "l").ask(Append(1))
                await harness.leave(harness.nodes[1])

        assert running == {threading.get_ident()}

    def when_the_loop_has_no_readers() -> None:
        async def it_hands_the_work_over_all_the_same() -> None:
            # A loop without readers is what the proactor loop of Windows is.
            with patch.object(asyncio.get_running_loop(), "add_reader", side_effect=NotImplementedError) as refused:
                async with Harness.start(2) as harness:
                    client = await harness.client()
                    assert await client.ref(account, "a").ask(Deposit(5)) == 5

            assert refused.called

    def when_its_system_exits() -> None:
        async def it_closes_the_sockets_it_woke_the_loop_by() -> None:
            before = _descriptors()
            for _ in range(3):
                async with ActorSystem(cluster=Cluster(bind="127.0.0.1:0")) as system:
                    assert await system.ref(account, "a").ask(Deposit(1)) >= 1

            async def closed() -> None:
                assert _descriptors() == before

            await eventually(closed)
