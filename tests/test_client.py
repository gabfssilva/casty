import asyncio
import socket
from collections import Counter
from collections.abc import Coroutine
from contextlib import suppress
from datetime import timedelta

import pytest

from casty import Client, NodeId, Refused, Unavailable
from tests.app import (
    Balance,
    Deposit,
    Entries,
    Hold,
    Locate,
    Note,
    Notes,
    Pay,
    Pending,
    Where,
    account,
    gate,
    ledger,
    notes,
    order,
)
from tests.cluster import FAST, WITHIN, Harness, Node, Proxy
from tests.support import eventually
from tests.traffic import Traffic


class _Tunnel:
    """A proxy to `target` on a port of its own, which a test takes down as a tunnel that drops."""

    def __init__(self, target: str) -> None:
        self._tasks: set[asyncio.Task[None]] = set()
        self._proxy = Proxy(self._spawn, target)
        self.address = self._proxy.address
        self._spawn(self._proxy.serve())

    def _spawn(self, work: Coroutine[None, None, None], /) -> None:
        task = asyncio.get_running_loop().create_task(work)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        await self._proxy.close()
        running = tuple(self._tasks)
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)


def _refusing() -> str:
    """An address nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{probe.getsockname()[1]}"


def describe_client() -> None:
    def when_it_talks_to_a_running_cluster() -> None:
        async def it_reaches_keys_on_every_node_without_becoming_a_member() -> None:
            async with Harness.start(3) as harness:
                client = await harness.client()
                nodes = {node.system.node for node in harness.nodes}

                assert await client.ref(order, "o-1", initial=Pending()).ask(Pay(10)) is True
                assert await client.ref(order, "o-1", initial=Pending()).ask(Pay(10)) is False

                located = [await client.ref(account, f"acc-{index}").ask(Where()) for index in range(_KEYS)]
                for value in range(_MANY):
                    client.ref(notes, "n-1").tell(Note(value))
                written = await client.ref(notes, "n-1").ask(Notes())

                assert {where.node for where in located} == nodes
                # One connection and one mailbox per owner: the client is as ordered a sender as a node is.
                assert written.notes == tuple(range(_MANY))
                assert [{member.node for member in node.system.members} for node in harness.nodes] == [nodes] * 3

    def when_owners_change_while_it_sends() -> None:
        async def it_follows_the_new_owners_without_losing_confirmed_state() -> None:
            async with Harness.start(3) as harness:
                client = await harness.client()
                started = harness.nodes
                async with Traffic.running(harness, senders=(client,)) as traffic:
                    await traffic.settle()
                    joined = await harness.add()
                    await _everyone_sees(harness, 4)
                    await traffic.settle()
                    gone = await _owner_of_most_keys(client, traffic.keys, started)
                    harness.isolate(gone)
                    await harness.crash(gone)
                    await _everyone_sees(harness, 3)
                    await _confirms_again(traffic, _confirmed(traffic))
                listings = await traffic.verify()

                # The client read every key through its own table, so an owner it never learned about is unreachable.
                assert joined.system.node in {listing.node for listing in listings.values()}

    def when_the_only_node_it_reaches_dies() -> None:
        async def it_fails_the_ask_in_flight_and_sees_the_node_dead() -> None:
            async with Harness.start(1) as harness:
                client = await harness.client()
                (node,) = harness.nodes
                assert await client.ref(gate, "g-1").ask(Locate()) == node.system.node
                waiting = asyncio.ensure_future(client.ref(gate, "g-1").ask(Hold()))

                await harness.crash(node)

                # Nobody is left to tell the client: what fails the ask is the connection it rode on.
                async with asyncio.timeout(_BEFORE_ASK_TIMEOUT.total_seconds()):
                    with pytest.raises(Unavailable):
                        await waiting
                assert [member.status for member in client.members] == ["dead"]

    def when_another_process_takes_the_address_of_the_node_it_waits_on() -> None:
        async def it_fails_the_ask_in_flight_and_reaches_the_new_process() -> None:
            async with Harness.start(1) as harness:
                client = await harness.client()
                (node,) = harness.nodes
                waiting = asyncio.ensure_future(client.ref(gate, "g-1").ask(Hold()))
                assert await client.ref(gate, "g-2").ask(Locate()) == node.system.node

                await harness.crash(node)
                restarted = await harness.add(address=node.address)

                async with asyncio.timeout(_BEFORE_ASK_TIMEOUT.total_seconds()):
                    with pytest.raises(Unavailable):
                        await waiting

                async def the_new_process_answers() -> None:
                    assert await client.ref(gate, "g-2").ask(Locate()) == restarted.system.node

                await eventually(the_new_process_answers, _BEFORE_ASK_TIMEOUT)

    def when_its_connection_drops_and_the_node_lives() -> None:
        async def it_reconnects_and_keeps_the_ask_in_flight() -> None:
            async with Harness.start(1) as harness:
                client = await harness.client()
                (node,) = harness.nodes
                waiting = asyncio.ensure_future(client.ref(gate, "g-1").ask(Hold()))
                assert await client.ref(gate, "g-2").ask(Locate()) == node.system.node

                await harness.sever()

                assert await client.ref(gate, "g-3").ask(Locate()) == node.system.node
                await asyncio.sleep(_BEFORE_ASK_TIMEOUT.total_seconds() / 3)
                assert not waiting.done()
                assert [member.status for member in client.members] == ["alive"]
                waiting.cancel()

    def when_it_closes_with_an_ask_in_flight() -> None:
        async def it_fails_the_ask_with_unavailable_before_the_ask_timeout() -> None:
            async with Harness.start(1) as harness:
                (node,) = harness.nodes
                async with Client(seeds=(node.address,), ask_timeout=timedelta(minutes=1)) as client:
                    waiting = asyncio.ensure_future(client.ref(gate, "g-1").ask(Hold()))
                    assert await client.ref(gate, "g-2").ask(Locate()) == node.system.node

                with pytest.raises(Unavailable, match="stopped"):
                    async with asyncio.timeout(WITHIN.total_seconds()):
                        await waiting

    def when_the_cluster_has_another_name() -> None:
        async def it_refuses_to_start() -> None:
            async with Harness.start(1) as harness:
                with pytest.raises(Refused):
                    await harness.client(name="another")

    def when_its_address_map_answers_something_else_later() -> None:
        async def it_asks_the_map_again_after_a_dial_that_failed() -> None:
            async with Harness.start(1) as harness:
                target = harness.nodes[0].address
                routes = {target: _refusing()}

                async def _mend() -> None:
                    await asyncio.sleep(0.3)
                    routes[target] = target

                async with asyncio.TaskGroup() as mending:
                    mending.create_task(_mend())
                    async with (
                        asyncio.timeout(WITHIN.total_seconds()),
                        Client(
                            seeds=(target,),
                            address_map=lambda advertised: routes[advertised],
                            sync_every=FAST.sync_every,
                        ) as client,
                    ):
                        assert await client.ref(account, "mended").ask(Balance()) == 0

        async def it_follows_a_tunnel_that_comes_back_on_another_port() -> None:
            async with Harness.start(1) as harness:
                target = harness.nodes[0].address
                first = _Tunnel(target)
                second: _Tunnel | None = None
                routes = {target: first.address}
                try:
                    async with Client(
                        seeds=(target,),
                        address_map=lambda advertised: routes[advertised],
                        ask_timeout=timedelta(seconds=1),
                        sync_every=FAST.sync_every,
                    ) as client:
                        ref = client.ref(account, "tunnelled")
                        await ref.ask(Deposit(1))
                        await first.close()
                        second = _Tunnel(target)
                        routes[target] = second.address

                        async def _reached() -> None:
                            assert await ref.ask(Balance()) == 1

                        await eventually(_reached, WITHIN)
                finally:
                    await first.close()
                    if second is not None:
                        await second.close()


async def _everyone_sees(harness: Harness, count: int, /) -> None:
    async def every_node_sees_the_others() -> None:
        assert len(harness.nodes) == count
        for node in harness.nodes:
            alive = {member.node for member in node.system.members if member.status == "alive"}
            assert alive == {other.system.node for other in harness.nodes}, f"{node.address} sees {len(alive)}"

    await eventually(every_node_sees_the_others, _WITHIN)


async def _confirms_again(traffic: Traffic, stalled: int, /) -> None:
    """Wait until the traffic confirms something after the crash, while a worker would still be inside one `ask`.

    Every worker is waiting on the machine that died. Without the table failing them, none comes back before
    `ask_timeout`, so confirming again within less than it is what says the table, and not the timeout, freed them.
    """

    async def something_was_confirmed_again() -> None:
        assert _confirmed(traffic) > stalled

    await eventually(something_was_confirmed_again, _BEFORE_ASK_TIMEOUT)


def _confirmed(traffic: Traffic, /) -> int:
    return sum(len(traffic.confirmed(key)) for key in traffic.keys)


async def _owner_of_most_keys(client: Client, keys: tuple[str, ...], among: tuple[Node, ...], /) -> Node:
    """The node of `among` that owns the most keys, as the client reaches them now."""
    owners: Counter[NodeId] = Counter()
    for key in keys:
        with suppress(Unavailable, TimeoutError):
            owners[(await client.ref(ledger, key).ask(Entries())).node] += 1
    busiest = max(among, key=lambda node: owners[node.system.node])
    assert owners[busiest.system.node] > 0, f"none of the {len(keys)} keys answered from a node that started"
    return busiest


_KEYS = 30
_MANY = 1_000
_WITHIN = timedelta(seconds=15)
_BEFORE_ASK_TIMEOUT = timedelta(seconds=3)
