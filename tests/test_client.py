from collections import Counter
from contextlib import suppress
from datetime import timedelta

import pytest

from casty import Client, NodeId, Refused, Unavailable
from tests.app import Entries, Note, Notes, Pay, Pending, Where, account, ledger, notes, order
from tests.cluster import Harness, Node
from tests.support import eventually
from tests.traffic import Traffic


def describe_client() -> None:
    def when_it_talks_to_a_running_cluster() -> None:
        async def it_reaches_keys_on_every_node_without_becoming_a_member() -> None:
            async with Harness.start(3) as harness:
                client = await harness.client()
                nodes = {node.system.node for node in harness.nodes}

                assert await client.ref(order, "o-1", initial=Pending()).ask(Pay, 10) is True
                assert await client.ref(order, "o-1", initial=Pending()).ask(Pay, 10) is False

                located = [await client.ref(account, f"acc-{index}").ask(Where) for index in range(_KEYS)]
                for value in range(_MANY):
                    client.ref(notes, "n-1").tell(Note(value))
                written = await client.ref(notes, "n-1").ask(Notes)

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

    def when_the_cluster_has_another_name() -> None:
        async def it_refuses_to_start() -> None:
            async with Harness.start(1) as harness:
                with pytest.raises(Refused):
                    await harness.client(name="another")


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
            owners[(await client.ref(ledger, key).ask(Entries)).node] += 1
    busiest = max(among, key=lambda node: owners[node.system.node])
    assert owners[busiest.system.node] > 0, f"none of the {len(keys)} keys answered from a node that started"
    return busiest


_KEYS = 30
_MANY = 1_000
_WITHIN = timedelta(seconds=15)
_BEFORE_ASK_TIMEOUT = timedelta(seconds=3)
