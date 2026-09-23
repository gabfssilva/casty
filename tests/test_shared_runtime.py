import socket
from functools import partial

import pytest

from casty import Runtime
from tests.app import Balance, Deposit, account
from tests.cluster import WITHIN, Harness
from tests.support import eventually


def _free(address: str) -> bool:
    host, port = address.rsplit(":", 1)
    with socket.socket() as probe:
        try:
            probe.bind((host, int(port)))
        except OSError:
            return False
    return True


async def _idle(runtime: Runtime) -> None:
    assert runtime._tasks() == 0  # pyright: ignore[reportPrivateUsage]


def describe_runtime() -> None:
    def it_takes_at_least_one_thread() -> None:
        with pytest.raises(ValueError, match="threads"):
            Runtime(threads=0)

    def when_systems_share_one() -> None:
        async def it_carries_the_nodes_and_the_clients_of_a_cluster() -> None:
            async with Harness.start(3, runtime=Runtime(threads=2)) as harness:
                client = await harness.client()
                ref = client.ref(account, "shared")
                await ref.ask(Deposit, 5)
                assert await harness.nodes[0].system.ref(account, "shared").ask(Balance) == 5

        async def it_leaves_nothing_of_a_node_that_crashed_or_left_while_the_others_go_on() -> None:
            async with Harness.start(3, runtime=Runtime(threads=2)) as harness:
                client = await harness.client()
                ref = client.ref(account, "ongoing")
                for end in (harness.crash, harness.leave):
                    apart = Runtime(threads=1)
                    node = await harness.add(runtime=apart)
                    assert apart._tasks() > 0  # pyright: ignore[reportPrivateUsage]

                    await end(node)

                    assert _free(node.address)
                    await eventually(partial(_idle, apart))

                    # A key the node that ended owned answers again once the others have taken it over.
                    async def _still_served() -> None:
                        await ref.ask(Balance)

                    await eventually(_still_served, WITHIN)
