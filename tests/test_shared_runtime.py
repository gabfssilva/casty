import asyncio
import socket
from functools import partial

import pytest

from casty import ActorSystem, Client, Cluster, Refused, Runtime
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


async def _released(address: str) -> None:
    assert _free(address)


def _unused() -> str:
    """An address on loopback nothing listens on: a seed there never answers."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{probe.getsockname()[1]}"


def describe_runtime() -> None:
    def it_takes_at_least_one_thread() -> None:
        with pytest.raises(ValueError, match="threads"):
            Runtime(threads=0)

    def when_systems_share_one() -> None:
        async def it_carries_the_nodes_and_the_clients_of_a_cluster() -> None:
            async with Harness.start(3, runtime=Runtime(threads=2)) as harness:
                client = await harness.client()
                ref = client.ref(account, "shared")
                await ref.ask(Deposit(5))
                assert await harness.nodes[0].system.ref(account, "shared").ask(Balance()) == 5

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
                        await ref.ask(Balance())

                    await eventually(_still_served, WITHIN)


def describe_entering() -> None:
    def when_the_caller_gives_up_on_the_join() -> None:
        async def it_frees_the_address_and_leaves_no_task_of_the_node() -> None:
            apart = Runtime(threads=1)
            address = _unused()
            system = ActorSystem(cluster=Cluster(bind=address, seeds=(_unused(),)), runtime=apart)

            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.5), system:
                    pass

            await eventually(partial(_released, address))
            await eventually(partial(_idle, apart))
            async with ActorSystem(cluster=Cluster(bind=address)) as again:
                assert again.node.address == address

        async def it_leaves_no_task_of_a_client() -> None:
            apart = Runtime(threads=1)
            client = Client(seeds=(_unused(),), runtime=apart)

            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.5), client:
                    pass

            await eventually(partial(_idle, apart))

    def when_the_seed_refuses_it() -> None:
        async def it_frees_the_address_and_leaves_no_task_of_the_node() -> None:
            async with ActorSystem(cluster=Cluster(bind="127.0.0.1:0", name="another")) as other:
                assert other.node.address is not None
                apart = Runtime(threads=1)
                address = _unused()
                system = ActorSystem(cluster=Cluster(bind=address, seeds=(other.node.address,)), runtime=apart)

                with pytest.raises(Refused):
                    async with system:
                        pass

                await eventually(partial(_released, address))
                await eventually(partial(_idle, apart))
