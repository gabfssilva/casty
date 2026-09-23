import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta

import pytest

from casty import ActorSystem, Backoff, Client, Cluster, Context, NodeId, Overlay, Unavailable, actor
from tests import deploy
from tests.app import LATCHES, Bump, Latch, Note, account, gated, notes
from tests.cluster import FAST, Harness, Node
from tests.support import eventually
from tests.traffic import Traffic


async def counter(ctx: Context[int, int]) -> None:
    """A body with nothing in it: these cases are about the parameters of `@actor`, not about what it runs."""
    async for _ in ctx.inbox:
        pass


INVALID: list[tuple[Callable[[], object], str]] = [
    (lambda: Cluster(bind="7400"), "bind"),
    (lambda: Cluster(bind="0.0.0.0:http"), "bind"),
    (lambda: Cluster(bind="0.0.0.0:70000"), "bind"),
    (lambda: Cluster(bind="0.0.0.0:7400", advertise="10.0.0.5"), "advertise"),
    (lambda: Cluster(bind="0.0.0.0:7400", seeds=("10.0.0.4:7400", "10.0.0.5")), "seeds"),
    (lambda: Cluster(bind="0.0.0.0:7400", heartbeat=timedelta(seconds=5)), "suspect_after"),
    (lambda: Overlay(active=0), "overlay.active"),
    (lambda: Overlay(active=5, passive=4), "overlay.passive"),
    (lambda: Backoff(factor=0.5), "backoff.factor"),
    (lambda: Backoff(factor=math.nan), "backoff.factor"),
    (lambda: Backoff(first=timedelta(seconds=20)), "backoff.first"),
    (lambda: actor(initial=0, replicas=0)(counter), "replicas"),
    (lambda: actor(initial=0, mailbox=0)(counter), "mailbox"),
    (lambda: actor(initial=0, on_full="wait")(counter), "on_full"),
    (lambda: actor(initial=0, ask_timeout=timedelta(seconds=-1))(counter), "ask_timeout"),
    (lambda: actor(initial=0, backoff=Backoff(first=timedelta(seconds=-1)))(counter), "backoff.first"),
    (lambda: Client(seeds=()), "seeds"),
    (lambda: Client(seeds=("10.0.0.4",)), "seeds"),
    (lambda: ActorSystem(idle_after=timedelta(seconds=-1)), "idle_after"),
    (lambda: ActorSystem(ask_timeout=timedelta(seconds=-1)), "ask_timeout"),
    (lambda: ActorSystem(write_timeout=timedelta(seconds=-1)), "write_timeout"),
    (lambda: ActorSystem(leave_timeout=timedelta(seconds=-1)), "leave_timeout"),
    (lambda: ActorSystem(backoff=Backoff(first=timedelta(seconds=-1))), "backoff.first"),
    (lambda: Client(seeds=("10.0.0.4:7400",), ask_timeout=timedelta(seconds=-1)), "ask_timeout"),
    (lambda: Client(seeds=("10.0.0.4:7400",), sync_every=timedelta(0)), "sync_every"),
]

#: Cluster timings that pass the dataclass and that the node refuses once it joins: the transport repeats the first
#: three and would never wait between two rounds, and the last is a threshold that cannot be negative.
REFUSED_ON_JOIN: list[tuple[Cluster, str]] = [
    (Cluster(bind="127.0.0.1:0", heartbeat=timedelta(0)), "heartbeat"),
    (Cluster(bind="127.0.0.1:0", anti_entropy=timedelta(0)), "anti_entropy"),
    (Cluster(bind="127.0.0.1:0", overlay=Overlay(graft_after=timedelta(0))), "overlay.graft_after"),
    (Cluster(bind="127.0.0.1:0", dead_after=timedelta(seconds=-1)), "dead_after"),
]


def describe_shutdown() -> None:
    def when_a_node_leaves_during_traffic() -> None:
        async def it_hands_off_its_keys_without_failed_asks() -> None:
            # An hour to suspect: the only way the others can stop counting `d` is `d` telling them it left.
            timing = replace(FAST, suspect_after=timedelta(hours=1))
            # Four nodes, so that the three that stay still form a quorum without the replica filling the range it
            # gained. Three would leave a ring of two with three replicas asked for, where every write needs both.
            async with Harness.start(4, timing=timing) as harness:
                a, b, c, d = harness.nodes
                gone = d.system.node
                async with Traffic.running(harness, senders=(a.system, b.system, c.system)) as traffic:
                    await traffic.settle()
                    # Only what the leave costs is counted: taking 50 keys over at once, at the start, is not it.
                    traffic.forget()
                    await harness.leave(d)
                    await traffic.settle()
                assert traffic.refused == ()
                await eventually(_forgotten(gone, a, b, c))
                listings = await traffic.verify()
                assert all(listing.node != gone for listing in listings.values())

    def when_a_local_system_exits_while_a_message_is_in_progress() -> None:
        async def it_finishes_the_message_before_exiting() -> None:
            latch = LATCHES["key"] = Latch()
            system = ActorSystem(leave_timeout=timedelta(seconds=30))
            exited = asyncio.Event()
            stop = asyncio.Event()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(_run(system, stop, exited))
                await eventually(_started(system))
                kept, noted = system.ref(gated, "key"), system.ref(notes, "key")
                answer = asyncio.ensure_future(kept.ask(Bump))
                await latch.held.wait()
                stop.set()
                await asyncio.sleep(0.1)
                assert not exited.is_set(), "the system exited while a message was still being processed"
                latch.released.set()
                # Bounded, and far below the `leave_timeout` above it: what ends the exit has to be the message
                # finishing, not the deadline running out.
                async with asyncio.timeout(timedelta(seconds=5).total_seconds()):
                    await exited.wait()
            assert await answer == 1
            with pytest.raises(RuntimeError):
                system.ref(gated, "key")
            # A ref taken before the exit says so too, instead of dropping the message or waiting out `ask_timeout`.
            with pytest.raises(RuntimeError):
                noted.tell(Note(1))
            with pytest.raises(RuntimeError):
                await kept.ask(Bump)

        async def it_exits_after_leave_timeout_if_the_body_is_stuck() -> None:
            latch = LATCHES["key"] = Latch()
            system = ActorSystem(leave_timeout=timedelta(milliseconds=200))
            exited = asyncio.Event()
            stop = asyncio.Event()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(_run(system, stop, exited))
                await eventually(_started(system))
                answer = asyncio.ensure_future(system.ref(gated, "key").ask(Bump))
                await latch.held.wait()
                stop.set()
                async with asyncio.timeout(timedelta(seconds=5).total_seconds()):
                    await exited.wait()
                with pytest.raises(Unavailable):
                    await answer
            latch.released.set()


def describe_rolling_deploy() -> None:
    def when_every_node_is_replaced_by_a_new_version_during_traffic() -> None:
        async def it_keeps_confirmed_state_and_serves_the_new_type() -> None:
            timing = replace(FAST, suspect_after=timedelta(hours=1))
            async with Harness.start(3, timing=timing) as harness:
                previous = harness.nodes
                fresh: list[Node] = []
                async with Traffic.running(harness) as traffic:
                    await traffic.settle()
                    for node in previous:
                        fresh.append(await harness.add(version=(deploy.ledger,)))
                        await eventually(_converged(harness))
                        # A type only the new version brings in is reachable through a node of the old one.
                        await node.system.ref(deploy.audit, f"audit-{node.id}").ask(deploy.Check)
                        await harness.leave(node)
                        await traffic.settle(timedelta(milliseconds=250))
                await traffic.verify()
                # The field the old version never wrote is written and read once every node is on the new one.
                served = fresh[-1].system
                assert await served.ref(deploy.ledger, "tagged").ask(deploy.Append, 1, ("audited",)) is True
                listing = await served.ref(deploy.ledger, "tagged").ask(deploy.Entries)
                assert listing.entries == (1,)
                assert listing.tags == ("audited",)


def describe_configuration() -> None:
    @pytest.mark.parametrize(("build", "parameter"), INVALID, ids=[parameter for _, parameter in INVALID])
    def it_rejects_inconsistent_parameters_naming_them(build: Callable[[], object], parameter: str) -> None:
        with pytest.raises(ValueError, match=parameter.replace(".", r"\.")):
            build()

    @pytest.mark.parametrize(
        ("network", "parameter"), REFUSED_ON_JOIN, ids=[parameter for _, parameter in REFUSED_ON_JOIN]
    )
    async def it_refuses_to_join_with_a_timing_the_transport_cannot_run(network: Cluster, parameter: str) -> None:
        with pytest.raises(ValueError, match=parameter.replace(".", r"\.")):
            async with ActorSystem(cluster=network):
                pass

    def when_the_parameters_agree() -> None:
        def it_accepts_them() -> None:
            # The settings of the tests and of the docstring of `Cluster` are the ones that must keep working.
            assert Cluster(bind="0.0.0.0:0", seeds=("10.0.0.4:7400",), heartbeat=timedelta(milliseconds=50))
            assert Overlay(active=2)
            assert account.replicas == notes.replicas == 3

    def when_a_type_sets_its_own_timings() -> None:
        def it_keeps_them_through_configured_and_leaves_the_unset_ones_to_the_system() -> None:
            backoff = Backoff(first=timedelta(milliseconds=10))
            tuned = actor(
                initial=0,
                idle_after=timedelta(seconds=1),
                ask_timeout=timedelta(seconds=2),
                write_timeout=timedelta(seconds=3),
                backoff=backoff,
            )(counter)

            configured = tuned.configured("tests.test_operation:tuned_5_all", 5, "all")

            assert (configured.replicas, configured.write) == (5, "all")
            assert (configured.idle_after, configured.ask_timeout, configured.write_timeout, configured.backoff) == (
                timedelta(seconds=1),
                timedelta(seconds=2),
                timedelta(seconds=3),
                backoff,
            )
            assert (account.idle_after, account.ask_timeout, account.write_timeout, account.backoff) == (
                None,
                None,
                None,
                None,
            )


async def _run(system: ActorSystem, stop: asyncio.Event, exited: asyncio.Event, /) -> None:
    """Hold a system open until `stop`, so that a test can watch its orderly exit from outside."""
    async with system:
        await stop.wait()
    exited.set()


def _started(system: ActorSystem, /) -> Callable[[], Awaitable[None]]:
    async def it_answers() -> None:
        assert system.members

    return it_answers


def _converged(harness: Harness, /) -> Callable[[], Awaitable[None]]:
    async def every_node_sees_every_other() -> None:
        nodes = {node.system.node for node in harness.nodes}
        for node in harness.nodes:
            alive = {member.node for member in node.system.members if member.status == "alive"}
            assert alive == nodes, f"{node.address} sees {len(alive)} of {len(nodes)} nodes"

    return every_node_sees_every_other


def _forgotten(gone: NodeId, *nodes: Node) -> Callable[[], Awaitable[None]]:
    async def nobody_counts_it() -> None:
        for node in nodes:
            assert gone not in {member.node for member in node.system.members}, f"{node.address} still sees it"

    return nobody_counts_it
