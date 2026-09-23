import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from casty import ActorSystem, Context, Placement, actor
from tests.app import LATCHES, Balance, Bump, Deposit, Gate, Latch, Locate, Touch, Where, account, gated, touched
from tests.cluster import Harness, Node
from tests.support import eventually

WITHIN = timedelta(seconds=10)


@actor(pinned=True, initial=Gate())
async def post(ctx: Context[Gate, Locate]) -> None:
    async for msg in ctx.inbox:
        msg.reply_to.tell(ctx.system.node)


def describe_introspection() -> None:
    def when_keys_come_and_go() -> None:
        async def it_lists_a_key_after_an_ask_and_not_after_it_idles_out() -> None:
            async with ActorSystem(idle_after=timedelta(milliseconds=300)) as system:
                before = datetime.now(UTC)
                assert await system.ref(touched, "t-1").ask(Touch)

                [listed] = system.activations()
                assert (listed.actor, listed.key, listed.queued) == (touched.name, "t-1", 0)
                assert before - timedelta(seconds=1) <= listed.since <= datetime.now(UTC)

                async def idled_out() -> None:
                    assert system.activations() == ()

                await eventually(idled_out)

        async def it_shows_what_waits_in_each_mailbox_by_key() -> None:
            latch = LATCHES["b-1"] = LATCHES["b-2"] = Latch()

            async with ActorSystem() as system:
                asking = [asyncio.create_task(system.ref(gated, "b-2").ask(Bump)) for _ in range(3)]
                asking += [asyncio.create_task(system.ref(gated, "b-1").ask(Bump)) for _ in range(2)]

                # Each body holds the message it took, and the others wait in its mailbox.
                async def the_mailboxes_hold_the_rest() -> None:
                    listed = [(row.actor, row.key, row.queued) for row in system.activations()]
                    assert listed == [(gated.name, "b-1", 1), (gated.name, "b-2", 2)]

                await eventually(the_mailboxes_hold_the_rest)
                latch.released.set()
                assert sorted(await asyncio.gather(*asking)) == [1, 1, 2, 2, 3]

    def when_it_runs_alone() -> None:
        async def it_is_the_owner_and_the_only_replica_of_every_key() -> None:
            async with ActorSystem() as system:
                alone = Placement(system.node, (system.node,))
                assert await system.placement(account, "a-1") == alone
                assert await system.placement(post, "worker", at="127.0.0.1:7400") == alone

        async def it_takes_the_key_as_ref_does() -> None:
            async with ActorSystem() as system:
                with pytest.raises(TypeError, match="at="):
                    await system.placement(post, "worker")
                with pytest.raises(ValueError, match="@host:port"):
                    await system.placement(account, "@127.0.0.1:7400/a-1")

    def when_it_is_a_node_of_a_cluster() -> None:
        async def it_places_each_key_where_it_activates_before_and_after_a_node_joins() -> None:
            keys = [f"acc-{index}" for index in range(_KEYS)]

            async with Harness.start(3) as harness:
                await _placed_where_they_run(harness, keys)
                joined = await harness.add()
                await _placed_where_they_run(harness, keys)

                # The ring gave the node that joined some of the keys, so the placement did change.
                owners = {(await joined.system.placement(account, key)).owner for key in keys}
                assert joined.system.node in owners

        async def it_places_a_pinned_key_on_the_node_it_names_from_every_node_and_a_client() -> None:
            async with Harness.start(3) as harness:
                client = await harness.client()
                for target in harness.nodes:
                    named = Placement(target.system.node, (target.system.node,))
                    for node in harness.nodes:
                        assert await node.system.placement(post, "worker", at=target.address) == named
                    assert await client.placement(post, "worker", at=target.address) == named
                    assert await client.ref(post, "worker", at=target.address).ask(Locate) == target.system.node
                    assert (post.name, f"@{target.address}/worker") in _listed(target)

        async def it_answers_a_client_with_the_node_the_client_sends_to() -> None:
            async with Harness.start(3) as harness:
                client = await harness.client()
                for key in ("acc-1", "acc-2", "acc-3"):
                    placed = await client.placement(account, key)
                    assert placed == await harness.nodes[0].system.placement(account, key)
                    assert (await client.ref(account, key).ask(Where)).node == placed.owner

    def when_a_key_is_released() -> None:
        async def it_ends_the_activation_and_the_next_message_starts_it_from_its_state() -> None:
            async with ActorSystem() as system:
                entry = system.ref(account, "a-1")
                assert await entry.ask(Deposit, 5) == 5

                assert await system.release(account, "a-1")
                assert system.activations() == ()
                assert await entry.ask(Balance) == 5
                assert [row.key for row in system.activations()] == ["a-1"]

        async def it_answers_false_when_the_key_has_no_activation_here() -> None:
            async with ActorSystem() as system:
                assert not await system.release(account, "a-1")

        async def it_lets_the_body_finish_its_message_and_hands_the_queue_on_in_order() -> None:
            latch = LATCHES["r-1"] = Latch()

            async with ActorSystem() as system:
                entry = system.ref(gated, "r-1")
                asking = [asyncio.create_task(entry.ask(Bump)) for _ in range(3)]

                async def one_held_two_queued() -> None:
                    assert [(row.key, row.queued) for row in system.activations()] == [("r-1", 2)]

                await eventually(one_held_two_queued)
                [first] = system.activations()
                releasing = asyncio.create_task(system.release(gated, "r-1"))
                await asyncio.sleep(0.1)
                # The body is still on the message it took, and nothing was answered or dropped meanwhile.
                assert not releasing.done()
                assert not any(task.done() for task in asking)

                latch.released.set()
                assert await releasing
                # Each message is answered once, in the order it came: the first by the body that was on it, the two
                # queued behind it by the activation they started again, from the state the first one wrote.
                assert [await task for task in asking] == [1, 2, 3]
                [again] = system.activations()
                assert again.since > first.since

        async def it_lets_a_key_go_only_on_the_node_that_runs_it() -> None:
            async with Harness.start(3) as harness:
                entry = harness.nodes[0].system.ref(account, "a-1")
                assert await entry.ask(Deposit, 3) == 3
                owner = (await harness.nodes[0].system.placement(account, "a-1")).owner
                running = next(node for node in harness.nodes if node.system.node == owner)
                others = [node for node in harness.nodes if node is not running]

                assert [await node.system.release(account, "a-1") for node in others] == [False, False]
                assert await running.system.release(account, "a-1")
                assert (account.name, "a-1") not in _listed(running)
                assert await entry.ask(Deposit, 4) == 7
                assert (account.name, "a-1") in _listed(running)

    def when_the_system_has_not_entered() -> None:
        async def it_refuses_to_be_read() -> None:
            system = ActorSystem()
            with pytest.raises(RuntimeError):
                system.activations()
            with pytest.raises(RuntimeError):
                await system.placement(account, "a-1")
            with pytest.raises(RuntimeError):
                await system.release(account, "a-1")


async def _placed_where_they_run(harness: Harness, keys: list[str]) -> None:
    """Wait until every node places each key on one owner, which answers for it and is the only node running it."""

    async def agreed() -> None:
        for key in keys:
            placements = {await node.system.placement(account, key) for node in harness.nodes}
            assert len(placements) == 1, f"{key} is placed differently by different nodes: {placements}"
            [placed] = placements
            assert placed.owner is not None
            assert (placed.owner, len(placed.replicas)) == (placed.replicas[0], 3)
            for node in harness.nodes:
                assert (await node.system.ref(account, key).ask(Where)).node == placed.owner
            running = {node.system.node for node in harness.nodes if (account.name, key) in _listed(node)}
            assert running == {placed.owner}

    await eventually(agreed, WITHIN)


def _listed(node: Node) -> set[tuple[str, str]]:
    """The keys active on `node`, as `(actor, key)`."""
    return {(row.actor, row.key) for row in node.system.activations()}


_KEYS = 32
