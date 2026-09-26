import asyncio
from datetime import timedelta

import pytest

from casty import ActorStats, ActorSystem
from tests.app import LATCHES, Bump, Deposit, Latch, Touch, account, gated, touched
from tests.cluster import Harness
from tests.support import eventually


def describe_stats() -> None:
    def when_keys_come_and_go() -> None:
        async def it_counts_the_keys_active_until_they_idle_out() -> None:
            async with ActorSystem(idle_after=timedelta(seconds=1)) as system:
                for index in range(3):
                    assert await system.ref(touched, f"t-{index}").ask(Touch())
                    assert system.stats().actors[touched.name].active == index + 1

                async def every_key_idled_out() -> None:
                    assert system.stats().actors[touched.name] == ActorStats(active=0, queued=0, deepest=0)

                await eventually(every_key_idled_out)

    def when_a_body_is_busy() -> None:
        async def it_shows_the_messages_waiting_behind_it_and_the_asks_waiting_for_them() -> None:
            latch = LATCHES["g-1"] = LATCHES["g-2"] = Latch()

            async with ActorSystem() as system:
                asking = [asyncio.create_task(system.ref(gated, "g-1").ask(Bump())) for _ in range(4)]
                asking += [asyncio.create_task(system.ref(gated, "g-2").ask(Bump())) for _ in range(2)]

                # Each body holds the message it took, and the others wait in its mailbox.
                async def the_mailboxes_hold_the_rest() -> None:
                    stats = system.stats()
                    assert stats.actors[gated.name] == ActorStats(active=2, queued=4, deepest=3)
                    assert stats.asks_in_flight == 6

                await eventually(the_mailboxes_hold_the_rest)
                latch.released.set()
                assert sorted(await asyncio.gather(*asking)) == [1, 1, 2, 2, 3, 4]

                stats = system.stats()
                assert (stats.actors[gated.name].queued, stats.asks_in_flight) == (0, 0)

    def when_it_runs_alone() -> None:
        async def it_counts_every_write_as_confirmed_and_has_no_connection() -> None:
            async with ActorSystem() as system:
                entry = system.ref(account, "a-1")
                for amount in (1, 2, 3):
                    await entry.ask(Deposit(amount))

                stats = system.stats()
                assert (stats.writes_confirmed, stats.writes_failed) == (3, 0)
                assert (stats.connections, stats.bytes_sent, stats.bytes_received) == (0, 0, 0)

        def it_cannot_be_read_before_the_system_enters() -> None:
            with pytest.raises(RuntimeError):
                ActorSystem().stats()

    def when_it_is_a_node_of_a_cluster() -> None:
        async def it_counts_the_writes_of_the_keys_it_owns_and_what_its_connections_carry() -> None:
            async with Harness.start(3) as harness:
                systems = [node.system for node in harness.nodes]
                entry = systems[0].ref(account, "a-1")
                assert [await entry.ask(Deposit(amount)) for amount in (1, 2, 3)] == [1, 3, 6]
                client = await harness.client()
                assert await client.ref(account, "a-1").ask(Deposit(4)) == 10

                every = [system.stats() for system in systems]
                # Only the owner writes, once per deposit: taking the key over is not a write.
                assert sorted(stats.writes_confirmed for stats in every) == [0, 0, 4]
                assert [stats.writes_failed for stats in every] == [0, 0, 0]
                assert all(stats.connections >= 1 for stats in every)
                assert all(stats.bytes_sent > 0 and stats.bytes_received > 0 for stats in every)
                # A replica may not have met the type yet, and a type a node has not met is not listed there.
                assert sum(stats.actors[account.name].active for stats in every if account.name in stats.actors) == 1

                # A client hosts no key and owns no write; what it has is its connections and its asks.
                reached = client.stats()
                assert reached.actors[account.name] == ActorStats(active=0, queued=0, deepest=0)
                assert (reached.writes_confirmed, reached.asks_in_flight) == (0, 0)
                assert reached.connections >= 1
                assert reached.bytes_sent > 0 and reached.bytes_received > 0
