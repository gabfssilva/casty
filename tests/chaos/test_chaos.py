"""A chaos run as a test. The run prints its report, which names the seed, the steps up to a failure and the command
that replays them; pytest shows it with the failure even without `-s`."""

from __future__ import annotations

import os

from tests.chaos.run import arranged, destination, run


def describe_chaos() -> None:
    async def it_keeps_every_invariant_through_the_faults_of_its_seed() -> None:
        settings, schedule = arranged(os.environ)
        report = await run(settings, schedule, destination(os.environ, settings))
        assert report.failure is None, f"{report.failure}\nreplay: {report.replay}"
