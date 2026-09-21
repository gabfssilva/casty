"""A process that uses the cluster without being part of it: `uv run client.py`, with the nodes already up.

A `Client` hosts no actor and keeps no state. It learns the member table from a seed, sends each message straight to
the node that owns the key, and offers the same `ref`, `tell` and `ask` of a node. It is what a web server
or a batch job would hold.
"""

import asyncio

from app import Count, Vote, poll

from casty import Client

OPTIONS = ("vim", "emacs", "helix", "nano")


async def main() -> None:
    async with Client(seeds=("127.0.0.1:7421",)) as client:
        for number in range(30):
            await client.ref(poll, OPTIONS[number % len(OPTIONS)]).ask(Vote, f"voter-{number}")
        for option in OPTIONS:
            tally = await client.ref(poll, option).ask(Count)
            print(f"{option}: {tally.votes} votes, kept on {tally.node.address}")


asyncio.run(main())
