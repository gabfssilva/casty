"""Drives the cluster from outside and reports what it sees: `docker compose run --rm client`.

Every round hits each page once, from many callers at a time, and then asks each page where it lives. Scale the
cluster up or down between rounds and watch the pages move while the counts keep growing.
"""

import asyncio
import os
from collections import Counter

from app import Hit, Locate, Seen, page

from casty import Client, Unavailable

PAGES = tuple(f"/articles/{index}" for index in range(500))
ROUNDS = int(os.environ.get("ROUNDS", "5"))


async def locate(client: Client, key: str, /) -> Seen | None:
    """Hit the page and ask where it is. A page that is changing hands is skipped this round, not waited for."""
    try:
        async with asyncio.timeout(5):
            await client.ref(page, key).ask(Hit())
            return await client.ref(page, key).ask(Locate())
    except (Unavailable, TimeoutError):
        return None


async def main() -> None:
    async with Client(seeds=tuple(os.environ["SEEDS"].split(","))) as client:
        for number in range(1, ROUNDS + 1):
            found = await asyncio.gather(*(locate(client, key) for key in PAGES))
            seen = [answer for answer in found if answer is not None]
            if not seen:
                print(f"round {number}: 0/{len(PAGES)} pages answered; counts unavailable", flush=True)
                await asyncio.sleep(2)
                continue
            per_node = Counter(answer.node.address for answer in seen)
            busiest, idlest = max(per_node.values()), min(per_node.values())
            print(
                f"round {number}: {len(seen)}/{len(PAGES)} pages answered from {len(per_node)} nodes "
                f"({idlest} to {busiest} pages each), {sum(answer.hits for answer in seen)} hits kept",
                flush=True,
            )
            await asyncio.sleep(2)


asyncio.run(main())
