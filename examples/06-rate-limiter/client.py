"""Load generator client that connects to the rate limiter cluster.

Simulates 1000 users across a 10-node cluster (50 requests per user).

Run with:
    docker-compose up

The client connects to the cluster and generates traffic from 1000 concurrent users.
"""

import asyncio
import time

from casty import ActorSystem

from rate_limiter import CheckToken, GetStatus, RateLimiter


async def simulate_user(limiters, user_id: str, num_requests: int, semaphore: asyncio.Semaphore):
    """Simulate a user making requests with concurrency control.

    Args:
        limiters: Reference to sharded rate limiters
        user_id: User identifier
        num_requests: Number of requests to make
        semaphore: Semaphore to limit concurrent users

    Returns:
        Tuple of (user_id, allowed_count, denied_count)
    """
    async with semaphore:
        allowed = 0
        denied = 0

        for i in range(num_requests):
            try:
                result = await limiters[user_id].ask(CheckToken(), timeout=2.0)
                if result:
                    allowed += 1
                else:
                    denied += 1
            except asyncio.TimeoutError:
                denied += 1

            # Small delay between requests (50 req/sec per user)
            await asyncio.sleep(0.02)

        return user_id, allowed, denied


async def get_final_status(limiters, user_id: str) -> dict:
    """Get final status of a user's rate limiter."""
    return await limiters[user_id].ask(GetStatus(), timeout=2.0)


async def main():
    """Connect to cluster and simulate traffic."""
    print("=" * 70)
    print("DISTRIBUTED RATE LIMITER CLIENT")
    print("=" * 70)
    print()

    # Connect to cluster as a regular node
    print("Connecting to cluster...")
    system = ActorSystem.clustered(
        host="0.0.0.0",
        port=9999,  # Client port
        node_id="client",
        seeds=["cluster-node:9000"],  # Connect to seed node (first replica)
    )

    await system.start()
    print("✓ Connected to cluster")

    # Wait for cluster discovery
    await asyncio.sleep(2.0)

    # Spawn sharded rate limiters (same config as nodes)
    # This gives us references that route through the cluster
    limiters = await system.actor(
        RateLimiter,
        name="rate-limiters",
        scope="cluster",
    )
    print("✓ Got reference to rate limiters")
    print()

    # Simulate 1000 users making requests
    print("Simulating 1000 users making requests...")
    print(f"Rate: 10 tokens/sec | Burst: 20 tokens | Requests per user: 50")
    print()

    start_time = time.time()

    # Create semaphore to limit concurrent users (50 at a time)
    semaphore = asyncio.Semaphore(50)

    # Create tasks for ALL 1000 users (controlled by semaphore)
    users = [f"user-{i}" for i in range(1000)]
    tasks = [simulate_user(limiters, user, 50, semaphore) for user in users]

    print("Processing 1000 users with 50 concurrent limit...")
    results = await asyncio.gather(*tasks)

    elapsed = time.time() - start_time

    # Get final status for sample users (every 100th)
    print()
    print("Fetching final statistics from sample users...")
    sample_users = [f"user-{i}" for i in range(0, 1000, 100)]
    status_tasks = [get_final_status(limiters, user) for user in sample_users]
    statuses = await asyncio.gather(*status_tasks)

    # Print summary
    print()
    print("=" * 70)
    print("RATE LIMIT SUMMARY - 1000 USERS")
    print("=" * 70)
    print()

    total_allowed = 0
    total_denied = 0

    # Aggregate all results
    for user, allowed, denied in results:
        total_allowed += allowed
        total_denied += denied

    total_requests = total_allowed + total_denied
    rate_limited_pct = (total_denied / total_requests * 100) if total_requests > 0 else 0

    print(f"Total Requests: {total_requests:,}")
    print(f"  Allowed: {total_allowed:,} ({100 - rate_limited_pct:.1f}%)")
    print(f"  Denied:  {total_denied:,} ({rate_limited_pct:.1f}%)")
    print()

    # Show sample user stats
    print("Sample User Statistics (every 100th user):")
    print("-" * 70)
    for user, status in zip(sample_users, statuses):
        print(f"{user:10} | Tokens: {status['tokens']:6.2f} | "
              f"Requests: {status['total_requests']:3} "
              f"(allowed: {status['allowed']:2}, denied: {status['denied']:2})")

    print()
    print("-" * 70)
    print(f"Elapsed time: {elapsed:.2f}s")
    print(f"Throughput: {total_requests / elapsed:,.1f} req/s")
    print(f"Average per user: {total_requests / 1000:.1f} req")
    print()

    print("=" * 70)
    print("Large-scale load test completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
