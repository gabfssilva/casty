"""Single-node rate limiter demo.

Run with:
    uv run python single_node.py

Demonstrates:
- Token bucket rate limiting (10 req/sec, burst of 20)
- Sharded actors routed by user ID
- Burst capacity and refill behavior
"""

import asyncio

from casty import ActorSystem

from rate_limiter import CheckToken, GetStatus, RateLimiter


async def main():
    """Run single-node rate limiter demo."""
    async with ActorSystem.clustered("127.0.0.1", 0) as system:
        # Spawn sharded rate limiters
        # Each user gets their own limiter (10 req/sec, burst 20)
        limiters = await system.actor(
            RateLimiter,
            name="rate-limiters",
            scope="cluster",
        )

        print("=" * 60)
        print("SINGLE-NODE RATE LIMITER DEMO")
        print("=" * 60)
        print("\nRate: 10 tokens/sec | Burst capacity: 20 tokens")
        print("Users: alice, bob, charlie")
        print("Each user tries 30 requests\n")

        # Simulate traffic from multiple users
        # Burst 30 requests quickly to exhaust the burst capacity
        users = ["alice", "bob", "charlie"]
        for user in users:
            print(f"\n{user}:")
            allowed_count = 0
            denied_count = 0

            for i in range(30):
                # Ask for a token
                allowed = await limiters[user].ask(CheckToken())

                if allowed:
                    print("✓", end="", flush=True)
                    allowed_count += 1
                else:
                    print("✗", end="", flush=True)
                    denied_count += 1

                # Very small delay (no time for refill in burst)
                await asyncio.sleep(0.001)

            print(f"\n  Result: {allowed_count} allowed, {denied_count} denied")

        # Print final statistics
        print("\n" + "=" * 60)
        print("FINAL STATUS")
        print("=" * 60)

        for user in users:
            status = await limiters[user].ask(GetStatus())
            print(f"\n{user}:")
            print(f"  Tokens: {status['tokens']:.2f} / {status['capacity']}")
            print(f"  Allowed: {status['allowed']}")
            print(f"  Denied: {status['denied']}")
            print(f"  Total: {status['total_requests']}")


if __name__ == "__main__":
    asyncio.run(main())
