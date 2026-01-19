"""Distributed rate limiter cluster node.

This is the entrypoint for Docker nodes. Each node joins a cluster
and hosts sharded rate limiter actors.

Run with:
    python cluster_node.py --node-id node-0 --port 9000
    python cluster_node.py --node-id node-1 --port 9001 --seed-nodes node-0:9000
"""

import argparse
import asyncio
import os

from casty import ActorSystem

from rate_limiter import RateLimiter


async def main():
    """Start a cluster node with rate limiters."""
    parser = argparse.ArgumentParser(description="Rate limiter cluster node")
    parser.add_argument("--node-id", required=True, help="Unique node identifier")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument(
        "--seed-nodes",
        default="",
        help="Comma-separated list of seed nodes (host:port)",
    )
    args = parser.parse_args()

    # Parse seed nodes from args or env
    seed_str = args.seed_nodes or os.getenv("SEED_NODES", "")
    seeds = [s.strip() for s in seed_str.split(",") if s.strip()]

    print(f"[{args.node_id}] Starting cluster node on port {args.port}")
    if seeds:
        print(f"[{args.node_id}] Connecting to seeds: {seeds}")

    # Start cluster system
    system = ActorSystem.clustered(
        host="0.0.0.0",
        port=args.port,
        node_id=args.node_id,
        seeds=seeds if seeds else None,
    )

    await system.start()
    print(f"[{args.node_id}] Cluster node started")

    # Wait for cluster formation
    await asyncio.sleep(2.0)

    # Spawn sharded rate limiters
    # Each user gets their own actor (10 req/sec, burst 20)
    limiters = await system.actor(
        RateLimiter,
        name="rate-limiters",
        scope="cluster",
    )

    print(f"[{args.node_id}] Rate limiters spawned")

    # Keep the node running indefinitely
    try:
        print(f"[{args.node_id}] Node is ready. Press Ctrl+C to stop.")
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print(f"\n[{args.node_id}] Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
