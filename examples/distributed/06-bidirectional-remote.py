"""
Bidirectional Remote Communication Example

Demonstrates that both sides of a remote connection can Expose and Lookup actors.

Run with:
    uv run python examples/distributed/06-bidirectional-remote.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox
from casty.serializable import serializable
from casty.remote import (
    remote, Listen, Connect, Listening, Connected,
    Expose, Lookup, LookupResult,
)


@serializable
@dataclass
class Ping:
    message: str


@serializable
@dataclass
class Pong:
    message: str


@actor
async def ping_actor(name: str, *, mailbox: Mailbox[Ping]):
    async for msg, ctx in mailbox:
        print(f"[{name}] Received: '{msg.message}'")
        await ctx.reply(Pong(message=f"pong from {name}"))


async def main():
    print("=== Bidirectional Remote Communication Test ===\n")

    async with ActorSystem() as system_a, ActorSystem() as system_b:
        # Start remote managers
        remote_a = await system_a.actor(remote(), name="remote")
        remote_b = await system_b.actor(remote(), name="remote")

        # Node A listens
        listen_result = await remote_a.ask(Listen(port=0))
        assert isinstance(listen_result, Listening)
        port = listen_result.address[1]
        print(f"[Node A] Listening on port {port}")

        # Node A exposes its ping actor (directly on remote manager!)
        ping_a = await system_a.actor(ping_actor("Node-A"), name="ping-a")
        await remote_a.ask(Expose(ref=ping_a, name="ping-a"))
        print("[Node A] Exposed 'ping-a' actor")

        # Node B connects to Node A
        connect_result = await remote_b.ask(Connect(host="127.0.0.1", port=port))
        assert isinstance(connect_result, Connected)
        print(f"[Node B] Connected to Node A (peer_id: {connect_result.peer_id})")

        # Node B exposes its ping actor
        ping_b = await system_b.actor(ping_actor("Node-B"), name="ping-b")
        await remote_b.ask(Expose(ref=ping_b, name="ping-b"))
        print("[Node B] Exposed 'ping-b' actor")

        # Give time for connection to stabilize
        await asyncio.sleep(0.1)

        # Test 1: Node B looks up Node A's actor and sends message
        print("\n--- Test 1: Node B -> Node A ---")
        lookup_result = await remote_b.ask(Lookup("ping-a"))
        assert isinstance(lookup_result, LookupResult)

        if lookup_result.ref:
            print(f"[Node B] Found 'ping-a': {lookup_result.ref}")
            result = await lookup_result.ref.ask(Ping(message="hello from B"))
            print(f"[Node B] Got reply: {result}")
        else:
            print("[Node B] ERROR: Could not find 'ping-a' actor!")

        # Test 2: Node A looks up Node B's actor (TRUE BIDIRECTIONAL TEST)
        print("\n--- Test 2: Node A -> Node B (bidirectional test) ---")
        lookup_result = await remote_a.ask(Lookup("ping-b"))
        assert isinstance(lookup_result, LookupResult)

        if lookup_result.ref:
            print(f"[Node A] Found 'ping-b': {lookup_result.ref}")
            result = await lookup_result.ref.ask(Ping(message="hello from A"))
            print(f"[Node A] Got reply: {result}")
            print("\n✓ BIDIRECTIONAL COMMUNICATION WORKS!")
        else:
            print("[Node A] ERROR: Could not find 'ping-b' actor on Node B!")
            print("\n✗ BIDIRECTIONAL COMMUNICATION FAILED")

        print("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
