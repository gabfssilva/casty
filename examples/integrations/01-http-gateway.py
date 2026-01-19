#!/usr/bin/env python
"""HTTP Gateway Example - Actor as HTTP Backend.

Demonstrates:
- Integration with aiohttp web server
- HTTP request -> actor message translation
- Using ask() with timeout for request-response
- Actor maintaining state between HTTP requests
- Graceful shutdown handling

This example creates a simple counter service with REST endpoints:
- POST /increment - Increment the counter (optionally with amount)
- POST /decrement - Decrement the counter (optionally with amount)
- GET /count - Get the current count
- POST /reset - Reset the counter to zero

Required dependencies:
    uv add aiohttp

Run with:
    uv run python examples/integrations/01-http-gateway.py

Then test with:
    curl http://localhost:8080/count
    curl -X POST http://localhost:8080/increment
    curl -X POST http://localhost:8080/increment -d '{"amount": 5}'
    curl http://localhost:8080/count
"""

import asyncio
import json
import signal
from dataclasses import dataclass

from aiohttp import web

from casty import Actor, ActorSystem, Context, on


# --- Messages ---

@dataclass
class Increment:
    """Increment counter by amount."""
    amount: int = 1


@dataclass
class Decrement:
    """Decrement counter by amount."""
    amount: int = 1


@dataclass
class GetCount:
    """Query current count."""
    pass


@dataclass
class Reset:
    """Reset counter to zero."""
    pass


# --- Actor ---

class CounterActor(Actor[Increment | Decrement | GetCount | Reset]):
    """Counter actor that maintains state between HTTP requests.

    This actor is thread-safe by design - all state modifications
    happen through message processing, one message at a time.
    """

    def __init__(self):
        self.count = 0
        self.total_operations = 0

    @on(Increment)
    async def handle_increment(self, msg: Increment, ctx: Context):
        self.count += msg.amount
        self.total_operations += 1
        print(f"[Counter] Incremented by {msg.amount} -> {self.count}")
        await ctx.reply(self.count)

    @on(Decrement)
    async def handle_decrement(self, msg: Decrement, ctx: Context):
        self.count -= msg.amount
        self.total_operations += 1
        print(f"[Counter] Decremented by {msg.amount} -> {self.count}")
        await ctx.reply(self.count)

    @on(GetCount)
    async def handle_get_count(self, msg: GetCount, ctx: Context):
        await ctx.reply({
            "count": self.count,
            "total_operations": self.total_operations
        })

    @on(Reset)
    async def handle_reset(self, msg: Reset, ctx: Context):
        old_count = self.count
        self.count = 0
        self.total_operations += 1
        print(f"[Counter] Reset from {old_count} -> 0")
        await ctx.reply({"reset": True, "previous_count": old_count})


# --- HTTP Handlers ---

class HttpGateway:
    """HTTP gateway that translates HTTP requests to actor messages."""

    def __init__(self, counter_ref):
        self.counter = counter_ref
        self.request_timeout = 5.0  # seconds

    async def handle_increment(self, request: web.Request) -> web.Response:
        """POST /increment - Increment the counter."""
        amount = 1

        # Parse optional JSON body for amount
        if request.body_exists:
            try:
                body = await request.json()
                amount = body.get("amount", 1)
            except json.JSONDecodeError:
                pass

        try:
            result = await self.counter.ask(Increment(amount), timeout=self.request_timeout)
            return web.json_response({"count": result})
        except asyncio.TimeoutError:
            return web.json_response({"error": "Request timeout"}, status=504)

    async def handle_decrement(self, request: web.Request) -> web.Response:
        """POST /decrement - Decrement the counter."""
        amount = 1

        if request.body_exists:
            try:
                body = await request.json()
                amount = body.get("amount", 1)
            except json.JSONDecodeError:
                pass

        try:
            result = await self.counter.ask(Decrement(amount), timeout=self.request_timeout)
            return web.json_response({"count": result})
        except asyncio.TimeoutError:
            return web.json_response({"error": "Request timeout"}, status=504)

    async def handle_get_count(self, request: web.Request) -> web.Response:
        """GET /count - Get the current count."""
        try:
            result = await self.counter.ask(GetCount(), timeout=self.request_timeout)
            return web.json_response(result)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Request timeout"}, status=504)

    async def handle_reset(self, request: web.Request) -> web.Response:
        """POST /reset - Reset the counter."""
        try:
            result = await self.counter.ask(Reset(), timeout=self.request_timeout)
            return web.json_response(result)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Request timeout"}, status=504)

    async def handle_health(self, request: web.Request) -> web.Response:
        """GET /health - Health check endpoint."""
        return web.json_response({"status": "healthy"})


async def main():
    print("=" * 60)
    print("Casty HTTP Gateway Example")
    print("=" * 60)
    print()

    # Create the actor system and counter actor
    async with ActorSystem() as system:
        counter = await system.actor(CounterActor, name="counter")
        print("[System] Counter actor created")

        # Create HTTP gateway
        gateway = HttpGateway(counter)

        # Setup aiohttp web application
        app = web.Application()
        app.router.add_get("/health", gateway.handle_health)
        app.router.add_get("/count", gateway.handle_get_count)
        app.router.add_post("/increment", gateway.handle_increment)
        app.router.add_post("/decrement", gateway.handle_decrement)
        app.router.add_post("/reset", gateway.handle_reset)

        # Create runner for graceful shutdown
        runner = web.AppRunner(app)
        await runner.setup()

        site = web.TCPSite(runner, "localhost", 8080)
        await site.start()

        print()
        print("[HTTP] Server running at http://localhost:8080")
        print()
        print("Endpoints:")
        print("  GET  /health     - Health check")
        print("  GET  /count      - Get current count")
        print("  POST /increment  - Increment counter (optional: {\"amount\": N})")
        print("  POST /decrement  - Decrement counter (optional: {\"amount\": N})")
        print("  POST /reset      - Reset counter to zero")
        print()
        print("Press Ctrl+C to stop...")
        print()

        # Setup graceful shutdown
        shutdown_event = asyncio.Event()

        def signal_handler():
            print("\n[System] Shutdown signal received...")
            shutdown_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        # Wait for shutdown signal
        await shutdown_event.wait()

        # Graceful shutdown
        print("[HTTP] Stopping server...")
        await runner.cleanup()
        print("[System] Server stopped gracefully")


if __name__ == "__main__":
    asyncio.run(main())
