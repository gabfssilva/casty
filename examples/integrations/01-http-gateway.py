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

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class Increment:
    amount: int = 1


@dataclass
class Decrement:
    amount: int = 1


@dataclass
class GetCount:
    pass


@dataclass
class Reset:
    pass


CounterMsg = Increment | Decrement | GetCount | Reset


@actor
async def counter(*, mailbox: Mailbox[CounterMsg]):
    count = 0
    total_operations = 0

    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                count += amount
                total_operations += 1
                print(f"[Counter] Incremented by {amount} -> {count}")
                await ctx.reply(count)

            case Decrement(amount):
                count -= amount
                total_operations += 1
                print(f"[Counter] Decremented by {amount} -> {count}")
                await ctx.reply(count)

            case GetCount():
                await ctx.reply({
                    "count": count,
                    "total_operations": total_operations
                })

            case Reset():
                old_count = count
                count = 0
                total_operations += 1
                print(f"[Counter] Reset from {old_count} -> 0")
                await ctx.reply({"reset": True, "previous_count": old_count})


class HttpGateway:
    def __init__(self, counter_ref: LocalActorRef[CounterMsg]):
        self.counter = counter_ref
        self.request_timeout = 5.0

    async def handle_increment(self, request: web.Request) -> web.Response:
        amount = 1

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
        try:
            result = await self.counter.ask(GetCount(), timeout=self.request_timeout)
            return web.json_response(result)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Request timeout"}, status=504)

    async def handle_reset(self, request: web.Request) -> web.Response:
        try:
            result = await self.counter.ask(Reset(), timeout=self.request_timeout)
            return web.json_response(result)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Request timeout"}, status=504)

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "healthy"})


async def main():
    print("=" * 60)
    print("Casty HTTP Gateway Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        counter_ref = await system.actor(counter(), name="counter")
        print("[System] Counter actor created")

        gateway = HttpGateway(counter_ref)

        app = web.Application()
        app.router.add_get("/health", gateway.handle_health)
        app.router.add_get("/count", gateway.handle_get_count)
        app.router.add_post("/increment", gateway.handle_increment)
        app.router.add_post("/decrement", gateway.handle_decrement)
        app.router.add_post("/reset", gateway.handle_reset)

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

        shutdown_event = asyncio.Event()

        def signal_handler():
            print("\n[System] Shutdown signal received...")
            shutdown_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        await shutdown_event.wait()

        print("[HTTP] Stopping server...")
        await runner.cleanup()
        print("[System] Server stopped gracefully")


if __name__ == "__main__":
    asyncio.run(main())
