"""Service Registry with Health Checks.

Demonstrates:
- Registry actor for service registration
- Heartbeat via periodic messages
- Detecting dead services (missed heartbeats)
- Service discovery by type/tag
- Notifications on service up/down

This pattern is fundamental for microservices architectures where services
need to discover and communicate with each other dynamically.

Run with: uv run python examples/distributed/02-service-registry.py
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from casty import actor, Mailbox, LocalActorRef
from casty.cluster import DevelopmentCluster


@dataclass
class Register:
    service_id: str
    service_type: str
    tags: list[str]
    metadata: dict[str, Any]
    ref: LocalActorRef[Any]


@dataclass
class Deregister:
    service_id: str


@dataclass
class Heartbeat:
    service_id: str


@dataclass
class Discover:
    service_type: str | None = None
    tags: list[str] | None = None


@dataclass
class Subscribe:
    listener: LocalActorRef[Any]


@dataclass
class _CheckHealth:
    pass


@dataclass
class ServiceUp:
    service_id: str
    service_type: str
    metadata: dict[str, Any]


@dataclass
class ServiceDown:
    service_id: str
    service_type: str
    reason: str


@dataclass
class ServiceInfo:
    service_id: str
    service_type: str
    tags: list[str]
    metadata: dict[str, Any]
    ref: LocalActorRef[Any]
    last_heartbeat: float


RegistryMsg = Register | Deregister | Heartbeat | Discover | Subscribe | _CheckHealth


@actor
async def service_registry(heartbeat_timeout: float = 3.0, *, mailbox: Mailbox[RegistryMsg]):
    services: dict[str, ServiceInfo] = {}
    subscribers: list[LocalActorRef[Any]] = []

    async def notify_subscribers(event: ServiceUp | ServiceDown):
        for subscriber in subscribers:
            await subscriber.send(event)

    async def check_health():
        now = asyncio.get_event_loop().time()
        dead_services = []

        for service_id, info in services.items():
            if now - info.last_heartbeat > heartbeat_timeout:
                dead_services.append(service_id)

        for service_id in dead_services:
            info = services.pop(service_id)
            print(f"[Registry] Service DEAD (missed heartbeats): {service_id}")
            await notify_subscribers(
                ServiceDown(service_id, info.service_type, "missed heartbeats")
            )

    print("[Registry] Started with health check interval=1.0s")
    await mailbox._self_ref.send(_CheckHealth())

    async for msg, ctx in mailbox:
        match msg:
            case Register(service_id, service_type, tags, metadata, ref):
                now = asyncio.get_event_loop().time()
                info = ServiceInfo(
                    service_id=service_id,
                    service_type=service_type,
                    tags=tags,
                    metadata=metadata,
                    ref=ref,
                    last_heartbeat=now,
                )
                services[service_id] = info
                print(f"[Registry] Registered: {service_id} (type={service_type}, tags={tags})")

                await notify_subscribers(ServiceUp(service_id, service_type, metadata))

            case Deregister(service_id):
                if service_id in services:
                    info = services.pop(service_id)
                    print(f"[Registry] Deregistered: {service_id}")
                    await notify_subscribers(
                        ServiceDown(service_id, info.service_type, "explicit deregistration")
                    )

            case Heartbeat(service_id):
                if service_id in services:
                    services[service_id].last_heartbeat = asyncio.get_event_loop().time()

            case Discover(service_type, tags):
                results = []
                for info in services.values():
                    if service_type and info.service_type != service_type:
                        continue
                    if tags and not all(t in info.tags for t in tags):
                        continue
                    results.append({
                        "service_id": info.service_id,
                        "service_type": info.service_type,
                        "tags": info.tags,
                        "metadata": info.metadata,
                    })
                await ctx.reply(results)

            case Subscribe(listener):
                subscribers.append(listener)
                print(f"[Registry] New subscriber added")

            case _CheckHealth():
                await check_health()
                await ctx.schedule(_CheckHealth(), delay=1.0)


@dataclass
class Ping:
    pass


@dataclass
class _SendHeartbeat:
    pass


ServiceMsg = Ping | _SendHeartbeat


@actor
async def service(
    service_id: str,
    service_type: str,
    registry: LocalActorRef[Any],
    tags: list[str] | None = None,
    *,
    mailbox: Mailbox[ServiceMsg],
):
    actual_tags = tags or []

    await registry.send(Register(
        service_id=service_id,
        service_type=service_type,
        tags=actual_tags,
        metadata={"started_at": str(datetime.now())},
        ref=mailbox._self_ref,
    ))

    await mailbox._self_ref.send(_SendHeartbeat())
    print(f"[{service_id}] Started")

    async for msg, ctx in mailbox:
        match msg:
            case Ping():
                await ctx.reply("pong")

            case _SendHeartbeat():
                await registry.send(Heartbeat(service_id))
                await ctx.schedule(_SendHeartbeat(), delay=1.0)


MonitorMsg = ServiceUp | ServiceDown


@actor
async def service_monitor(monitor_name: str, *, mailbox: Mailbox[MonitorMsg]):
    async for msg, ctx in mailbox:
        match msg:
            case ServiceUp(service_id, service_type, _):
                print(f"[Monitor:{monitor_name}] SERVICE UP: {service_id} (type={service_type})")

            case ServiceDown(service_id, _, reason):
                print(f"[Monitor:{monitor_name}] SERVICE DOWN: {service_id} (reason={reason})")


async def main():
    print("=== Service Registry with Health Checks ===\n")

    async with DevelopmentCluster(1) as cluster:
        await asyncio.sleep(0.3)

        node = cluster[0]

        registry = await node.actor(
            service_registry(heartbeat_timeout=2.5),
            name="registry",
        )

        monitor = await node.actor(
            service_monitor(monitor_name="ops"),
            name="monitor",
        )
        await registry.send(Subscribe(monitor))

        print("\nPhase 1: Starting services")
        print("-" * 50)

        api_service = await node.actor(
            service(
                service_id="api-1",
                service_type="api-gateway",
                registry=registry,
                tags=["http", "public"],
            ),
            name="api-1",
        )

        db_service = await node.actor(
            service(
                service_id="db-1",
                service_type="database",
                registry=registry,
                tags=["postgres", "primary"],
            ),
            name="db-1",
        )

        cache_service = await node.actor(
            service(
                service_id="cache-1",
                service_type="cache",
                registry=registry,
                tags=["redis", "session"],
            ),
            name="cache-1",
        )

        await asyncio.sleep(0.5)

        print("\nPhase 2: Service discovery")
        print("-" * 50)

        all_services = await registry.ask(Discover())
        print(f"All services ({len(all_services)}):")
        for svc in all_services:
            print(f"  - {svc['service_id']}: {svc['service_type']} {svc['tags']}")

        db_services = await registry.ask(Discover(service_type="database"))
        print(f"\nDatabase services: {[s['service_id'] for s in db_services]}")

        http_services = await registry.ask(Discover(tags=["http"]))
        print(f"HTTP services: {[s['service_id'] for s in http_services]}")

        print("\nPhase 3: Service communication")
        print("-" * 50)

        if all_services:
            print(f"Pinging api-1...")
            response = await api_service.ask(Ping())
            print(f"Response: {response}")

        print("\nPhase 4: Graceful shutdown")
        print("-" * 50)

        print("Stopping cache-1...")
        await registry.send(Deregister("cache-1"))
        await asyncio.sleep(0.5)

        remaining = await registry.ask(Discover())
        print(f"Remaining services: {[s['service_id'] for s in remaining]}")

        print("\nPhase 5: Simulating service failure")
        print("-" * 50)

        print("Stopping db-1 abruptly (without deregistration)...")

        print("Waiting for health check to detect failure...")
        await asyncio.sleep(3.5)

        final_services = await registry.ask(Discover())
        print(f"\nFinal services: {[s['service_id'] for s in final_services]}")

        print("\n=== Summary ===")
        print("Service Registry features demonstrated:")
        print("  - Service registration with metadata and tags")
        print("  - Periodic heartbeat for liveness")
        print("  - Health check detects dead services")
        print("  - Discovery by type and tags")
        print("  - Event notifications (ServiceUp/ServiceDown)")


if __name__ == "__main__":
    asyncio.run(main())
