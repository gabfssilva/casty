"""Service Registry with Health Checks.

Demonstrates:
- Registry actor for service registration
- Heartbeat via periodic messages (Ticker)
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

from casty import Actor, Context, LocalActorRef
from casty.cluster import DevelopmentCluster


# --- Registry Messages ---

@dataclass
class Register:
    """Register a service with the registry."""
    service_id: str
    service_type: str
    tags: list[str]
    metadata: dict[str, Any]
    ref: LocalActorRef[Any]


@dataclass
class Deregister:
    """Explicitly deregister a service."""
    service_id: str


@dataclass
class Heartbeat:
    """Service heartbeat to indicate liveness."""
    service_id: str


@dataclass
class Discover:
    """Discover services by type and/or tags."""
    service_type: str | None = None
    tags: list[str] | None = None


@dataclass
class Subscribe:
    """Subscribe to service up/down notifications."""
    listener: LocalActorRef[Any]


@dataclass
class _CheckHealth:
    """Internal: periodic health check trigger."""
    pass


# --- Notification Messages ---

@dataclass
class ServiceUp:
    """Notification: a service came online."""
    service_id: str
    service_type: str
    metadata: dict[str, Any]


@dataclass
class ServiceDown:
    """Notification: a service went offline."""
    service_id: str
    service_type: str
    reason: str


# --- Service Info ---

@dataclass
class ServiceInfo:
    """Stored information about a registered service."""
    service_id: str
    service_type: str
    tags: list[str]
    metadata: dict[str, Any]
    ref: LocalActorRef[Any]
    last_heartbeat: float


# --- Registry Actor ---

class ServiceRegistry(Actor[Register | Deregister | Heartbeat | Discover | Subscribe | _CheckHealth]):
    """Central registry for service discovery with health checking.

    Services register themselves and send periodic heartbeats.
    If a service misses too many heartbeats, it's considered dead.
    Subscribers are notified of service up/down events.
    """

    def __init__(self, heartbeat_timeout: float = 3.0):
        self.services: dict[str, ServiceInfo] = {}
        self.subscribers: list[LocalActorRef[Any]] = []
        self.heartbeat_timeout = heartbeat_timeout
        self._check_task_id: str | None = None

    async def on_start(self):
        self._check_task_id = await self._ctx.tick(_CheckHealth(), interval=1.0)
        print("[Registry] Started with health check interval=1.0s")

    async def on_stop(self):
        if self._check_task_id:
            await self._ctx.cancel_tick(self._check_task_id)

    async def receive(self, msg, ctx: Context):
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
                self.services[service_id] = info
                print(f"[Registry] Registered: {service_id} (type={service_type}, tags={tags})")

                await self._notify_subscribers(ServiceUp(service_id, service_type, metadata))

            case Deregister(service_id):
                if service_id in self.services:
                    info = self.services.pop(service_id)
                    print(f"[Registry] Deregistered: {service_id}")
                    await self._notify_subscribers(
                        ServiceDown(service_id, info.service_type, "explicit deregistration")
                    )

            case Heartbeat(service_id):
                if service_id in self.services:
                    self.services[service_id].last_heartbeat = asyncio.get_event_loop().time()

            case Discover(service_type, tags):
                results = []
                for info in self.services.values():
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
                self.subscribers.append(listener)
                print(f"[Registry] New subscriber added")

            case _CheckHealth():
                await self._check_health()

    async def _check_health(self):
        """Check for dead services (missed heartbeats)."""
        now = asyncio.get_event_loop().time()
        dead_services = []

        for service_id, info in self.services.items():
            if now - info.last_heartbeat > self.heartbeat_timeout:
                dead_services.append(service_id)

        for service_id in dead_services:
            info = self.services.pop(service_id)
            print(f"[Registry] Service DEAD (missed heartbeats): {service_id}")
            await self._notify_subscribers(
                ServiceDown(service_id, info.service_type, "missed heartbeats")
            )

    async def _notify_subscribers(self, event: ServiceUp | ServiceDown):
        """Notify all subscribers of a service event."""
        for subscriber in self.subscribers:
            await subscriber.send(event)


# --- Service Actor (example) ---

@dataclass
class Ping:
    pass


@dataclass
class _SendHeartbeat:
    """Internal: trigger heartbeat send."""
    pass


class Service(Actor[Ping | _SendHeartbeat]):
    """Example service that registers itself and sends heartbeats."""

    def __init__(
        self,
        service_id: str,
        service_type: str,
        registry: LocalActorRef[Any],
        tags: list[str] | None = None,
    ):
        self.service_id = service_id
        self.service_type = service_type
        self.registry = registry
        self.tags = tags or []
        self._heartbeat_task_id: str | None = None

    async def on_start(self):
        # Register with the registry
        await self.registry.send(Register(
            service_id=self.service_id,
            service_type=self.service_type,
            tags=self.tags,
            metadata={"started_at": str(datetime.now())},
            ref=self._ctx.self_ref,
        ))

        # Start periodic heartbeat
        self._heartbeat_task_id = await self._ctx.tick(_SendHeartbeat(), interval=1.0)
        print(f"[{self.service_id}] Started")

    async def on_stop(self):
        if self._heartbeat_task_id:
            await self._ctx.cancel_tick(self._heartbeat_task_id)
        await self.registry.send(Deregister(self.service_id))
        print(f"[{self.service_id}] Stopped")

    async def receive(self, msg, ctx: Context):
        match msg:
            case Ping():
                await ctx.reply("pong")

            case _SendHeartbeat():
                await self.registry.send(Heartbeat(self.service_id))


# --- Subscriber Actor (monitoring) ---

class ServiceMonitor(Actor[ServiceUp | ServiceDown]):
    """Actor that monitors service up/down events."""

    def __init__(self, monitor_name: str):
        self.monitor_name = monitor_name

    async def receive(self, msg, ctx: Context):
        match msg:
            case ServiceUp(service_id, service_type, _):
                print(f"[Monitor:{self.monitor_name}] SERVICE UP: {service_id} (type={service_type})")

            case ServiceDown(service_id, _, reason):
                print(f"[Monitor:{self.monitor_name}] SERVICE DOWN: {service_id} (reason={reason})")


async def main():
    print("=== Service Registry with Health Checks ===\n")

    # Using cluster directly - operations go to random nodes
    async with DevelopmentCluster(1) as cluster:
        await asyncio.sleep(0.3)

        # All spawns go through cluster (random node selection)
        registry = await cluster.spawn(ServiceRegistry, heartbeat_timeout=2.5)

        monitor = await cluster.spawn(ServiceMonitor, monitor_name="ops")
        await registry.send(Subscribe(monitor))

        print("\nPhase 1: Starting services")
        print("-" * 50)

        # Services spawned via cluster
        api_service = await cluster.spawn(
            Service,
            service_id="api-1",
            service_type="api-gateway",
            registry=registry,
            tags=["http", "public"],
        )

        db_service = await cluster.spawn(
            Service,
            service_id="db-1",
            service_type="database",
            registry=registry,
            tags=["postgres", "primary"],
        )

        cache_service = await cluster.spawn(
            Service,
            service_id="cache-1",
            service_type="cache",
            registry=registry,
            tags=["redis", "session"],
        )

        await asyncio.sleep(0.5)

        print("\nPhase 2: Service discovery")
        print("-" * 50)

        # Discover all services
        all_services = await registry.ask(Discover())
        print(f"All services ({len(all_services)}):")
        for svc in all_services:
            print(f"  - {svc['service_id']}: {svc['service_type']} {svc['tags']}")

        # Discover by type
        db_services = await registry.ask(Discover(service_type="database"))
        print(f"\nDatabase services: {[s['service_id'] for s in db_services]}")

        # Discover by tag
        http_services = await registry.ask(Discover(tags=["http"]))
        print(f"HTTP services: {[s['service_id'] for s in http_services]}")

        print("\nPhase 3: Service communication")
        print("-" * 50)

        # Ping a discovered service
        if all_services:
            api = all_services[0]
            print(f"Pinging {api['service_id']}...")
            response = await api_service.ask(Ping())
            print(f"Response: {response}")

        print("\nPhase 4: Graceful shutdown")
        print("-" * 50)

        # Stop via cluster
        await cluster.stop(cache_service)
        await asyncio.sleep(0.5)

        # Check remaining services
        remaining = await registry.ask(Discover())
        print(f"Remaining services: {[s['service_id'] for s in remaining]}")

        print("\nPhase 5: Simulating service failure")
        print("-" * 50)

        print("Stopping db-1 abruptly (without deregistration)...")
        await cluster.stop(db_service)

        # Wait for health check to detect the dead service
        print("Waiting for health check to detect failure...")
        await asyncio.sleep(3.0)

        # Final state
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
