from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, ActorSystem, Behavior, Behaviors, OneForOneStrategy
from casty.sharding import ClusteredActorSystem
from casty.cluster_state import NodeAddress, ServiceEntry
from casty.receptionist import (
    Deregister,
    Find,
    Listing,
    Register,
    ServiceKey,
    Subscribe,
    receptionist_actor,
)
from casty.topology import TopologySnapshot


@dataclass(frozen=True)
class Stop:
    pass


@dataclass(frozen=True)
class Ping:
    seq: int


type PingKey = ServiceKey[Ping]


NODE = NodeAddress(host="127.0.0.1", port=2551)


def collector(results: list[Any]) -> Behavior[Any]:
    async def receive(ctx: Any, msg: Any) -> Any:
        results.append(msg)
        return Behaviors.same()

    return Behaviors.receive(receive)


async def test_register_and_find() -> None:
    """Register a service, then Find should return it in Listing."""
    results: list[Any] = []
    async with ActorSystem(name="test") as system:
        rec = system.spawn(
            receptionist_actor(self_node=NODE, system_name="test"), "receptionist",
        )
        target = system.spawn(collector([]), "ping-service")
        sink = system.spawn(collector(results), "sink")
        await asyncio.sleep(0.1)

        key: PingKey = ServiceKey("ping")
        rec.tell(Register(key=key, ref=target))
        await asyncio.sleep(0.1)

        rec.tell(Find(key=key, reply_to=sink))
        await asyncio.sleep(0.1)

    assert len(results) == 1
    listing = results[0]
    assert isinstance(listing, Listing)
    assert listing.key.name == "ping"
    assert len(listing.instances) == 1
    instance = next(iter(listing.instances))
    assert instance.node == NODE


async def test_subscribe_receives_listing_immediately() -> None:
    """Subscribe should immediately receive the current Listing."""
    results: list[Any] = []
    async with ActorSystem(name="test") as system:
        rec = system.spawn(
            receptionist_actor(self_node=NODE, system_name="test"), "receptionist",
        )
        sink = system.spawn(collector(results), "sink")
        await asyncio.sleep(0.1)

        key: PingKey = ServiceKey("ping")
        rec.tell(Subscribe(key=key, reply_to=sink))
        await asyncio.sleep(0.1)

    assert len(results) == 1
    listing = results[0]
    assert isinstance(listing, Listing)
    assert listing.key.name == "ping"
    assert len(listing.instances) == 0


async def test_subscribe_notified_on_new_registration() -> None:
    """Subscriber should be notified when a new service registers."""
    results: list[Any] = []
    async with ActorSystem(name="test") as system:
        rec = system.spawn(
            receptionist_actor(self_node=NODE, system_name="test"), "receptionist",
        )
        target = system.spawn(collector([]), "ping-service")
        sink = system.spawn(collector(results), "sink")
        await asyncio.sleep(0.1)

        key: PingKey = ServiceKey("ping")
        rec.tell(Subscribe(key=key, reply_to=sink))
        await asyncio.sleep(0.1)

        rec.tell(Register(key=key, ref=target))
        await asyncio.sleep(0.1)

    assert len(results) == 2
    initial_listing = results[0]
    assert isinstance(initial_listing, Listing)
    assert len(initial_listing.instances) == 0

    updated_listing = results[1]
    assert isinstance(updated_listing, Listing)
    assert len(updated_listing.instances) == 1


async def test_deregister_removes_service() -> None:
    """Deregister should remove the service from the registry."""
    results: list[Any] = []
    async with ActorSystem(name="test") as system:
        rec = system.spawn(
            receptionist_actor(self_node=NODE, system_name="test"), "receptionist",
        )
        target = system.spawn(collector([]), "ping-service")
        sink = system.spawn(collector(results), "sink")
        await asyncio.sleep(0.1)

        key: PingKey = ServiceKey("ping")
        rec.tell(Register(key=key, ref=target))
        await asyncio.sleep(0.1)

        rec.tell(Deregister(key=key, ref=target))
        await asyncio.sleep(0.1)

        rec.tell(Find(key=key, reply_to=sink))
        await asyncio.sleep(0.1)

    assert len(results) == 1
    listing = results[0]
    assert isinstance(listing, Listing)
    assert len(listing.instances) == 0


async def test_registry_updated_from_topology_snapshot() -> None:
    """TopologySnapshot with registry should make remote entries available via Find."""
    results: list[Any] = []
    remote_node = NodeAddress(host="10.0.0.2", port=2551)
    async with ActorSystem(name="test") as system:
        rec = system.spawn(
            receptionist_actor(self_node=NODE, system_name="test"), "receptionist",
        )
        sink = system.spawn(collector(results), "sink")
        await asyncio.sleep(0.1)

        remote_entry = ServiceEntry(
            key="ping", node=remote_node, path="/user/ping-service",
        )
        snapshot = TopologySnapshot(
            members=frozenset(),
            leader=None,
            shard_allocations={},
            allocation_epoch=0,
            registry=frozenset({remote_entry}),
        )
        rec.tell(snapshot)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        key: PingKey = ServiceKey("ping")
        rec.tell(Find(key=key, reply_to=sink))
        await asyncio.sleep(0.1)

    assert len(results) == 1
    listing = results[0]
    assert isinstance(listing, Listing)
    assert len(listing.instances) == 1
    instance = next(iter(listing.instances))
    assert instance.node == remote_node


def stoppable() -> Behavior[Any]:
    async def receive(ctx: Any, msg: Any) -> Any:
        match msg:
            case Stop():
                return Behaviors.stopped()
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


async def test_auto_deregister_on_actor_stopped() -> None:
    """When a registered actor stops, it should be automatically deregistered."""
    results: list[Any] = []
    async with ActorSystem(name="test") as system:
        rec = system.spawn(
            receptionist_actor(
                self_node=NODE,
                system_name="test",
                event_stream=system.event_stream,
            ),
            "receptionist",
        )
        target = system.spawn(stoppable(), "ping-service")
        sink = system.spawn(collector(results), "sink")
        await asyncio.sleep(0.1)

        key: PingKey = ServiceKey("ping")
        rec.tell(Register(key=key, ref=target))
        await asyncio.sleep(0.1)

        target.tell(Stop())
        await asyncio.sleep(0.3)

        rec.tell(Find(key=key, reply_to=sink))
        await asyncio.sleep(0.1)

    assert len(results) == 1
    listing = results[0]
    assert isinstance(listing, Listing)
    assert len(listing.instances) == 0


# ---------------------------------------------------------------------------
# Behaviors.discoverable() tests (require ClusteredActorSystem)
# ---------------------------------------------------------------------------

PING_KEY: ServiceKey[Ping] = ServiceKey("ping")


async def test_discoverable_auto_registers() -> None:
    """Spawning a discoverable behavior auto-registers with the receptionist."""
    async with ClusteredActorSystem(
        name="disc-reg", host="127.0.0.1", port=0, node_id="node-1",
    ) as system:
        ref: ActorRef[Ping] = system.spawn(
            Behaviors.discoverable(collector([]), key=PING_KEY), "ping-svc",
        )
        await asyncio.sleep(0.3)

        listing = await system.lookup(PING_KEY)
        paths = {inst.ref.address.path for inst in listing.instances}
        assert ref.address.path in paths


async def test_discoverable_auto_deregisters_on_stop() -> None:
    """When a discoverable actor stops, it is automatically deregistered."""
    async with ClusteredActorSystem(
        name="disc-dereg", host="127.0.0.1", port=0, node_id="node-1",
    ) as system:
        ref: ActorRef[Any] = system.spawn(
            Behaviors.discoverable(stoppable(), key=PING_KEY), "ping-svc",
        )
        await asyncio.sleep(0.3)

        listing = await system.lookup(PING_KEY)
        assert len(listing.instances) == 1

        ref.tell(Stop())
        await asyncio.sleep(0.5)

        listing = await system.lookup(PING_KEY)
        assert len(listing.instances) == 0


async def test_discoverable_composes_with_supervise() -> None:
    """Discoverable wraps a supervised behavior — both work correctly."""
    async with ClusteredActorSystem(
        name="disc-sup", host="127.0.0.1", port=0, node_id="node-1",
    ) as system:
        supervised = Behaviors.supervise(
            collector([]),
            OneForOneStrategy(),
        )
        system.spawn(
            Behaviors.discoverable(supervised, key=PING_KEY), "ping-svc",
        )
        await asyncio.sleep(0.3)

        listing = await system.lookup(PING_KEY)
        assert len(listing.instances) == 1


async def test_discoverable_via_ctx_spawn() -> None:
    """A parent actor that ctx.spawn()s a discoverable child auto-registers it."""

    @dataclass(frozen=True)
    class SpawnChild:
        pass

    def parent_actor() -> Behavior[SpawnChild]:
        async def receive(ctx: Any, msg: SpawnChild) -> Any:
            match msg:
                case SpawnChild():
                    ctx.spawn(
                        Behaviors.discoverable(collector([]), key=PING_KEY),
                        "child-svc",
                    )
                    return Behaviors.same()

        return Behaviors.receive(receive)

    async with ClusteredActorSystem(
        name="disc-ctx", host="127.0.0.1", port=0, node_id="node-1",
    ) as system:
        parent = system.spawn(parent_actor(), "parent")
        await asyncio.sleep(0.3)

        parent.tell(SpawnChild())
        await asyncio.sleep(0.3)

        listing = await system.lookup(PING_KEY)
        assert len(listing.instances) == 1
        instance = next(iter(listing.instances))
        assert "child-svc" in instance.ref.address.path
