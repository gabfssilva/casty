"""Receptionist actor for typed service discovery.

Provides a registry where actors can register themselves under typed
``ServiceKey`` names and other actors can discover them via ``Find``
(one-shot) or ``Subscribe`` (continuous notifications).  In a cluster,
the receptionist propagates registrations through gossip so that remote
services become discoverable on every node.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, TYPE_CHECKING

from casty import ActorContext
from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress, ServiceEntry

if TYPE_CHECKING:
    from casty.events import EventStream
    from casty.ref import ActorRef
    from casty.remote_transport import RemoteTransport


@dataclass(frozen=True)
class ServiceKey[M]:
    """Typed key identifying a service in the cluster registry.

    Parameters
    ----------
    name : str
        Human-readable service name (e.g. ``"payment-service"``).

    Examples
    --------
    >>> key: ServiceKey[int] = ServiceKey("counter")
    >>> key.name
    'counter'
    """

    name: str


@dataclass(frozen=True)
class ServiceInstance[M]:
    """A single instance of a service in the cluster.

    Parameters
    ----------
    ref : ActorRef[M]
        Reference to the actor providing the service.
    node : NodeAddress
        Node where the actor lives.

    Examples
    --------
    >>> instance = ServiceInstance(ref=some_ref, node=NodeAddress("127.0.0.1", 2551))
    """

    ref: ActorRef[M]
    node: NodeAddress


@dataclass(frozen=True)
class Listing[M]:
    """Current set of instances for a given service key.

    Parameters
    ----------
    key : ServiceKey[M]
        The service key this listing is for.
    instances : frozenset[ServiceInstance[M]]
        All known instances of the service.

    Examples
    --------
    >>> listing = Listing(key=ServiceKey("counter"), instances=frozenset())
    >>> len(listing.instances)
    0
    """

    key: ServiceKey[M]
    instances: frozenset[ServiceInstance[M]]


@dataclass(frozen=True)
class Register[M]:
    """Register a local actor as a service instance.

    Parameters
    ----------
    key : ServiceKey[M]
        Service key to register under.
    ref : ActorRef[M]
        Reference to the actor providing the service.
    """

    key: ServiceKey[M]
    ref: ActorRef[M]


@dataclass(frozen=True)
class Deregister[M]:
    """Remove a local actor from the service registry.

    Parameters
    ----------
    key : ServiceKey[M]
        Service key to deregister from.
    ref : ActorRef[M]
        Reference to the actor to remove.
    """

    key: ServiceKey[M]
    ref: ActorRef[M]


@dataclass(frozen=True)
class Subscribe[M]:
    """Subscribe to changes in a service key's instance set.

    The subscriber immediately receives the current ``Listing`` and is
    notified on every subsequent change.

    Parameters
    ----------
    key : ServiceKey[M]
        Service key to watch.
    reply_to : ActorRef[Listing[M]]
        Where to send ``Listing`` updates.
    """

    key: ServiceKey[M]
    reply_to: ActorRef[Listing[M]]


@dataclass(frozen=True)
class Find[M]:
    """One-shot query for the current instances of a service key.

    Parameters
    ----------
    key : ServiceKey[M]
        Service key to look up.
    reply_to : ActorRef[Listing[M]]
        Where to send the current ``Listing``.
    """

    key: ServiceKey[M]
    reply_to: ActorRef[Listing[M]]


@dataclass(frozen=True)
class RegistryUpdated:
    """Cluster-wide registry snapshot from gossip (internal message).

    Parameters
    ----------
    registry : frozenset[ServiceEntry]
        Complete set of service entries received via gossip.
    """

    registry: frozenset[ServiceEntry]


@dataclass(frozen=True)
class ActorTerminated:
    """Internal: an actor we were tracking has stopped."""

    path: str


type ReceptionistMsg = (
    Register[Any]
    | Deregister[Any]
    | Subscribe[Any]
    | Find[Any]
    | RegistryUpdated
    | ActorTerminated
)


@dataclass(frozen=True)
class ReceptionistEnv:
    """Immutable configuration captured once at receptionist startup."""

    self_node: NodeAddress
    system_name: str
    remote_transport: RemoteTransport | None


type Subscribers = MappingProxyType[str, frozenset[ActorRef[Listing[Any]]]]


def _make_ref(entry: ServiceEntry, env: ReceptionistEnv) -> ActorRef[Any]:
    """Construct an ``ActorRef`` from a ``ServiceEntry``."""
    from casty.address import ActorAddress
    from casty.ref import ActorRef as ActorRefCls
    from casty.transport import LocalTransport

    addr = ActorAddress(
        system=env.system_name,
        path=entry.path,
        host=entry.node.host,
        port=entry.node.port,
    )
    if env.remote_transport is not None:
        return env.remote_transport.make_ref(addr)
    return ActorRefCls(address=addr, _transport=LocalTransport())


def _build_listing(
    key_name: str,
    local_entries: frozenset[ServiceEntry],
    cluster_registry: frozenset[ServiceEntry],
    env: ReceptionistEnv,
) -> Listing[Any]:
    """Build a ``Listing`` from local and cluster entries for a key."""
    all_entries = local_entries | cluster_registry
    matching = frozenset(e for e in all_entries if e.key == key_name)
    instances = frozenset(
        ServiceInstance(ref=_make_ref(e, env), node=e.node)
        for e in matching
    )
    return Listing(key=ServiceKey(key_name), instances=instances)


def _notify_subscribers(
    key_name: str,
    subscribers: Subscribers,
    local_entries: frozenset[ServiceEntry],
    cluster_registry: frozenset[ServiceEntry],
    env: ReceptionistEnv,
) -> None:
    """Send a ``Listing`` to all subscribers of *key_name*."""
    subs = subscribers.get(key_name, frozenset())
    if not subs:
        return
    listing = _build_listing(key_name, local_entries, cluster_registry, env)
    for sub in subs:
        sub.tell(listing)


def receptionist_actor(
    *,
    self_node: NodeAddress,
    gossip_ref: ActorRef[Any] | None = None,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    event_stream: EventStream | None = None,
) -> Behavior[ReceptionistMsg]:
    """Create a receptionist behavior for typed service discovery.

    Parameters
    ----------
    self_node : NodeAddress
        Address of the local node.
    gossip_ref : ActorRef[Any] or None
        Reference to the gossip actor for propagating registry changes.
    remote_transport : RemoteTransport or None
        Remote transport for constructing refs to remote actors.
    system_name : str
        Name of the actor system.
    event_stream : EventStream or None
        Event stream for subscribing to actor lifecycle events.

    Returns
    -------
    Behavior[ReceptionistMsg]
        The receptionist behavior ready to be spawned.

    Examples
    --------
    >>> behavior = receptionist_actor(self_node=NodeAddress("127.0.0.1", 2551))
    >>> ref = system.spawn(behavior, "receptionist")
    """
    env = ReceptionistEnv(
        self_node=self_node,
        system_name=system_name,
        remote_transport=remote_transport,
    )

    def active(
        local_entries: frozenset[ServiceEntry],
        cluster_registry: frozenset[ServiceEntry],
        subscribers: Subscribers,
    ) -> Behavior[ReceptionistMsg]:
        async def receive(
            ctx: ActorContext[ReceptionistMsg], msg: ReceptionistMsg,
        ) -> Behavior[ReceptionistMsg]:
            match msg:
                case Register(key=skey, ref=ref):
                    entry = ServiceEntry(
                        key=skey.name,
                        node=env.self_node,
                        path=ref.address.path,
                    )
                    new_local = local_entries | {entry}
                    if gossip_ref is not None:
                        from casty.gossip_actor import UpdateRegistry
                        gossip_ref.tell(UpdateRegistry(entries=new_local))
                    _notify_subscribers(
                        skey.name, subscribers, new_local, cluster_registry, env,
                    )
                    return active(new_local, cluster_registry, subscribers)

                case Deregister(key=skey, ref=ref):
                    entry = ServiceEntry(
                        key=skey.name,
                        node=env.self_node,
                        path=ref.address.path,
                    )
                    new_local = local_entries - {entry}
                    if gossip_ref is not None:
                        from casty.gossip_actor import UpdateRegistry
                        gossip_ref.tell(UpdateRegistry(entries=new_local))
                    _notify_subscribers(
                        skey.name, subscribers, new_local, cluster_registry, env,
                    )
                    return active(new_local, cluster_registry, subscribers)

                case Subscribe(key=skey, reply_to=reply_to):
                    existing = subscribers.get(skey.name, frozenset())
                    new_subs = MappingProxyType(
                        {**subscribers, skey.name: existing | {reply_to}},
                    )
                    listing = _build_listing(
                        skey.name, local_entries, cluster_registry, env,
                    )
                    reply_to.tell(listing)
                    return active(local_entries, cluster_registry, new_subs)

                case Find(key=skey, reply_to=reply_to):
                    listing = _build_listing(
                        skey.name, local_entries, cluster_registry, env,
                    )
                    reply_to.tell(listing)
                    return Behaviors.same()

                case RegistryUpdated(registry=registry):
                    remote_entries = frozenset(
                        e for e in registry if e.node != env.self_node
                    )
                    old_keys = {e.key for e in cluster_registry}
                    new_keys = {e.key for e in remote_entries}
                    changed_keys = old_keys ^ new_keys
                    for e in cluster_registry ^ remote_entries:
                        changed_keys.add(e.key)
                    for key_name in changed_keys:
                        _notify_subscribers(
                            key_name, subscribers, local_entries,
                            remote_entries, env,
                        )
                    return active(local_entries, remote_entries, subscribers)

                case ActorTerminated(path=stopped_path):
                    removed = frozenset(
                        e for e in local_entries if e.path != stopped_path
                    )
                    if removed != local_entries:
                        changed_keys = {e.key for e in (local_entries - removed)}
                        if gossip_ref is not None:
                            from casty.gossip_actor import UpdateRegistry
                            gossip_ref.tell(UpdateRegistry(entries=removed))
                        for key_name in changed_keys:
                            _notify_subscribers(
                                key_name, subscribers, removed,
                                cluster_registry, env,
                            )
                        return active(removed, cluster_registry, subscribers)
                    return Behaviors.same()

                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    async def setup(ctx: ActorContext[ReceptionistMsg]) -> Behavior[ReceptionistMsg]:
        if event_stream is not None:
            from casty.events import ActorStopped

            async def on_stopped(event: ActorStopped) -> None:
                try:
                    ctx.self.tell(ActorTerminated(path=event.ref.address.path))
                except Exception as e:
                    ctx.log.warning("Failed to notify receptionist: %s", e)
                    ctx.log.debug("Failed to notify receptionist: %s", e)

            event_stream.subscribe(ActorStopped, on_stopped)

        return active(frozenset(), frozenset(), MappingProxyType({}))

    return Behaviors.setup(setup)
