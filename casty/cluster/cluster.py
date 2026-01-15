"""Cluster Actor - 100% actor-based cluster coordination.

The Cluster actor is the main entry point for cluster functionality in Casty.
It manages:
- TransportMux for single-port communication
- SWIM membership via SwimCoordinator child actor
- Gossip-based actor registry via GossipManager child actor
- Remote actor messaging (send/ask)
- Sharded entities via consistent hashing
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import msgpack

from casty import Actor, Context
from casty.scheduler import Scheduler
from casty.transport import TransportMux
from casty.actor import on

from .config import ClusterConfig
from .state import ClusterState, _RegistrationTimeout
from .codec import ActorCodec
from .messages import (
    Subscribe,
    Unsubscribe,
    ClusterEvent,
)
from .handlers import (
    TransportHandlers,
    SwimHandlers,
    GossipHandlers,
    ActorHandlers,
    EntityHandlers,
    SingletonHandlers,
    MergeHandlers,
    ReplicationHandlers,
)

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


# =============================================================================
# Helper Functions (Deserialization, Import)
# =============================================================================


def _import_class(fully_qualified_name: str) -> type:
    """Import a class from its fully qualified name."""
    import importlib

    parts = fully_qualified_name.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid class name: {fully_qualified_name}")

    module_path, class_name = parts

    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (ImportError, AttributeError):
        module_parts = module_path.rsplit(".", 1)
        if len(module_parts) == 2:
            parent_module_path, parent_name = module_parts
            module = importlib.import_module(parent_module_path)
            parent = getattr(module, parent_name)
            return getattr(parent, class_name)
        raise


def _deserialize_recursive(cls: type, data: Any) -> Any:
    """Recursively deserialize data into a dataclass instance."""
    import dataclasses
    from typing import get_type_hints, get_origin, get_args, Union

    if not isinstance(data, dict):
        return data

    if not dataclasses.is_dataclass(cls):
        try:
            return cls(**data)
        except TypeError:
            return data

    try:
        hints = get_type_hints(cls)
    except Exception:
        return cls(**data)

    processed = {}
    for field_name, value in data.items():
        if field_name not in hints:
            processed[field_name] = value
            continue

        field_type = hints[field_name]
        origin = get_origin(field_type)

        if origin is Union:
            args = get_args(field_type)
            for arg in args:
                if arg is type(None):
                    continue
                if dataclasses.is_dataclass(arg) and isinstance(value, dict):
                    try:
                        processed[field_name] = _deserialize_recursive(arg, value)
                        break
                    except (TypeError, ValueError):
                        continue
            else:
                processed[field_name] = value
        elif dataclasses.is_dataclass(field_type) and isinstance(value, dict):
            processed[field_name] = _deserialize_recursive(field_type, value)
        elif origin is list and isinstance(value, list):
            args = get_args(field_type)
            if args and dataclasses.is_dataclass(args[0]):
                processed[field_name] = [
                    _deserialize_recursive(args[0], item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                processed[field_name] = value
        else:
            processed[field_name] = value

    return cls(**processed)


def deserialize_message(payload_type: str, payload_dict: dict) -> Any:
    """Deserialize a message from its type name and dict representation."""
    cls = _import_class(payload_type)
    return _deserialize_recursive(cls, payload_dict)


# =============================================================================
# Cluster Actor - refactored with mixins + handlers
# =============================================================================


class Cluster(
    Actor,
    TransportHandlers,
    SwimHandlers,
    GossipHandlers,
    ActorHandlers,
    EntityHandlers,
    SingletonHandlers,
    MergeHandlers,
    ReplicationHandlers,
):
    """Actor-based cluster coordinator with modular handler mixins.

    Uses TransportMux for protocol multiplexing over a single TCP port.
    Routes SWIM and GOSSIP protocols to their respective handlers.
    Handles ACTOR protocol for remote actor messaging via handler mixins.

    Example:
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig.development().with_seeds(["192.168.1.10:7946"]),
            )

            # Register a local actor
            worker = await system.spawn(Worker, name="worker-1")
            await cluster.send(LocalRegister("worker-1", worker))

            # Get a remote reference
            remote_ref = await cluster.ask(GetRemoteRef("worker-2", "node-2"))
            await remote_ref.send(DoWork())
    """

    # Shared state across all mixins
    state: ClusterState

    def __init__(self, config: ClusterConfig | None = None) -> None:
        """Initialize cluster with state container."""
        self.state = ClusterState.create(config)

    async def on_start(self) -> None:
        """Initialize the cluster."""
        ctx = self._ctx

        # Spawn scheduler for timeouts
        self.state.scheduler = await ctx.spawn(Scheduler)

        # Spawn TransportMux
        advertise = None
        if self.state.config.advertise_host and self.state.config.advertise_port:
            advertise = (self.state.config.advertise_host, self.state.config.advertise_port)

        self.state.mux = await ctx.spawn(
            TransportMux,
            node_id=self.state.node_id,
            bind_address=(self.state.config.bind_host, self.state.config.bind_port),
            advertise_address=advertise,
        )

    async def on_stop(self) -> None:
        """Cleanup on stop."""
        for pending in self.state.pending_asks.values():
            if not pending.future.done():
                pending.future.cancel()

    # =========================================================================
    # Subscribe/Unsubscribe Handlers (simple cases without special routing)
    # =========================================================================

    @on(Subscribe)
    async def handle_subscribe(self, msg: Subscribe, ctx: Context) -> None:
        """Handle event subscription request."""
        self.state.subscribers.add(msg.subscriber)

    @on(Unsubscribe)
    async def handle_unsubscribe(self, msg: Unsubscribe, ctx: Context) -> None:
        """Handle event unsubscription request."""
        self.state.subscribers.discard(msg.subscriber)

    @on(_RegistrationTimeout)
    async def handle_registration_timeout(self, msg: _RegistrationTimeout, ctx: Context) -> None:
        """Handle timeout for sharded type registration."""
        await self._handle_registration_timeout(msg.request_id)

    # =========================================================================
    # Shared Helper Methods
    # =========================================================================

    async def _broadcast_event(self, event: ClusterEvent) -> None:
        """Broadcast cluster event to all subscribers."""
        for subscriber in list(self.state.subscribers):
            await subscriber.send(event)

    async def _handle_registration_timeout(self, request_id: str) -> None:
        """Handle timeout for sharded type registration."""
        from .state import PendingRegistration
        pending = self.state.pending_registrations.pop(request_id, None)
        if pending and pending.reply_future and not pending.reply_future.done():
            log.warning(
                f"Timeout waiting for acks for {pending.entity_type}: "
                f"got {len(pending.received_acks)}/{pending.required_acks}"
            )
            # Reply with None on timeout (registration still happened locally)
            pending.reply_future.set_result(None)
