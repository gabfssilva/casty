"""Singleton actor management mixin for global cluster singletons."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from ...actor import Actor, ActorRef
from ..raft_types import SingletonCommand, SingletonEntry
from ..remote import RemoteRef
from ..serialize import get_type_name, serialize
from ..transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class SingletonMixin:
    """Manage global singleton actors across the cluster via Raft consensus."""

    _cluster: ClusterState
    _running: bool

    def register_singleton_class(self, name: str, cls: type[Actor[Any]], **default_kwargs: Any) -> None:
        """Register a class for singleton auto-recreate after failover.

        Args:
            name: The singleton name this class is associated with.
            cls: The actor class to instantiate.
            **default_kwargs: Default constructor arguments.
        """
        self._cluster.singleton_actor_classes[name] = cls
        # Store default kwargs in type_registry for later use
        key = f"__singleton_kwargs__{name}"
        self._cluster.type_registry[key] = default_kwargs  # type: ignore

    async def _spawn_singleton[M](
        self,
        actor_cls: type[Actor[M]],
        name: str,
        kwargs: dict[str, Any],
    ) -> ActorRef[M]:
        """Spawn a singleton actor or return reference to existing one.

        If the singleton already exists on another node, returns a RemoteRef.
        If the singleton is orphaned (host node died), adopts it locally.
        If the singleton doesn't exist, creates it and registers via Raft.
        """
        # Check if we already host this singleton locally
        if name in self._cluster.singleton_local_refs:
            return self._cluster.singleton_local_refs[name]  # type: ignore

        # Check global registry (replicated via Raft)
        if name in self._cluster.singleton_registry:
            entry = self._cluster.singleton_registry[name]

            if entry.status == "active" and entry.node_id != self._cluster.node_id:
                # Singleton exists on another node - return RemoteRef
                transport = self._cluster.known_nodes.get(entry.node_id)
                if transport:
                    return RemoteRef(
                        name,
                        transport,
                        self._cluster.type_registry,
                        self._cluster.pending_asks,
                    )
                else:
                    # Node not connected, but singleton registered as active
                    # This could happen if the node just disconnected
                    log.warning(
                        f"Singleton '{name}' registered on {entry.node_id[:8]} but node not connected"
                    )

            elif entry.status == "orphan":
                # Singleton is orphaned - we can adopt it
                return await self._adopt_orphan_singleton(actor_cls, name, kwargs)

            elif entry.status == "active" and entry.node_id == self._cluster.node_id:
                # We should have it locally but don't - inconsistent state
                # This can happen after restart, recreate it
                return await self._create_singleton_local(actor_cls, name, kwargs, update_registry=False)

        # Singleton doesn't exist - create it
        return await self._create_singleton_local(actor_cls, name, kwargs, update_registry=True)

    async def _create_singleton_local[M](
        self,
        actor_cls: type[Actor[M]],
        name: str,
        kwargs: dict[str, Any],
        *,
        update_registry: bool,
    ) -> ActorRef[M]:
        """Create a singleton locally and optionally register via Raft."""
        # Spawn the actor locally (using base ActorSystem.spawn)
        from ..distributed import DistributedActorSystem

        ref = await super(DistributedActorSystem, self).spawn(actor_cls, name=name, **kwargs)  # type: ignore
        self._cluster.singleton_local_refs[name] = ref

        # Register the actor class for potential auto-recreate
        self._cluster.singleton_actor_classes[name] = actor_cls
        key = f"__singleton_kwargs__{name}"
        self._cluster.type_registry[key] = kwargs  # type: ignore

        if update_registry:
            # Register in cluster via Raft
            cmd = SingletonCommand(
                action="register",
                name=name,
                node_id=self._cluster.node_id,
                actor_cls_name=get_type_name(actor_cls),
                kwargs=tuple(kwargs.items()),
                timestamp=time.time(),
            )

            # If we're the leader, append directly
            # Otherwise, send to leader
            success = await self._append_singleton_command(cmd)
            if not success:
                log.warning(f"Failed to register singleton '{name}' in Raft log")

        return ref

    async def _adopt_orphan_singleton[M](
        self,
        actor_cls: type[Actor[M]],
        name: str,
        kwargs: dict[str, Any],
    ) -> ActorRef[M]:
        """Adopt an orphaned singleton by recreating it locally."""
        log.info(f"Adopting orphan singleton '{name}'")

        # Create locally
        from ..distributed import DistributedActorSystem

        ref = await super(DistributedActorSystem, self).spawn(actor_cls, name=name, **kwargs)  # type: ignore
        self._cluster.singleton_local_refs[name] = ref

        # Update registry via Raft (re-register with our node_id)
        cmd = SingletonCommand(
            action="register",
            name=name,
            node_id=self._cluster.node_id,
            actor_cls_name=get_type_name(actor_cls),
            kwargs=tuple(kwargs.items()),
            timestamp=time.time(),
        )

        success = await self._append_singleton_command(cmd)
        if not success:
            log.warning(f"Failed to update singleton '{name}' registry after adoption")

        return ref

    async def _append_singleton_command(self, cmd: SingletonCommand) -> bool:
        """Append a singleton command to the Raft log."""
        # Use the existing append_command from LogReplicationMixin
        return await self.append_command(cmd)  # type: ignore

    async def _apply_singleton_command(self, cmd: Any) -> None:
        """Apply a singleton command to the local state machine.

        This is called by the Raft apply callback for each committed entry.
        """
        if not isinstance(cmd, SingletonCommand):
            return

        if cmd.action == "register":
            self._cluster.singleton_registry[cmd.name] = SingletonEntry(
                name=cmd.name,
                node_id=cmd.node_id,
                actor_cls_name=cmd.actor_cls_name,
                kwargs=dict(cmd.kwargs),
                status="active",
            )
            log.debug(f"Registered singleton '{cmd.name}' on node {cmd.node_id[:8]}")

        elif cmd.action == "unregister":
            self._cluster.singleton_registry.pop(cmd.name, None)
            log.debug(f"Unregistered singleton '{cmd.name}'")

        elif cmd.action == "orphan":
            if cmd.name in self._cluster.singleton_registry:
                entry = self._cluster.singleton_registry[cmd.name]
                # Only orphan if the node_id matches (prevent stale orphan commands)
                if entry.node_id == cmd.node_id:
                    self._cluster.singleton_registry[cmd.name] = SingletonEntry(
                        name=entry.name,
                        node_id=entry.node_id,
                        actor_cls_name=entry.actor_cls_name,
                        kwargs=entry.kwargs,
                        status="orphan",
                    )
                    log.info(f"Marked singleton '{cmd.name}' as orphan (node {cmd.node_id[:8]} disconnected)")

    async def _handle_node_disconnect_for_singletons(self, disconnected_node_id: str) -> None:
        """Mark singletons hosted by disconnected node as orphans.

        Only the leader should call this to ensure consistency via Raft.
        """
        # Don't try to replicate during shutdown
        if not self._running:
            return

        if self._cluster.state != "leader":
            return

        for name, entry in list(self._cluster.singleton_registry.items()):
            if entry.node_id == disconnected_node_id and entry.status == "active":
                # Check again before each command (cluster state may change)
                if not self._running or self._cluster.state != "leader":
                    return
                cmd = SingletonCommand(
                    action="orphan",
                    name=name,
                    node_id=disconnected_node_id,
                    actor_cls_name=entry.actor_cls_name,
                    kwargs=tuple(entry.kwargs.items()),
                    timestamp=time.time(),
                )
                try:
                    await self._append_singleton_command(cmd)
                except Exception as e:
                    log.warning(f"Failed to orphan singleton '{name}': {e}")

    async def _lookup_singleton(self, name: str) -> ActorRef[Any]:
        """Look up a singleton by name.

        Returns local ref if hosted here, RemoteRef if hosted elsewhere.
        Raises KeyError if singleton doesn't exist.
        """
        # Check local first
        if name in self._cluster.singleton_local_refs:
            return self._cluster.singleton_local_refs[name]  # type: ignore

        # Check global registry
        if name in self._cluster.singleton_registry:
            entry = self._cluster.singleton_registry[name]

            if entry.status == "orphan":
                raise KeyError(f"Singleton '{name}' is orphaned (host node disconnected)")

            if entry.node_id == self._cluster.node_id:
                # Should be local but isn't - inconsistent
                raise KeyError(f"Singleton '{name}' registered locally but not found")

            # Get transport to the hosting node
            transport = self._cluster.known_nodes.get(entry.node_id)
            if not transport:
                raise KeyError(f"Singleton '{name}' host node {entry.node_id[:8]} not connected")

            return RemoteRef(
                name,
                transport,
                self._cluster.type_registry,
                self._cluster.pending_asks,
            )

        raise KeyError(f"Singleton '{name}' not found in cluster")

    # Message handlers for singleton info (used for debugging/monitoring)
    async def _handle_singleton_info_request(self, transport: Transport, msg: WireMessage) -> None:
        """Handle request for singleton registry info."""
        info = {
            "singletons": {
                name: {
                    "node_id": entry.node_id,
                    "status": entry.status,
                    "actor_cls": entry.actor_cls_name,
                }
                for name, entry in self._cluster.singleton_registry.items()
            }
        }
        response = WireMessage(
            msg_type=MessageType.SINGLETON_INFO_RES,
            target_name=msg.target_name,
            payload=serialize(info),
        )
        await transport.send(response)
