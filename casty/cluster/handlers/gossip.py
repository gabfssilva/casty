"""Gossip protocol state change handler mixin."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from casty import Context
from casty.actor import on
from casty.gossip import StateChanged, GossipStarted
from ..messages import ActorRegistered, ActorUnregistered

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class GossipHandlers:
    """Handles Gossip protocol state changes."""

    state: ClusterState

    @on(StateChanged)
    async def handle_state_changed(
        self, msg: StateChanged, ctx: Context
    ) -> None:
        """Handle gossip state changed event."""
        key = msg.key
        value = msg.value
        log.debug(f"[{self.state.node_id}] StateChanged: {key}")

        # Handle actor registrations
        if key.startswith("_actors/"):
            actor_name = key[len("_actors/") :]

            if value is None:
                old_node = self.state.actor_locations.pop(actor_name, None)
                if old_node:
                    await self._broadcast_event(ActorUnregistered(actor_name, old_node))
            else:
                node_id = value.decode("utf-8")
                self.state.actor_locations[actor_name] = node_id
                await self._broadcast_event(ActorRegistered(actor_name, node_id))
            return

        # Handle sharded type registrations (from gossip - for late-joining nodes)
        if key.startswith("_sharded/"):
            await self._handle_gossip_sharded_type(key, value)
            return

        # Handle singleton location updates
        if key.startswith("_singletons/"):
            await self._handle_gossip_singleton_location(key, value)
            return

        # Handle singleton metadata (class FQN for remote spawning)
        if key.startswith("_singleton_meta/"):
            await self._handle_gossip_singleton_meta(key, value, ctx)
            return

        # Handle merge version updates (for three-way merge conflict detection)
        if key.startswith("__merge/"):
            await self._handle_merge_version_change(key, value, ctx)
            return

    @on(GossipStarted)
    async def handle_gossip_started(self, msg: GossipStarted, ctx: Context) -> None:
        """Handle gossip started event."""
        pass  # Gossip is ready

    async def _handle_gossip_sharded_type(self, key: str, value: bytes | None) -> None:
        """Handle sharded type registration from gossip."""
        entity_type = key[len("_sharded/") :]

        if value is None:
            # Sharded type unregistered
            self.state.sharded_types.pop(entity_type, None)
            self.state.local_entities.pop(entity_type, None)
            log.debug(f"Unregistered sharded type from cluster: {entity_type}")
        else:
            # Import and register the sharded type (if not already known)
            if entity_type not in self.state.sharded_types:
                class_fqn = value.decode("utf-8")
                try:
                    from ..cluster import _import_class

                    actor_cls = _import_class(class_fqn)
                    self.state.sharded_types[entity_type] = actor_cls
                    if entity_type not in self.state.local_entities:
                        self.state.local_entities[entity_type] = {}
                    log.debug(f"Registered sharded type from gossip: {entity_type} -> {class_fqn}")
                except Exception as e:
                    log.error(f"Failed to import sharded type {entity_type}: {class_fqn}: {e}")

    async def _handle_gossip_singleton_location(self, key: str, value: bytes | None) -> None:
        """Handle singleton location update from gossip."""
        singleton_name = key[len("_singletons/") :]

        if value is None:
            # Singleton unregistered
            self.state.singleton_locations.pop(singleton_name, None)
            log.debug(f"Singleton unregistered from cluster: {singleton_name}")
        else:
            # Singleton location updated
            owner_node = value.decode("utf-8")
            self.state.singleton_locations[singleton_name] = owner_node
            log.debug(f"Singleton {singleton_name} located at {owner_node}")

    async def _handle_gossip_singleton_meta(
        self, key: str, value: bytes | None, ctx: Context
    ) -> None:
        """Handle singleton metadata (class FQN) from gossip."""
        singleton_name = key[len("_singleton_meta/") :]

        if value is None:
            # Metadata removed
            self.state.singleton_types.pop(singleton_name, None)
        else:
            # Import and register the singleton type
            if singleton_name not in self.state.singleton_types:
                class_fqn = value.decode("utf-8")
                try:
                    from ..cluster import _import_class

                    actor_cls = _import_class(class_fqn)
                    self.state.singleton_types[singleton_name] = actor_cls
                    log.debug(f"Registered singleton type from cluster: {singleton_name} -> {class_fqn}")

                    # Check if we should own this singleton
                    await self._maybe_spawn_singleton(singleton_name, ctx)
                except Exception as e:
                    log.error(f"Failed to import singleton type {singleton_name}: {class_fqn}: {e}")

    async def _maybe_spawn_singleton(self, singleton_name: str, ctx: Context) -> None:
        """Check if we should spawn a singleton and do so if we're the owner."""
        # Implemented in singletons.py mixin
        pass

    async def _handle_merge_version_change(
        self, key: str, value: bytes | None, ctx: Context
    ) -> None:
        """Handle merge version update from gossip."""
        # Implemented in merge.py mixin
        pass

    async def _broadcast_event(self, event) -> None:
        """Broadcast cluster event to subscribers."""
        # This is implemented in cluster.py
        pass
