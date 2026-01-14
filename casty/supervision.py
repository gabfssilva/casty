"""Supervision hierarchy for Casty actors.

Provides Erlang/Akka-style supervision trees with configurable
restart strategies, backoff, and failure handling.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Callable, Awaitable

if TYPE_CHECKING:
    from .actor import Actor, ActorId, ActorRef


class SupervisionStrategy(Enum):
    """Defines how to handle a single child failure."""

    RESTART = auto()  # Restart the failed child
    STOP = auto()  # Stop the failed child permanently
    ESCALATE = auto()  # Propagate failure to parent supervisor


class MultiChildStrategy(Enum):
    """Defines how to handle failures when actor has multiple children."""

    ONE_FOR_ONE = auto()  # Only restart the failed child
    ONE_FOR_ALL = auto()  # Restart all children if one fails
    REST_FOR_ONE = auto()  # Restart failed child and children spawned after it


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    """Configuration for supervision behavior.

    Attributes:
        strategy: How to handle individual failures
        multi_child: How to handle failures with multiple children
        max_restarts: Maximum restarts allowed within the time window
        within_seconds: Time window for counting restarts
        backoff_initial: Initial backoff delay in seconds
        backoff_max: Maximum backoff delay in seconds
        backoff_multiplier: Multiplier for exponential backoff
    """

    strategy: SupervisionStrategy = SupervisionStrategy.RESTART
    multi_child: MultiChildStrategy = MultiChildStrategy.ONE_FOR_ONE
    max_restarts: int = 3
    within_seconds: float = 60.0
    backoff_initial: float = 0.1
    backoff_max: float = 30.0
    backoff_multiplier: float = 2.0


@dataclass
class SupervisionDecision:
    """Decision returned by on_child_failure to override default behavior.

    Attributes:
        action: The supervision action to take
        apply_to: List of actors to apply action to (for ONE_FOR_ALL/REST_FOR_ONE)
    """

    action: SupervisionStrategy
    apply_to: list["ActorRef[Any]"] | None = None


class SupervisorConfigPresets:
    """Common supervision configurations."""

    @staticmethod
    def default() -> SupervisorConfig:
        """Default: restart up to 3 times in 60 seconds."""
        return SupervisorConfig()

    @staticmethod
    def aggressive_restart() -> SupervisorConfig:
        """Aggressive restarts with short backoff for transient failures."""
        return SupervisorConfig(
            strategy=SupervisionStrategy.RESTART,
            max_restarts=10,
            within_seconds=30.0,
            backoff_initial=0.01,
            backoff_max=5.0,
        )

    @staticmethod
    def fail_fast() -> SupervisorConfig:
        """Stop immediately on first failure."""
        return SupervisorConfig(
            strategy=SupervisionStrategy.STOP,
            max_restarts=0,
        )

    @staticmethod
    def escalate_always() -> SupervisorConfig:
        """Always escalate failures to parent."""
        return SupervisorConfig(
            strategy=SupervisionStrategy.ESCALATE,
        )

    @staticmethod
    def one_for_all() -> SupervisorConfig:
        """Restart all children when one fails."""
        return SupervisorConfig(
            strategy=SupervisionStrategy.RESTART,
            multi_child=MultiChildStrategy.ONE_FOR_ALL,
        )


def supervised(
    strategy: SupervisionStrategy = SupervisionStrategy.RESTART,
    multi_child: MultiChildStrategy = MultiChildStrategy.ONE_FOR_ONE,
    max_restarts: int = 3,
    within_seconds: float = 60.0,
    backoff_initial: float = 0.1,
    backoff_max: float = 30.0,
    backoff_multiplier: float = 2.0,
) -> Callable[[type["Actor[Any]"]], type["Actor[Any]"]]:
    """Decorator to configure supervision for an actor class.

    Usage:
        @supervised(strategy=SupervisionStrategy.RESTART, max_restarts=5)
        class MyActor(Actor[MyMsg]):
            async def receive(self, msg, ctx):
                ...

    Args:
        strategy: How to handle individual failures
        multi_child: How to handle failures with multiple children
        max_restarts: Maximum restarts allowed within the time window
        within_seconds: Time window for counting restarts
        backoff_initial: Initial backoff delay in seconds
        backoff_max: Maximum backoff delay in seconds
        backoff_multiplier: Multiplier for exponential backoff

    Returns:
        Decorated actor class with supervision_config attribute
    """

    def decorator(cls: type["Actor[Any]"]) -> type["Actor[Any]"]:
        cls.supervision_config = SupervisorConfig(  # type: ignore[attr-defined]
            strategy=strategy,
            multi_child=multi_child,
            max_restarts=max_restarts,
            within_seconds=within_seconds,
            backoff_initial=backoff_initial,
            backoff_max=backoff_max,
            backoff_multiplier=backoff_multiplier,
        )
        return cls

    return decorator


# Internal supervision tree management


@dataclass
class RestartRecord:
    """Tracks restart history for an actor."""

    actor_id: "ActorId"
    restart_times: list[float] = field(default_factory=list)
    current_backoff: float = 0.0
    consecutive_failures: int = 0

    def record_restart(self, backoff: float) -> None:
        """Record a restart attempt."""
        self.restart_times.append(time.time())
        self.current_backoff = backoff
        self.consecutive_failures += 1

    def record_success(self) -> None:
        """Record successful message processing (reset failure count)."""
        self.consecutive_failures = 0
        self.current_backoff = 0.0

    def restarts_in_window(self, within_seconds: float) -> int:
        """Count restarts within the time window."""
        cutoff = time.time() - within_seconds
        return sum(1 for t in self.restart_times if t >= cutoff)

    def exceeds_limit(self, config: SupervisorConfig) -> bool:
        """Check if restart limit has been exceeded."""
        return self.restarts_in_window(config.within_seconds) >= config.max_restarts

    def calculate_next_backoff(self, config: SupervisorConfig) -> float:
        """Calculate the next backoff delay."""
        if self.current_backoff == 0:
            return config.backoff_initial
        return min(
            self.current_backoff * config.backoff_multiplier,
            config.backoff_max,
        )


@dataclass
class SupervisionNode:
    """Node in the supervision tree."""

    actor_id: "ActorId"
    actor_ref: "ActorRef[Any]"
    actor_instance: "Actor[Any]"
    actor_cls: type["Actor[Any]"]
    kwargs: dict[str, Any]

    # Hierarchy
    parent: "SupervisionNode | None" = None
    children: dict["ActorId", "SupervisionNode"] = field(default_factory=dict)
    spawn_order: list["ActorId"] = field(default_factory=list)

    # Configuration and state
    config: SupervisorConfig = field(default_factory=SupervisorConfig)
    restart_record: RestartRecord | None = None


class SupervisionTreeManager:
    """Manages the supervision tree structure.

    Tracks parent-child relationships and provides methods for
    querying the tree structure.
    """

    def __init__(self) -> None:
        self._nodes: dict["ActorId", SupervisionNode] = {}
        self._root_actors: set["ActorId"] = set()
        self._lock = asyncio.Lock()

    async def register(
        self,
        actor_id: "ActorId",
        actor_ref: "ActorRef[Any]",
        actor_instance: "Actor[Any]",
        actor_cls: type["Actor[Any]"],
        kwargs: dict[str, Any],
        config: SupervisorConfig,
        parent_id: "ActorId | None" = None,
    ) -> SupervisionNode:
        """Register an actor in the supervision tree.

        Args:
            actor_id: The actor's unique identifier
            actor_ref: Reference to the actor
            actor_instance: The actor instance
            actor_cls: The actor class (for restart)
            kwargs: Constructor arguments (for restart)
            config: Supervision configuration
            parent_id: Parent actor's ID (None for root actors)

        Returns:
            The created SupervisionNode
        """
        async with self._lock:
            node = SupervisionNode(
                actor_id=actor_id,
                actor_ref=actor_ref,
                actor_instance=actor_instance,
                actor_cls=actor_cls,
                kwargs=kwargs,
                config=config,
            )

            self._nodes[actor_id] = node

            if parent_id is not None and parent_id in self._nodes:
                parent_node = self._nodes[parent_id]
                node.parent = parent_node
                node.restart_record = RestartRecord(actor_id)
                parent_node.children[actor_id] = node
                parent_node.spawn_order.append(actor_id)
            else:
                self._root_actors.add(actor_id)
                node.restart_record = RestartRecord(actor_id)

            return node

    async def unregister(self, actor_id: "ActorId") -> SupervisionNode | None:
        """Remove an actor from the supervision tree.

        Args:
            actor_id: The actor to remove

        Returns:
            The removed node, or None if not found
        """
        async with self._lock:
            node = self._nodes.pop(actor_id, None)
            if node is None:
                return None

            # Remove from parent
            if node.parent:
                node.parent.children.pop(actor_id, None)
                if actor_id in node.parent.spawn_order:
                    node.parent.spawn_order.remove(actor_id)
            else:
                self._root_actors.discard(actor_id)

            # Re-parent orphaned children to root
            for child_id, child_node in node.children.items():
                child_node.parent = None
                self._root_actors.add(child_id)

            return node

    def get_node(self, actor_id: "ActorId") -> SupervisionNode | None:
        """Get a supervision node by actor ID."""
        return self._nodes.get(actor_id)

    def get_children(self, parent_id: "ActorId") -> list[SupervisionNode]:
        """Get all children of an actor."""
        parent = self._nodes.get(parent_id)
        if not parent:
            return []
        return list(parent.children.values())

    def get_children_after(
        self,
        parent_id: "ActorId",
        after_child_id: "ActorId",
    ) -> list[SupervisionNode]:
        """Get children spawned after a specific child (for REST_FOR_ONE).

        Args:
            parent_id: The parent actor's ID
            after_child_id: Get children spawned after this one (inclusive)

        Returns:
            List of child nodes in spawn order
        """
        parent = self._nodes.get(parent_id)
        if not parent:
            return []

        try:
            idx = parent.spawn_order.index(after_child_id)
            children_to_return = parent.spawn_order[idx:]
            return [
                parent.children[cid]
                for cid in children_to_return
                if cid in parent.children
            ]
        except ValueError:
            return []

    def get_path_to_root(self, actor_id: "ActorId") -> list["ActorId"]:
        """Get the path from an actor to the root of its supervision tree.

        Args:
            actor_id: The starting actor

        Returns:
            List of actor IDs from actor_id up to root (inclusive)
        """
        path = []
        current = self._nodes.get(actor_id)
        while current:
            path.append(current.actor_id)
            current = current.parent
        return path

    @property
    def root_actors(self) -> frozenset["ActorId"]:
        """Get all root actors (actors without parents)."""
        return frozenset(self._root_actors)

    def __len__(self) -> int:
        """Get total number of actors in the tree."""
        return len(self._nodes)


class ActorStopSignal(Exception):
    """Internal signal to stop an actor gracefully."""

    pass
