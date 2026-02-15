"""Cluster membership management and coordination.

Provides ``ClusterConfig`` for cluster setup and ``Cluster`` as a thin wrapper
around the internal cluster actor.  The cluster actor spawns a ``topology_actor``
that owns all membership state, gossip, heartbeat, and failure detection.
"""

# src/casty/cluster.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.core.address import ActorAddress
from casty.cluster.state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    NodeId,
)
from casty.cluster.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef
from casty.core.scheduler import (
    CancelSchedule,
    ScheduleOnce,
    SchedulerMsg,
    ScheduleTick,
)
from casty.cluster.topology import SubscribeTopology, TopologySnapshot
from casty.cluster.topology_actor import (
    CheckAvailability,
    GetState as TopologyGetState,
    GossipTick,
    HeartbeatTick,
    JoinRequest,
    TopologyMsg,
    WaitForMembers as TopologyWaitForMembers,
    topology_actor,
)

if TYPE_CHECKING:
    from casty.config import FailureDetectorConfig
    from casty.context import ActorContext
    from casty.core.event_stream import EventStreamMsg
    from casty.core.transport import LocalTransport
    from casty.remote.tcp_transport import RemoteTransport
    from casty.core.system import ActorSystem


# --- ClusterCmd messages ---


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class WaitForMembers:
    n: int
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class GetReceptionist:
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class JoinRetry:
    pass


type ClusterCmd = (
    GetState | WaitForMembers
    | GetReceptionist | JoinRetry | TopologySnapshot
)


# --- ClusterConfig ---


@dataclass(frozen=True)
class ClusterConfig:
    """Configuration for joining or forming a cluster.

    Parameters
    ----------
    host : str
        Bind address for this node.
    port : int
        TCP port for this node.
    seed_nodes : tuple[tuple[str, int], ...]
        Initial contact points for cluster formation.  An empty tuple means
        this node forms a new single-node cluster.
    node_id : NodeId
        Human-readable identifier for this node (e.g. ``"worker-1"``).
    roles : frozenset[str]
        Roles to advertise for this node.

    Examples
    --------
    >>> cfg = ClusterConfig("127.0.0.1", 2551, node_id="node-1", seed_nodes=(("127.0.0.1", 2552),))
    >>> cfg.host, cfg.port
    ('127.0.0.1', 2551)
    """

    host: str
    port: int
    node_id: NodeId
    seed_nodes: tuple[tuple[str, int], ...] = ()
    roles: frozenset[str] = field(default_factory=lambda: frozenset[str]())


# --- cluster_actor behavior ---


def cluster_actor(
    *,
    config: ClusterConfig,
    scheduler_ref: ActorRef[SchedulerMsg],
    remote_transport: RemoteTransport | None = None,
    local_transport: LocalTransport | None = None,
    system_name: str = "",
    gossip_interval: float = 1.0,
    gossip_fanout: int = 3,
    heartbeat_interval: float = 0.5,
    availability_interval: float = 2.0,
    failure_detector_config: FailureDetectorConfig | None = None,
    event_stream: ActorRef[EventStreamMsg] | None = None,
) -> Behavior[ClusterCmd]:
    async def setup(ctx: ActorContext[ClusterCmd]) -> Behavior[ClusterCmd]:
        self_node = NodeAddress(host=config.host, port=config.port)
        initial_member = Member(
            address=self_node, status=MemberStatus.up, roles=config.roles, id=config.node_id
        )
        initial_state = ClusterState().add_member(initial_member)

        logger = logging.getLogger(f"casty.cluster.{system_name}")

        from casty.config import FailureDetectorConfig as _FDConfig

        fd = failure_detector_config or _FDConfig()
        detector = PhiAccrualFailureDetector(
            threshold=fd.threshold,
            max_sample_size=fd.max_sample_size,
            min_std_deviation_ms=fd.min_std_deviation_ms,
            acceptable_heartbeat_pause_ms=fd.acceptable_heartbeat_pause_ms,
            first_heartbeat_estimate_ms=fd.first_heartbeat_estimate_ms,
        )

        topo_behavior = topology_actor(
            self_node=self_node,
            node_id=config.node_id,
            roles=config.roles,
            initial_state=initial_state,
            detector=detector,
            remote_transport=remote_transport,
            system_name=system_name,
            fanout=gossip_fanout,
            logger=logging.getLogger(f"casty.topology.{system_name}"),
        )
        topo_ref: ActorRef[TopologyMsg] = ctx.spawn(topo_behavior, "_topology")

        from casty.cluster.receptionist import receptionist_actor

        recep_behavior: Behavior[Any] = receptionist_actor(
            self_node=self_node,
            gossip_ref=topo_ref,  # type: ignore[arg-type]
            remote_transport=remote_transport,
            system_name=system_name,
            event_stream=event_stream,
            topology_ref=topo_ref,  # type: ignore[arg-type]
        )
        receptionist_ref: ActorRef[Any] = ctx.spawn(recep_behavior, "_receptionist")

        # Build seed refs pointing to remote topology actors
        seed_refs: list[ActorRef[TopologyMsg]] = []
        if remote_transport is not None:
            for seed_host, seed_port in config.seed_nodes:
                seed_node = NodeAddress(host=seed_host, port=seed_port)
                if seed_node == self_node:
                    continue
                seed_addr = ActorAddress(
                    system=system_name,
                    path="/_cluster/_topology",
                    host=seed_host,
                    port=seed_port,
                )
                seed_refs.append(
                    remote_transport.make_ref(seed_addr)  # type: ignore[arg-type]
                )

        # Schedule periodic ticks — all targeting topology_actor
        scheduler_ref.tell(ScheduleTick(
            key="gossip", target=topo_ref, message=GossipTick(), interval=gossip_interval,
        ))
        scheduler_ref.tell(ScheduleTick(
            key="heartbeat",
            target=topo_ref,
            message=HeartbeatTick(members=frozenset()),
            interval=heartbeat_interval,
        ))
        scheduler_ref.tell(ScheduleTick(
            key="availability",
            target=topo_ref,
            message=CheckAvailability(reply_to=topo_ref),  # type: ignore[arg-type]
            interval=availability_interval,
        ))

        # Subscribe to topology so we know when join succeeds
        if seed_refs:
            topo_ref.tell(SubscribeTopology(reply_to=ctx.self))  # type: ignore[arg-type]

        # Schedule join retry for multi-node clusters
        if seed_refs:
            scheduler_ref.tell(ScheduleOnce(
                key="join-retry", target=ctx.self, message=JoinRetry(), delay=0.0,
            ))

        async def cancel_schedules(ctx: ActorContext[ClusterCmd]) -> None:
            scheduler_ref.tell(CancelSchedule(key="gossip"))
            scheduler_ref.tell(CancelSchedule(key="heartbeat"))
            scheduler_ref.tell(CancelSchedule(key="availability"))
            scheduler_ref.tell(CancelSchedule(key="join-retry"))

        logger.info("Cluster started %s:%d (seeds=%d)", self_node.host, self_node.port, len(config.seed_nodes))

        return Behaviors.with_lifecycle(
            ready(
                topo_ref=topo_ref,
                receptionist_ref=receptionist_ref,
                seed_refs=tuple(seed_refs),
                self_node=self_node,
                node_id=config.node_id,
                roles=config.roles,
                scheduler_ref=scheduler_ref,
                logger=logger,
                joined=not seed_refs,
            ),
            post_stop=cancel_schedules,
        )

    return Behaviors.setup(setup)


def ready(
    *,
    topo_ref: ActorRef[TopologyMsg],
    receptionist_ref: ActorRef[Any],
    seed_refs: tuple[ActorRef[TopologyMsg], ...],
    self_node: NodeAddress,
    node_id: NodeId,
    roles: frozenset[str],
    scheduler_ref: ActorRef[SchedulerMsg],
    logger: logging.Logger,
    joined: bool,
) -> Behavior[ClusterCmd]:
    async def receive(
        ctx: ActorContext[ClusterCmd], msg: ClusterCmd,
    ) -> Behavior[ClusterCmd]:
        match msg:
            case GetState(reply_to=reply_to):
                topo_ref.tell(TopologyGetState(reply_to=reply_to))
                return Behaviors.same()

            case WaitForMembers(n=n, reply_to=reply_to):
                topo_ref.tell(TopologyWaitForMembers(n=n, reply_to=reply_to))
                return Behaviors.same()

            case GetReceptionist(reply_to=reply_to):
                reply_to.tell(receptionist_ref)
                return Behaviors.same()

            case TopologySnapshot(members=members) if not joined:
                if len(members) > 1:
                    logger.debug("Join confirmed (members=%d), stopping retries", len(members))
                    scheduler_ref.tell(CancelSchedule(key="join-retry"))
                    return ready(
                        topo_ref=topo_ref,
                        receptionist_ref=receptionist_ref,
                        seed_refs=seed_refs,
                        self_node=self_node,
                        node_id=node_id,
                        roles=roles,
                        scheduler_ref=scheduler_ref,
                        logger=logger,
                        joined=True,
                    )
                return Behaviors.same()

            case JoinRetry() if joined:
                return Behaviors.same()

            case JoinRetry():
                logger.debug("Join retry (%d seeds)", len(seed_refs))
                for seed_ref in seed_refs:
                    seed_ref.tell(
                        JoinRequest(
                            node=self_node,
                            roles=roles,
                            node_id=node_id,
                            reply_to=topo_ref,  # type: ignore[arg-type]
                        )
                    )
                scheduler_ref.tell(
                    ScheduleOnce(
                        key="join-retry",
                        target=ctx.self,
                        message=JoinRetry(),
                        delay=1.0,
                    )
                )
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


# --- Cluster thin wrapper ---


class Cluster:
    """High-level handle for starting and querying a cluster on an ``ActorSystem``.

    Wraps the internal cluster actor, providing ``start``, ``get_state``, and
    ``shutdown`` methods.

    Parameters
    ----------
    system : ActorSystem
        The actor system that will host the cluster actor.
    config : ClusterConfig
        Cluster network and seed configuration.
    remote_transport : RemoteTransport or None
        Transport for cross-node communication.  ``None`` for local-only.
    system_name : str
        Logical name of the actor system (used in actor addresses).
    gossip_interval : float
        Seconds between gossip rounds.
    gossip_fanout : int
        Number of peers contacted per gossip round.
    heartbeat_interval : float
        Seconds between heartbeat state polls.
    availability_interval : float
        Seconds between failure-detector availability checks.
    failure_detector_config : FailureDetectorConfig or None
        Tuning for the phi accrual failure detector.

    Examples
    --------
    >>> cluster = Cluster(system, ClusterConfig("127.0.0.1", 2551, node_id="node-1"))
    >>> await cluster.start()
    >>> state = await cluster.get_state()
    >>> len(state.members)
    1
    """

    def __init__(
        self,
        system: ActorSystem,
        config: ClusterConfig,
        *,
        remote_transport: RemoteTransport | None = None,
        local_transport: LocalTransport | None = None,
        system_name: str = "",
        gossip_interval: float = 1.0,
        gossip_fanout: int = 3,
        heartbeat_interval: float = 0.5,
        availability_interval: float = 2.0,
        failure_detector_config: FailureDetectorConfig | None = None,
        event_stream: ActorRef[EventStreamMsg] | None = None,
    ) -> None:
        self._system = system
        self._config = config
        self._remote_transport = remote_transport
        self._local_transport = local_transport
        self._system_name = system_name
        self._gossip_interval = gossip_interval
        self._gossip_fanout = gossip_fanout
        self._heartbeat_interval = heartbeat_interval
        self._availability_interval = availability_interval
        self._failure_detector_config = failure_detector_config
        self._event_stream = event_stream
        self._ref: ActorRef[ClusterCmd] | None = None

    @property
    def ref(self) -> ActorRef[ClusterCmd]:
        """The cluster actor's ref.

        Returns
        -------
        ActorRef[ClusterCmd]
            Reference to the running cluster actor.

        Raises
        ------
        RuntimeError
            If the cluster has not been started yet.
        """
        if self._ref is None:
            msg = "Cluster not started"
            raise RuntimeError(msg)
        return self._ref

    async def start(self) -> None:
        """Spawn the cluster actor and begin membership protocol.

        After this method returns the cluster is actively gossiping and
        monitoring heartbeats.
        """
        behavior = cluster_actor(
            config=self._config,
            scheduler_ref=self._system.scheduler,
            remote_transport=self._remote_transport,
            local_transport=self._local_transport,
            system_name=self._system_name,
            gossip_interval=self._gossip_interval,
            gossip_fanout=self._gossip_fanout,
            heartbeat_interval=self._heartbeat_interval,
            availability_interval=self._availability_interval,
            failure_detector_config=self._failure_detector_config,
            event_stream=self._event_stream,
        )
        self._ref = self._system.spawn(behavior, "_cluster")

    async def get_state(self, *, timeout: float = 5.0) -> ClusterState:
        """Request the current cluster state from the topology actor.

        Parameters
        ----------
        timeout : float
            Maximum seconds to wait for a reply.

        Returns
        -------
        ClusterState
            The latest cluster state snapshot.
        """
        return await self._system.ask(
            self.ref,
            lambda r: GetState(reply_to=r),
            timeout=timeout,
        )

    async def get_receptionist(self, *, timeout: float = 5.0) -> ActorRef[Any]:
        """Ask the cluster actor for the receptionist ref."""
        return await self._system.ask(
            self.ref,
            lambda r: GetReceptionist(reply_to=r),
            timeout=timeout,
        )

    async def shutdown(self) -> None:
        """Gracefully shut down the cluster actor."""
        pass
