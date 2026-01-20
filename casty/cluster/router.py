from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from casty import actor, Mailbox
from casty.ref import ActorRef
from casty.envelope import Envelope
from casty.serializable import deserialize, serialize
from .replication import ReplicationConfig, Routing
from .replication.messages import Replicate, ReplicateAck
from .transport_messages import Register, Deliver, Transmit, RegisterReplication, SpawnReplica, Connect, RegisterReplicators
from .messages import MembershipUpdate, GetResponsibleNodes, GetAliveMembers
from .registry import get_behavior

logger = logging.getLogger(__name__)


def select_target(
    replicas: list[str],
    config: ReplicationConfig,
    local_node: str,
    is_write: bool,
) -> str:
    leader = replicas[0]

    match config.routing:
        case Routing.LEADER:
            return leader

        case Routing.ANY:
            if is_write:
                if local_node in replicas:
                    return local_node
                return leader
            else:
                if local_node in replicas:
                    return local_node
                for node in replicas:
                    if node != local_node:
                        return node
                return leader

        case Routing.LOCAL_FIRST:
            if is_write:
                return leader
            else:
                if local_node in replicas:
                    return local_node
                return replicas[0]

    return leader


@dataclass
class RegisterPending:
    pending: dict[str, asyncio.Future[Any]]


@dataclass
class SetReferences:
    membership_ref: ActorRef | None = None
    outbound_ref: ActorRef | None = None


@actor
async def router_actor(
    *,
    mailbox: Mailbox[Register | RegisterPending | RegisterReplication | RegisterReplicators | Deliver | MembershipUpdate | SetReferences],
):
    from .replication import Replicator

    registry: dict[str, ActorRef] = {}
    pending_asks: dict[str, asyncio.Future[Any]] = {}
    replication_configs: dict[str, ReplicationConfig] = {}
    actor_leaders: dict[str, str] = {}
    membership_ref: ActorRef | None = None
    outbound_ref: ActorRef | None = None
    replicators: dict[str, Replicator] = {}

    async for msg, ctx in mailbox:
        match msg:
            case SetReferences(m_ref, o_ref):
                membership_ref = m_ref
                outbound_ref = o_ref

            case Register(ref):
                registry[ref.actor_id] = ref

            case RegisterPending(pending):
                pending_asks = pending

            case RegisterReplicators(reps):
                replicators = reps

            case RegisterReplication(actor_id, config):
                replication_configs[actor_id] = config

            case Deliver(data, reply_connection):
                envelope = deserialize(data)

                if isinstance(envelope.payload, SpawnReplica):
                    spawn = envelope.payload
                    if spawn.actor_id not in registry:
                        behavior = get_behavior(spawn.behavior_name)
                        if behavior:
                            actor_name = spawn.actor_id.split("/", 1)[1] if "/" in spawn.actor_id else spawn.actor_id
                            ref = await ctx._system.actor(behavior, name=actor_name)
                            registry[spawn.actor_id] = ref
                            replication_configs[spawn.actor_id] = spawn.config
                    continue

                if isinstance(envelope.payload, Replicate):
                    replicate = envelope.payload
                    actor_mailbox = ctx._system._mailboxes.get(replicate.actor_id)
                    logger.info(f"Replicate for {replicate.actor_id}: mailbox={'found' if actor_mailbox else 'NOT FOUND'}")
                    if actor_mailbox and actor_mailbox.state is not None:
                        actor_mailbox.state.restore(replicate.snapshot)
                        if reply_connection:
                            ack = ReplicateAck(
                                actor_id=replicate.actor_id,
                                version=replicate.version,
                                node_id=ctx.node_id,
                            )
                            ack_envelope = Envelope(payload=ack)
                            await reply_connection.send(Transmit(data=serialize(ack_envelope)))
                            logger.info(f"Sent ReplicateAck for {replicate.actor_id} v{replicate.version} from {ctx.node_id}")
                    continue

                if isinstance(envelope.payload, ReplicateAck):
                    ack = envelope.payload
                    replicator = replicators.get(ack.actor_id)
                    if replicator:
                        replicator.on_ack_received(ack.actor_id, ack.version, ack.node_id)
                        logger.info(f"Received ReplicateAck for {ack.actor_id} v{ack.version} from {ack.node_id}")
                    continue

                if envelope.correlation_id and envelope.correlation_id in pending_asks:
                    future = pending_asks.pop(envelope.correlation_id)
                    if not future.done():
                        future.set_result(envelope.payload)
                else:
                    ref = registry.get(envelope.target)
                    if ref:
                        if envelope.correlation_id and reply_connection:
                            reply_future: asyncio.Future[Any] = asyncio.Future()

                            async def send_reply(
                                fut: asyncio.Future[Any],
                                corr_id: str,
                                conn: ActorRef,
                            ):
                                try:
                                    result = await fut
                                    reply_envelope = Envelope(
                                        payload=result,
                                        correlation_id=corr_id,
                                    )
                                    reply_data = serialize(reply_envelope)
                                    await conn.send(Transmit(data=reply_data))
                                except asyncio.CancelledError:
                                    pass

                            asyncio.create_task(
                                send_reply(reply_future, envelope.correlation_id, reply_connection)
                            )
                            envelope.reply_to = reply_future

                        await ref.send_envelope(envelope)
                    elif membership_ref and outbound_ref and envelope.target:
                        nodes = await membership_ref.ask(GetResponsibleNodes(envelope.target, 1))
                        if nodes:
                            target_node = nodes[0]
                            members = await membership_ref.ask(GetAliveMembers())
                            if target_node in members:
                                address = members[target_node].address
                                conn = await outbound_ref.ask(Connect(node_id=target_node, address=address))
                                if conn:
                                    await conn.send(Transmit(data=data))

            case MembershipUpdate(node_id, status, _):
                if status == "down":
                    logger.info(f"Node {node_id} is DOWN, checking affected actors")

                    for actor_id, config in list(replication_configs.items()):
                        leader = actor_leaders.get(actor_id)
                        if leader == node_id:
                            logger.warning(f"Leader {node_id} down for actor {actor_id}")
