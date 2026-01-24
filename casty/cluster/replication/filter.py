from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, AsyncGenerator, Callable, Any

from casty.state import Stateful
from ..states import StoreState, StoreAck, ReplicationQuorumError

if TYPE_CHECKING:
    from casty.context import Context
    from casty.ref import ActorRef
    from casty.cluster.shard import ShardCoordinator

type MessageStream[M] = AsyncGenerator[tuple[M, Context], None]
type Filter[M] = Callable[[Any, MessageStream[M]], MessageStream[M]]


def leadership_filter[M](
    shard_coordinator: "ShardCoordinator",
    actor_id: str,
    replicas: int,
) -> Filter[M]:

    async def apply(
        state: Any,
        inner: MessageStream[M],
    ) -> MessageStream[M]:
        async for msg, ctx in inner:
            if not shard_coordinator.is_leader(actor_id, replicas):
                continue
            yield msg, ctx

    return apply


def replication_filter[M](
    states_refs: list["ActorRef"],
    write_quorum: int = 1,
) -> Filter[M]:

    async def apply(
        state: Any,
        inner: MessageStream[M],
    ) -> MessageStream[M]:
        async for msg, ctx in inner:
            yield msg, ctx

            if states_refs and state is not None:
                if isinstance(state, Stateful):
                    snapshot = state.snapshot()
                elif hasattr(state, 'snapshot'):
                    snapshot = state.snapshot()
                else:
                    snapshot = vars(state) if hasattr(state, '__dict__') else {}

                actor_id = ctx.self_id

                tasks = [
                    states_ref.ask(StoreState(actor_id, snapshot))
                    for states_ref in states_refs
                ]

                done = 0
                for coro in asyncio.as_completed(tasks):
                    try:
                        ack = await coro
                        if isinstance(ack, StoreAck) and ack.success:
                            done += 1
                            if done >= write_quorum:
                                break
                    except Exception:
                        pass

                if done < write_quorum:
                    raise ReplicationQuorumError(
                        f"Failed to replicate {actor_id}: got {done}/{write_quorum} acks"
                    )

    return apply
