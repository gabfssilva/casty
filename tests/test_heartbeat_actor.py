from __future__ import annotations

import asyncio
from typing import Any

from casty import ActorSystem, Behaviors
from casty.cluster_state import NodeAddress
from casty.failure_detector import PhiAccrualFailureDetector
from casty._heartbeat_actor import (
    heartbeat_actor,
    HeartbeatRequest,
    HeartbeatResponse,
)


async def test_heartbeat_responds_to_request() -> None:
    """HeartbeatActor responds to HeartbeatRequest with HeartbeatResponse."""
    responses: list[HeartbeatResponse] = []

    async def collector_handler(ctx: Any, msg: HeartbeatResponse) -> Any:
        responses.append(msg)
        return Behaviors.same()

    async with ActorSystem(name="test") as system:
        hb_ref = system.spawn(
            heartbeat_actor(
                self_node=NodeAddress(host="127.0.0.1", port=25520),
                detector=PhiAccrualFailureDetector(),
            ),
            "heartbeat",
        )
        collector_ref = system.spawn(Behaviors.receive(collector_handler), "collector")
        await asyncio.sleep(0.1)

        hb_ref.tell(HeartbeatRequest(
            from_node=NodeAddress(host="127.0.0.2", port=25520),
            reply_to=collector_ref,
        ))
        await asyncio.sleep(0.1)

    assert len(responses) == 1
    assert responses[0].from_node == NodeAddress(host="127.0.0.1", port=25520)
