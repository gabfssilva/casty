"""SWIM+ Protocol - Scalable Weakly-consistent Infection-style Membership.

This module implements the SWIM+ protocol with Lifeguard extensions
using the Casty actor model.

Features:
- Failure Detection: Ping/Ack/Ping-Req with timeouts
- Lifeguard Suspicion: Adaptive timeout with logarithmic decay
- Round-Robin Probing: Deterministic target selection
- Protocol Period Adaptation: Dynamic adjustment based on network
- λ log n Dissemination: Via integration with casty.gossip
- Buddy System: Direct notification of suspected nodes

Example:
    ```python
    from casty import ActorSystem
    from casty.swim import SwimCoordinator, SwimConfig

    config = SwimConfig.production().with_bind_address("0.0.0.0", 7946)

    async with ActorSystem() as system:
        swim = await system.spawn(
            SwimCoordinator,
            node_id="node-1",
            config=config,
        )

        # Subscribe to membership events
        await swim.send(Subscribe(my_handler))

        # Query membership
        members = await swim.ask(GetMembers())
    ```
"""

# Configuration
from .config import SwimConfig

# State types
from .state import (
    MemberState,
    MemberInfo,
    SuspicionState,
    ProbeList,
)

# Messages - Public API
from .messages import (
    # Membership queries
    Subscribe,
    Unsubscribe,
    GetMembers,
    Leave,
    Join,
    # Membership events
    MemberJoined,
    MemberLeft,
    MemberSuspected,
    MemberFailed,
    MemberAlive,
    MembershipEvent,
    # Wire protocol types
    GossipType,
    GossipEntry,
)

# Actors
from .coordinator import SwimCoordinator
from .member import MemberActor
from .suspicion import SuspicionActor
from .adaptive import AdaptiveTimerActor

# Codec
from .codec import SwimCodec

__all__ = [
    # Configuration
    "SwimConfig",
    # State types
    "MemberState",
    "MemberInfo",
    "SuspicionState",
    "ProbeList",
    # Messages
    "Subscribe",
    "Unsubscribe",
    "GetMembers",
    "Leave",
    "Join",
    "MemberJoined",
    "MemberLeft",
    "MemberSuspected",
    "MemberFailed",
    "MemberAlive",
    "MembershipEvent",
    "GossipType",
    "GossipEntry",
    # Actors
    "SwimCoordinator",
    "MemberActor",
    "SuspicionActor",
    "AdaptiveTimerActor",
    # Codec
    "SwimCodec",
]
