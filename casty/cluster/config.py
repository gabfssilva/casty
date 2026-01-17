from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ClusterConfig:
    bind_host: str = "0.0.0.0"
    bind_port: int = 7946
    advertise_host: str | None = None
    advertise_port: int | None = None
    node_id: str | None = None
    seeds: list[str] = field(default_factory=list)

    protocol_period: float = 1.0
    ping_timeout: float = 0.5
    ping_req_timeout: float = 1.0
    ping_req_fanout: int = 3
    suspicion_mult: int = 5

    virtual_nodes: int = 150

    @classmethod
    def development(cls) -> ClusterConfig:
        return cls(
            protocol_period=0.3,
            ping_timeout=0.2,
            ping_req_timeout=0.5,
            suspicion_mult=3,
        )

    @classmethod
    def production(cls) -> ClusterConfig:
        return cls(
            protocol_period=1.0,
            ping_timeout=0.5,
            ping_req_timeout=1.0,
            suspicion_mult=5,
        )
