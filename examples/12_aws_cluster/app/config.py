from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    node_index: int
    seed_ips: list[str]
    casty_port: int
    http_port: int

    @staticmethod
    def from_env() -> AppConfig:
        node_index = int(os.environ.get("NODE_INDEX", "0"))
        seed_ips_raw = os.environ.get("SEED_IPS", "127.0.0.1")
        seed_ips = [ip.strip() for ip in seed_ips_raw.split(",") if ip.strip()]
        casty_port = int(os.environ.get("CASTY_PORT", "25520"))
        http_port = int(os.environ.get("HTTP_PORT", "8000"))
        return AppConfig(
            node_index=node_index,
            seed_ips=seed_ips,
            casty_port=casty_port,
            http_port=http_port,
        )

    @property
    def host_ip(self) -> str:
        """IP of this node: 10.0.1.(10 + node_index)."""
        return f"10.0.1.{10 + self.node_index}"

    @property
    def seed_nodes(self) -> list[tuple[str, int]]:
        """Seed node addresses for ClusteredActorSystem."""
        if self.node_index == 0:
            return []
        return [(self.seed_ips[0], self.casty_port)]
