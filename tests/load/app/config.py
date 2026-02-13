from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    node_index: int
    seed_ips: list[str]
    casty_port: int
    http_port: int
    node_count: int

    @staticmethod
    def from_env() -> AppConfig:
        node_index = int(os.environ.get("NODE_INDEX", "0"))
        seed_ips_raw = os.environ.get("SEED_IPS", "127.0.0.1")
        seed_ips = [ip.strip() for ip in seed_ips_raw.split(",") if ip.strip()]
        casty_port = int(os.environ.get("CASTY_PORT", "25520"))
        http_port = int(os.environ.get("HTTP_PORT", "8000"))
        node_count = int(os.environ.get("NODE_COUNT", "5"))
        return AppConfig(
            node_index=node_index,
            seed_ips=seed_ips,
            casty_port=casty_port,
            http_port=http_port,
            node_count=node_count,
        )

    @property
    def host_ip(self) -> str:
        hostname = f"node-{self.node_index}"
        try:
            return socket.gethostbyname(hostname)
        except socket.gaierror:
            return "127.0.0.1"

    @property
    def seed_nodes(self) -> list[tuple[str, int]]:
        if self.node_index == 0:
            return []
        seed = self.seed_ips[0]
        try:
            seed = socket.gethostbyname(seed)
        except socket.gaierror:
            pass
        return [(seed, self.casty_port)]
