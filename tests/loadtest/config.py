from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class LoadTestConfig:
    target_url: str
    ssh_key: str
    ssh_user: str
    num_workers: int
    warmup: int
    duration: int
    no_faults: bool

    @staticmethod
    def from_args(args: argparse.Namespace) -> LoadTestConfig:
        return LoadTestConfig(
            target_url=args.target_url,
            ssh_key=args.ssh_key or "",
            ssh_user=args.ssh_user,
            num_workers=args.num_workers,
            warmup=args.warmup,
            duration=args.duration,
            no_faults=args.no_faults,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Casty Load + Resilience Test Suite",
    )
    parser.add_argument(
        "--target-url",
        required=True,
        help="Cluster URL (e.g. ALB URL). Node IPs are auto-discovered via /cluster/status",
    )
    parser.add_argument(
        "--ssh-key",
        help="Path to SSH private key for fault injection",
    )
    parser.add_argument(
        "--ssh-user",
        default="ec2-user",
        help="SSH username (default: ec2-user)",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=50,
        help="Number of concurrent HTTP workers (default: 50)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=30,
        help="Warmup duration in seconds (default: 30)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=480,
        help="Total test duration in seconds (default: 480)",
    )
    parser.add_argument(
        "--no-faults",
        action="store_true",
        help="Run load only, skip fault injection scenarios",
    )
    return parser
