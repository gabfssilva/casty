from __future__ import annotations

import asyncio
import logging
import sys
import time

from .config import LoadTestConfig, build_parser

log = logging.getLogger("loadtest")


def setup_logging() -> None:
    ts = int(time.time())
    log_file = f"loadtest-{ts}.log"

    root = logging.getLogger("loadtest")
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(fmt)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    root.addHandler(stdout_handler)
    root.addHandler(file_handler)

    log.info("Logging to %s", log_file)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.no_faults and not args.ssh_key:
        print(
            "Error: --ssh-key is required for fault injection (use --no-faults to skip)",
            file=sys.stderr,
        )
        sys.exit(1)

    setup_logging()
    cfg = LoadTestConfig.from_args(args)

    log.info("=== Casty Load + Resilience Test ===")
    log.info("Target: %s", cfg.target_url)
    log.info("Workers: %d | Duration: %ds | Warmup: %ds", cfg.num_workers, cfg.duration, cfg.warmup)
    if cfg.no_faults:
        log.info("Mode: load only (no fault injection)")
    else:
        log.info("Mode: load + fault injection")

    from .timeline import run_timeline

    asyncio.run(run_timeline(cfg))


main()
