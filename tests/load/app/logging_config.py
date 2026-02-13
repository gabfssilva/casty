from __future__ import annotations

import logging
import os


def configure_logging() -> None:
    level = os.environ.get("CASTY_LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-40s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    casty_loggers = [
        "casty.cluster",
        "casty.topology",
        "casty.coordinator",
        "casty.region",
        "casty.replica",
        "casty.shard_proxy",
        "casty.remote_transport",
        "casty.tcp",
        "casty.transport",
        "casty.system",
        "casty.actor",
        "casty.sharding",
        "casty.singleton",
    ]
    for name in casty_loggers:
        logging.getLogger(name).setLevel(getattr(logging, level))

    logging.getLogger("casty.loadtest").setLevel(logging.DEBUG)

    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)
