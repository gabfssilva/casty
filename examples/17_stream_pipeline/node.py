"""Stream Pipeline — Cluster Node
=================================

Each node spawns a ``stream_producer`` discoverable under ``SENSOR_KEY``
and pushes ``SensorReading`` values at ~1 Hz for ~20 seconds.

If ``--alert-monitor`` is passed, the node also spawns an
``alert_monitor`` actor discoverable under ``ALERT_MONITOR_KEY``.
The alert monitor receives ``ConsumeAlerts`` from the client,
subscribes to the remote alert stream, and logs incoming alerts.

Usage (local, 3-node cluster):
    uv run python examples/17_stream_pipeline/node.py --port 25520 --sensor temp --alert-monitor
    uv run python examples/17_stream_pipeline/node.py --port 25521 --seed 127.0.0.1:25520 --sensor humidity
    uv run python examples/17_stream_pipeline/node.py --port 25522 --seed 127.0.0.1:25520 --sensor pressure
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import socket
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from messages import (
    ALERT_MONITOR_KEY,
    SENSOR_KEY,
    SENSOR_RANGES,
    SENSOR_UNITS,
    Alert,
    ConsumeAlerts,
    SensorReading,
)

from casty import (
    Behavior,
    Behaviors,
    GetSink,
    GetSource,
    SinkRef,
    SourceRef,
    stream_consumer,
    stream_producer,
)
from casty.config import load_config
from casty.cluster.system import ClusteredActorSystem

# Suppress noisy topology/transport logs
logging.getLogger("casty.cluster").setLevel(logging.WARNING)
logging.getLogger("casty.remote").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
MAGENTA = "\033[35m"
LEVEL_COLORS = {
    logging.DEBUG: DIM,
    logging.INFO: GREEN,
    logging.WARNING: YELLOW,
    logging.ERROR: RED,
    logging.CRITICAL: "\033[1;31m",
}


class ColorFormatter(logging.Formatter):
    def __init__(self) -> None:
        self.node = socket.gethostname()

    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        color = LEVEL_COLORS.get(record.levelno, "")
        level = record.levelname.ljust(7)
        return (
            f"{DIM}{ts}{RESET}  "
            f"{color}{level}{RESET} "
            f"{CYAN}{self.node}{RESET}  "
            f"{record.getMessage()}"
        )


# ---------------------------------------------------------------------------
# Alert monitor
# ---------------------------------------------------------------------------


def alert_receiver(
    queue: asyncio.Queue[Any],
) -> Behavior[ConsumeAlerts]:
    """Discoverable actor that receives the client's producer ref
    and hands it to the main loop for stream consumption."""

    async def receive(ctx: Any, msg: ConsumeAlerts) -> Behavior[ConsumeAlerts]:
        match msg:
            case ConsumeAlerts(producer=producer):
                log.info(
                    "%sAlert monitor%s received producer ref",
                    MAGENTA,
                    RESET,
                )
                await queue.put(producer)
                return Behaviors.same()
        return Behaviors.unhandled()

    return Behaviors.receive(receive)


async def consume_alerts(
    system: ClusteredActorSystem,
    queue: asyncio.Queue[Any],
) -> None:
    """Wait for a producer ref, spawn a stream_consumer, iterate alerts."""
    producer_ref = await queue.get()
    log.info(
        "%sAlert monitor%s subscribing to alert stream",
        MAGENTA,
        RESET,
    )

    consumer = system.spawn(
        stream_consumer(producer_ref, timeout=30.0), "alert-consumer"
    )
    source: SourceRef[Alert] = await system.ask(
        consumer, lambda r: GetSource(reply_to=r), timeout=5.0
    )

    count = 0
    async for alert in source:
        count += 1
        log.warning(
            "%sALERT #%d%s  sensor=%s%s%s  value=%.1f  threshold=%.1f",
            RED,
            count,
            RESET,
            YELLOW,
            alert.sensor,
            RESET,
            alert.value,
            alert.threshold,
        )

    log.info("Alert stream completed (%d alerts)", count)


# ---------------------------------------------------------------------------
# Sensor publishing loop
# ---------------------------------------------------------------------------


async def publish_readings(
    sink: SinkRef[SensorReading],
    sensor: str,
    node_name: str,
) -> None:
    lo, hi = SENSOR_RANGES[sensor]
    unit = SENSOR_UNITS[sensor]

    for i in range(20):
        value = round(random.uniform(lo, hi), 2)
        reading = SensorReading(
            sensor=sensor,
            value=value,
            timestamp=time.time(),
            node=node_name,
        )
        await sink.put(reading)
        log.info(
            "Pushed %s%s%s = %s%.2f %s%s  (%d/20)",
            YELLOW,
            sensor,
            RESET,
            CYAN,
            value,
            unit,
            RESET,
            i + 1,
        )
        await asyncio.sleep(random.uniform(0.8, 1.2))

    await sink.complete()
    log.info("Sensor %s%s%s stream completed", YELLOW, sensor, RESET)


# ---------------------------------------------------------------------------
# Node entry point
# ---------------------------------------------------------------------------


async def run_node(
    host: str,
    port: int,
    bind_host: str,
    seed_nodes: list[tuple[str, int]] | None,
    num_nodes: int,
    sensor: str,
    run_alert_monitor: bool,
) -> None:
    config = load_config(Path(__file__).parent / "casty.toml")
    node_id = socket.gethostname()

    log.info("Starting node (sensor=%s%s%s) ...", YELLOW, sensor, RESET)

    async with ClusteredActorSystem.from_config(
        config,
        host=host,
        port=port,
        node_id=node_id,
        seed_nodes=seed_nodes,
        bind_host=bind_host,
    ) as system:
        await system.wait_for(num_nodes)
        log.info(
            "Cluster ready (%s%d%s nodes)",
            GREEN,
            num_nodes,
            RESET,
        )

        producer_ref = system.spawn(
            Behaviors.discoverable(stream_producer(), key=SENSOR_KEY),
            f"sensor-{sensor}",
        )
        sink: SinkRef[SensorReading] = await system.ask(
            producer_ref, lambda r: GetSink(reply_to=r), timeout=5.0
        )

        alert_task: asyncio.Task[None] | None = None
        if run_alert_monitor:
            alert_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)
            system.spawn(
                Behaviors.discoverable(
                    alert_receiver(alert_queue), key=ALERT_MONITOR_KEY
                ),
                "alert-monitor",
            )
            log.info("%sAlert monitor%s spawned", MAGENTA, RESET)
            alert_task = asyncio.create_task(consume_alerts(system, alert_queue))

        await publish_readings(sink, sensor, node_id)

        log.info("Keeping node alive for drain ...")
        await asyncio.sleep(5.0)

        if alert_task and not alert_task.done():
            alert_task.cancel()

    log.info("Shutdown")


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty Stream Pipeline — Node")
    parser.add_argument("--port", type=int, default=25520)
    parser.add_argument(
        "--host",
        default=None,
        help="Identity hostname. Use 'auto' for container IP. Default: 127.0.0.1",
    )
    parser.add_argument(
        "--bind-host",
        default=None,
        help="Address to bind TCP listener. Default: same as --host",
    )
    parser.add_argument(
        "--seed",
        default=None,
        help="Seed node address (host:port) to join an existing cluster",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=3,
        help="Wait for N nodes before starting (default: 3)",
    )
    parser.add_argument(
        "--sensor",
        required=True,
        choices=["temp", "humidity", "pressure"],
        help="Sensor type this node produces",
    )
    parser.add_argument(
        "--alert-monitor",
        action="store_true",
        help="Spawn an alert monitor actor on this node",
    )
    args = parser.parse_args()

    match args.host:
        case None:
            host = "127.0.0.1"
        case "auto":
            host = socket.gethostbyname(socket.gethostname())
        case h:
            host = h

    bind_host: str = args.bind_host or host

    seed_nodes: list[tuple[str, int]] | None = None
    if args.seed:
        seed_host, seed_port_str = args.seed.rsplit(":", maxsplit=1)
        seed_nodes = [(seed_host, int(seed_port_str))]

    asyncio.run(
        run_node(
            host,
            args.port,
            bind_host,
            seed_nodes,
            args.nodes,
            args.sensor,
            args.alert_monitor,
        )
    )


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    main()
