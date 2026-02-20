"""Stream Pipeline — Cluster Client
====================================

External ``ClusterClient`` that discovers sensor producers in the
cluster, spawns local ``stream_consumer`` actors, iterates each
``SourceRef`` in parallel, detects anomalies, and streams ``Alert``
messages back to a cluster-side ``alert_monitor``.

Usage:
    uv run python examples/17_stream_pipeline/client.py --contact 127.0.0.1:25520
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from messages import (
    ALERT_MONITOR_KEY,
    SENSOR_KEY,
    SENSOR_UNITS,
    THRESHOLDS,
    Alert,
    ConsumeAlerts,
    SensorReading,
)

from casty import (
    ClusterClient,
    GetSink,
    GetSource,
    ServiceInstance,
    SinkRef,
    SourceRef,
    StreamProducerMsg,
    stream_consumer,
    stream_producer,
)

# Suppress noisy topology/transport logs in the client
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
        self.node = "client"

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
# Stream consumption
# ---------------------------------------------------------------------------


async def consume_sensor(
    source: SourceRef[SensorReading],
    alert_sink: SinkRef[Alert],
    label: str,
) -> tuple[str, int, float, int]:
    """Iterate a single sensor stream, detect anomalies, return stats."""
    count = 0
    total = 0.0
    anomalies = 0

    async for reading in source:
        count += 1
        total += reading.value
        unit = SENSOR_UNITS.get(reading.sensor, "?")
        threshold = THRESHOLDS.get(reading.sensor, float("inf"))

        log.info(
            "%s%-10s%s  %s%.2f %s%s  from %s%s%s",
            YELLOW,
            reading.sensor,
            RESET,
            CYAN,
            reading.value,
            unit,
            RESET,
            DIM,
            reading.node,
            RESET,
        )

        if reading.value > threshold:
            anomalies += 1
            alert = Alert(
                sensor=reading.sensor,
                value=reading.value,
                threshold=threshold,
                timestamp=time.time(),
            )
            await alert_sink.put(alert)
            log.warning(
                "%sAnomaly%s  %s > %.1f %s",
                RED,
                RESET,
                reading.sensor,
                threshold,
                unit,
            )

    return label, count, total, anomalies


# ---------------------------------------------------------------------------
# Client entry point
# ---------------------------------------------------------------------------


async def run_client(
    contact_points: list[tuple[str, int]],
    expected_producers: int = 3,
) -> None:
    client_host = socket.gethostbyname(socket.gethostname())

    async with ClusterClient(
        contact_points=contact_points,
        system_name="sensors",
        client_host=client_host,
        client_port=0,
    ) as client:
        log.info("Connected to cluster, waiting for topology ...")
        await asyncio.sleep(3.0)

        # -- Discover sensor producers (wait for expected count) --
        producers: list[ServiceInstance[StreamProducerMsg[SensorReading]]] = []
        for attempt in range(30):
            sensor_listing = client.lookup(SENSOR_KEY)
            producers = list(sensor_listing.instances)
            if len(producers) >= expected_producers:
                break
            log.info(
                "Found %d/%d sensor producers (attempt %d/30), retrying ...",
                len(producers),
                expected_producers,
                attempt + 1,
            )
            await asyncio.sleep(2.0)

        if not producers:
            log.error("No sensor producers found after 30 attempts")
            return

        log.info(
            "Discovered %s%d%s sensor producer(s)",
            GREEN,
            len(producers),
            RESET,
        )

        # -- Spawn local alert producer and get its SinkRef --
        alert_producer = client.spawn(stream_producer(), "alert-producer")
        alert_sink: SinkRef[Alert] = await client.ask(
            alert_producer, lambda r: GetSink(reply_to=r), timeout=5.0
        )

        # -- Discover alert monitor (with retries) --
        for attempt in range(15):
            monitor_listing = client.lookup(ALERT_MONITOR_KEY)
            if monitor_listing and monitor_listing.instances:
                monitor_ref = next(iter(monitor_listing.instances)).ref
                monitor_ref.tell(ConsumeAlerts(producer=alert_producer))
                log.info("%sAlert monitor%s connected", MAGENTA, RESET)
                break
            log.info(
                "Alert monitor not found yet (attempt %d/15), retrying ...",
                attempt + 1,
            )
            await asyncio.sleep(2.0)
        else:
            log.warning("No alert monitor found after 15 attempts")

        # -- Spawn consumers for each sensor producer --
        tasks: list[asyncio.Task[tuple[str, int, float, int]]] = []

        for i, instance in enumerate(producers):
            producer_ref = instance.ref
            consumer_name = f"sensor-consumer-{i}"

            consumer_ref = client.spawn(
                stream_consumer(producer_ref, timeout=10.0),
                consumer_name,
            )
            source: SourceRef[SensorReading] = await client.ask(
                consumer_ref, lambda r: GetSource(reply_to=r), timeout=5.0
            )

            node_host = instance.node.host if instance.node else "unknown"
            node_port = instance.node.port if instance.node else 0
            sensor_label = f"sensor@{node_host}:{node_port}"

            task = asyncio.create_task(
                consume_sensor(source, alert_sink, sensor_label)
            )
            tasks.append(task)

        log.info("Consuming %s%d%s stream(s) ...", GREEN, len(tasks), RESET)

        # -- Wait for all streams to complete --
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # -- Complete alert stream --
        await alert_sink.complete()

        # -- Print summary --
        log.info("")
        log.info("%s--- Summary ---%s", CYAN, RESET)
        total_readings = 0
        total_anomalies = 0
        for result in results:
            if isinstance(result, BaseException):
                log.error("Stream failed: %s", result)
                continue
            sensor_name, count, value_sum, anomalies = result
            avg = value_sum / count if count > 0 else 0.0
            total_readings += count
            total_anomalies += anomalies
            log.info(
                "  %s%-30s%s  readings=%s%d%s  avg=%s%.2f%s  anomalies=%s%d%s",
                YELLOW,
                sensor_name,
                RESET,
                GREEN,
                count,
                RESET,
                CYAN,
                avg,
                RESET,
                RED if anomalies > 0 else DIM,
                anomalies,
                RESET,
            )
        log.info(
            "  %sTotal%s: %d readings, %d anomalies",
            CYAN,
            RESET,
            total_readings,
            total_anomalies,
        )

    log.info("Client shutdown")


def main() -> None:
    parser = argparse.ArgumentParser(description="Casty Stream Pipeline — Client")
    parser.add_argument(
        "--contact",
        required=True,
        help="Cluster contact point (host:port)",
    )
    parser.add_argument(
        "--producers",
        type=int,
        default=3,
        help="Expected number of sensor producers to discover (default: 3)",
    )
    args = parser.parse_args()

    host, port_str = args.contact.rsplit(":", maxsplit=1)
    contact_points = [(host, int(port_str))]

    asyncio.run(run_client(contact_points, expected_producers=args.producers))


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    main()
