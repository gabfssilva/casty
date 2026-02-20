"""Shared messages for the stream pipeline example.

Both node.py and client.py import from here so that pickle
serialization sees identical fully-qualified class names on
both sides of the wire.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import ServiceKey, StreamProducerMsg

if TYPE_CHECKING:
    from casty import ActorRef


@dataclass(frozen=True)
class SensorReading:
    sensor: str
    value: float
    timestamp: float
    node: str


@dataclass(frozen=True)
class Alert:
    sensor: str
    value: float
    threshold: float
    timestamp: float


@dataclass(frozen=True)
class ConsumeAlerts:
    producer: ActorRef[StreamProducerMsg[Alert]]


SENSOR_KEY: ServiceKey[StreamProducerMsg[SensorReading]] = ServiceKey("sensor")
ALERT_MONITOR_KEY: ServiceKey[ConsumeAlerts] = ServiceKey("alert-monitor")

SENSOR_RANGES: dict[str, tuple[float, float]] = {
    "temp": (18.0, 35.0),
    "humidity": (30.0, 80.0),
    "pressure": (990.0, 1030.0),
}

SENSOR_UNITS: dict[str, str] = {
    "temp": "C",
    "humidity": "%",
    "pressure": "hPa",
}

THRESHOLDS: dict[str, float] = {
    "temp": 30.0,
    "humidity": 70.0,
    "pressure": 1020.0,
}
