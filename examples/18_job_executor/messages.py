from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, ServiceKey

JOB_WORKER_KEY: ServiceKey[Any] = ServiceKey("job-worker")


@dataclass(frozen=True)
class InstallDeps:
    packages: tuple[str, ...]
    reply_to: ActorRef[DepsInstalled | DepsFailed]


@dataclass(frozen=True)
class DepsInstalled:
    node: str


@dataclass(frozen=True)
class DepsFailed:
    node: str
    error: str


@dataclass(frozen=True)
class RunJob:
    fn: Callable[[], Any]
    reply_to: ActorRef[JobResult]


@dataclass(frozen=True)
class JobResult:
    node: str
    value: Any | None = None
    error: str | None = None


@dataclass(frozen=True)
class Shutdown:
    pass
