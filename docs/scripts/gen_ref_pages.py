"""Auto-generate API reference pages from casty.__all__.

Each public symbol is mapped to a reference page based on its source module.
When a new symbol is exported from __init__.py, it appears automatically in
the correct reference doc on the next mkdocs build.
"""

from __future__ import annotations

import types
from collections import defaultdict

import mkdocs_gen_files

import casty

MODULE_TO_PAGE: dict[str, tuple[str, str]] = {
    "casty.actor": ("reference/behaviors.md", "Behaviors"),
    "casty.ref": ("reference/ref.md", "ActorRef"),
    "casty.context": ("reference/context.md", "ActorContext"),
    "casty.system": ("reference/system.md", "ActorSystem"),
    "casty.supervision": ("reference/supervision.md", "Supervision"),
    "casty.mailbox": ("reference/mailbox.md", "Mailbox"),
    "casty.events": ("reference/events.md", "Events"),
    "casty.messages": ("reference/events.md", "Events"),
    "casty.scheduler": ("reference/scheduler.md", "Scheduler"),
    "casty.journal": ("reference/journal.md", "Event Sourcing"),
    "casty.cluster": ("reference/cluster.md", "Cluster"),
    "casty.cluster_state": ("reference/cluster-state.md", "Cluster State"),
    "casty.gossip_actor": ("reference/cluster.md", "Cluster"),
    "casty.failure_detector": ("reference/failure-detector.md", "Failure Detector"),
    "casty.sharding": ("reference/sharding.md", "Sharding"),
    "casty.replication": ("reference/sharding.md", "Sharding"),
    "casty.receptionist": ("reference/receptionist.md", "Receptionist"),
    "casty.distributed": ("reference/distributed.md", "Distributed"),
    "casty.address": ("reference/address.md", "Address"),
    "casty.transport": ("reference/transport.md", "Transport"),
    "casty.remote_transport": ("reference/transport.md", "Transport"),
    "casty.serialization": ("reference/serialization.md", "Serialization"),
    "casty.config": ("reference/config.md", "Configuration"),
    "casty.task_runner": ("reference/task-runner.md", "Task Runner"),
}

pages: dict[str, list[str]] = defaultdict(list)
titles: dict[str, str] = {}

for name in casty.__all__:
    obj = getattr(casty, name)

    if isinstance(obj, types.ModuleType):
        continue

    module = getattr(obj, "__module__", None)
    if module is None:
        continue

    page_info = None
    parts = module.split(".")
    while parts and page_info is None:
        page_info = MODULE_TO_PAGE.get(".".join(parts))
        parts.pop()
    if page_info is None:
        msg = f"casty.__all__ exports '{name}' from unmapped module '{module}'"
        raise ValueError(msg)

    page_path, title = page_info
    pages[page_path].append(name)
    titles[page_path] = title

for page_path, symbols in sorted(pages.items()):
    title = titles[page_path]
    with mkdocs_gen_files.open(page_path, "w") as f:
        f.write(f"# {title}\n")
        for sym in symbols:
            f.write(f"\n::: casty.{sym}\n")
