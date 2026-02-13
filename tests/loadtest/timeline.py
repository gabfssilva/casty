from __future__ import annotations

import asyncio
import logging

import httpx

from .cluster import discover_node_ips, find_leader_ip, get_cluster_status, wait_for_recovery
from .config import LoadTestConfig
from .faults import crash_node, heal_partition, partition_node, recover_node
from .load import run_load
from .metrics import MetricsCollector
from .report import generate_report

log = logging.getLogger("loadtest.timeline")


def ips_from_urls(node_urls: list[str]) -> list[str]:
    return [u.split("//")[1].split(":")[0] for u in node_urls]


def exclude_ip(node_urls: list[str], ip: str) -> list[str]:
    return [u for u in node_urls if ips_from_urls([u])[0] != ip]


async def scenario_node_crash(
    cfg: LoadTestConfig,
    collector: MetricsCollector,
    client: httpx.AsyncClient,
    node_urls: list[str],
) -> None:
    log.info("--- Scenario 1: Node crash + recovery ---")
    node_ips = ips_from_urls(node_urls)
    status = await get_cluster_status(client, node_urls)
    leader = find_leader_ip(status)
    non_leader_ips = [ip for ip in node_ips if ip != leader]
    victim = non_leader_ips[0]

    log.info("Crashing non-leader node %s", victim)
    collector.record_fault("node_crash", victim)
    await crash_node(victim, cfg.ssh_key, cfg.ssh_user)

    log.info("Waiting 10s under degraded load...")
    await asyncio.sleep(10.0)

    log.info("Recovering node %s", victim)
    collector.record_fault("node_recover", victim)
    await recover_node(victim, cfg.ssh_key, cfg.ssh_user)

    healthy_urls = exclude_ip(node_urls, victim)
    recovery_time = await wait_for_recovery(
        client, healthy_urls, len(node_ips), timeout=60.0
    )
    log.info("Cluster recovered in %.1fs", recovery_time)
    await asyncio.sleep(10.0)


async def scenario_leader_kill(
    cfg: LoadTestConfig,
    collector: MetricsCollector,
    client: httpx.AsyncClient,
    node_urls: list[str],
) -> None:
    log.info("--- Scenario 2: Leader kill ---")
    node_ips = ips_from_urls(node_urls)
    status = await get_cluster_status(client, node_urls)
    leader = find_leader_ip(status)
    if not leader:
        log.warning("No leader found, skipping")
        return

    log.info("Killing leader node %s", leader)
    collector.record_fault("leader_kill", leader)
    await crash_node(leader, cfg.ssh_key, cfg.ssh_user)

    healthy_urls = exclude_ip(node_urls, leader)
    await asyncio.sleep(5.0)

    status = await get_cluster_status(client, healthy_urls)
    new_leader = find_leader_ip(status)
    log.info("New leader: %s", new_leader)

    log.info("Recovering old leader %s", leader)
    collector.record_fault("leader_recover", leader)
    await recover_node(leader, cfg.ssh_key, cfg.ssh_user)

    recovery_time = await wait_for_recovery(
        client, healthy_urls, len(node_ips), timeout=60.0
    )
    log.info("Cluster recovered in %.1fs", recovery_time)
    await asyncio.sleep(10.0)


async def scenario_network_partition(
    cfg: LoadTestConfig,
    collector: MetricsCollector,
    client: httpx.AsyncClient,
    node_urls: list[str],
) -> None:
    log.info("--- Scenario 3: Network partition ---")
    node_ips = ips_from_urls(node_urls)
    status = await get_cluster_status(client, node_urls)
    leader = find_leader_ip(status)
    non_leader_ips = [ip for ip in node_ips if ip != leader]
    victim = non_leader_ips[-1]

    log.info("Partitioning node %s (blocking port 25520)", victim)
    collector.record_fault("partition_start", victim)
    await partition_node(victim, cfg.ssh_key, cfg.ssh_user)

    log.info("Waiting 20s for phi accrual detection...")
    await asyncio.sleep(20.0)

    log.info("Healing partition for %s", victim)
    collector.record_fault("partition_heal", victim)
    await heal_partition(victim, cfg.ssh_key, cfg.ssh_user)

    healthy_urls = exclude_ip(node_urls, victim)
    recovery_time = await wait_for_recovery(
        client, healthy_urls, len(node_ips), timeout=60.0
    )
    log.info("Cluster reconverged in %.1fs", recovery_time)
    await asyncio.sleep(10.0)


async def scenario_rolling_restart(
    cfg: LoadTestConfig,
    collector: MetricsCollector,
    client: httpx.AsyncClient,
    node_urls: list[str],
) -> None:
    log.info("--- Scenario 4: Rolling restart ---")
    node_ips = ips_from_urls(node_urls)
    for ip in node_ips:
        log.info("Restarting node %s", ip)
        collector.record_fault("rolling_restart", ip)
        await crash_node(ip, cfg.ssh_key, cfg.ssh_user)
        await asyncio.sleep(5.0)
        await recover_node(ip, cfg.ssh_key, cfg.ssh_user)

        healthy_urls = exclude_ip(node_urls, ip)
        await wait_for_recovery(
            client, healthy_urls, len(node_ips), timeout=60.0
        )
        log.info("Node %s back and healthy", ip)

    log.info("Rolling restart complete")
    await asyncio.sleep(10.0)


async def run_timeline(cfg: LoadTestConfig) -> None:
    collector = MetricsCollector()
    stop_event = asyncio.Event()

    limits = httpx.Limits(max_connections=50, max_keepalive_connections=25)
    async with httpx.AsyncClient(limits=limits) as client:
        log.info("Discovering node IPs from cluster status...")
        node_ips = await discover_node_ips(client, cfg.target_url)
        log.info("Discovered %d nodes: %s", len(node_ips), ", ".join(node_ips))

        node_urls = [f"http://{ip}:8000" for ip in node_ips]
        load_urls = [cfg.target_url]

        collector.start_printer()

        load_task = asyncio.create_task(
            run_load(load_urls, collector, cfg.num_workers, stop_event)
        )

        log.info("=== Warmup (%ds) ===", cfg.warmup)
        await asyncio.sleep(cfg.warmup)

        if not cfg.no_faults and node_ips:
            await scenario_node_crash(cfg, collector, client, node_urls)
            await scenario_leader_kill(cfg, collector, client, node_urls)
            await scenario_network_partition(cfg, collector, client, node_urls)
            await scenario_rolling_restart(cfg, collector, client, node_urls)

        remaining = cfg.duration - collector.now()
        if remaining > 0:
            log.info("=== Cooldown (%.0fs) ===", remaining)
            await asyncio.sleep(remaining)

        stop_event.set()
        await load_task
        collector.stop_printer()

    await generate_report(collector)
