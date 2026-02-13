from __future__ import annotations

import asyncssh


async def run_ssh(ip: str, command: str, ssh_key: str, ssh_user: str) -> str:
    async with asyncssh.connect(
        ip,
        username=ssh_user,
        client_keys=[ssh_key],
        known_hosts=None,
    ) as conn:
        result = await conn.run(command, check=True)
        return result.stdout or ""


async def disable_restart_policy(ip: str, ssh_key: str, ssh_user: str) -> None:
    await run_ssh(ip, "sudo docker update --restart=no casty-node", ssh_key, ssh_user)


async def enable_restart_policy(ip: str, ssh_key: str, ssh_user: str) -> None:
    await run_ssh(
        ip, "sudo docker update --restart=always casty-node", ssh_key, ssh_user
    )


async def crash_node(ip: str, ssh_key: str, ssh_user: str) -> None:
    await disable_restart_policy(ip, ssh_key, ssh_user)
    await run_ssh(ip, "sudo docker kill casty-node", ssh_key, ssh_user)


async def recover_node(ip: str, ssh_key: str, ssh_user: str) -> None:
    await run_ssh(ip, "sudo docker start casty-node", ssh_key, ssh_user)
    await enable_restart_policy(ip, ssh_key, ssh_user)


async def partition_node(ip: str, ssh_key: str, ssh_user: str) -> None:
    commands = (
        "sudo iptables -A INPUT -p tcp --dport 25520 -j DROP && "
        "sudo iptables -A OUTPUT -p tcp --dport 25520 -j DROP"
    )
    await run_ssh(ip, commands, ssh_key, ssh_user)


async def heal_partition(ip: str, ssh_key: str, ssh_user: str) -> None:
    commands = (
        "sudo iptables -D INPUT -p tcp --dport 25520 -j DROP && "
        "sudo iptables -D OUTPUT -p tcp --dport 25520 -j DROP"
    )
    await run_ssh(ip, commands, ssh_key, ssh_user)
