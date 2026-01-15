"""
Chaos engineering tools for network simulation.

This module provides actor-based tools for simulating adverse network
conditions in development and testing environments.

Usage:
    from casty.chaos import ChaosProxy, ChaosConfig, StartProxy

    proxy = await system.spawn(ChaosProxy)
    await proxy.send(StartProxy(
        listen_port=9001,
        target_host="127.0.0.1",
        target_port=8001,
        chaos=ChaosConfig.slow_network()
    ))

Presets:
    ChaosConfig.slow_network()      # 100ms latency, 20ms jitter
    ChaosConfig.lossy_network()     # 5% packet loss
    ChaosConfig.terrible_network()  # High latency, loss, corruption
    ChaosConfig.satellite()         # ~600ms RTT
    ChaosConfig.intercontinental()  # ~150ms RTT
    ChaosConfig.mobile_3g()         # 3G conditions
"""

from .config import ChaosConfig
from .messages import (
    StartProxy,
    StopProxy,
    UpdateChaos,
    ProxyStarted,
    ProxyStopped,
    ProxyError,
    ConnectionProxied,
    ConnectionClosed,
)
from .proxy import ChaosProxy, ProxyConnection

__all__ = [
    # Config
    "ChaosConfig",
    # Commands
    "StartProxy",
    "StopProxy",
    "UpdateChaos",
    # Events
    "ProxyStarted",
    "ProxyStopped",
    "ProxyError",
    "ConnectionProxied",
    "ConnectionClosed",
    # Actors
    "ChaosProxy",
    "ProxyConnection",
]
