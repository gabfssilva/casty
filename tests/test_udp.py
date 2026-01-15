"""UDP transport tests with client->server communication."""

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import Actor, ActorSystem, Context
from casty.io import udp


@dataclass
class GetReceivedData:
    """Request to get collected data."""

    pass


class DataCollectorActor(Actor[Any]):
    """Actor that collects received data into a list."""

    def __init__(self):
        self.received_data: list[tuple[bytes, tuple[str, int]]] = []

    async def receive(self, msg: Any, ctx: Context) -> None:
        # If it's a GetReceivedData request, return collected data
        if isinstance(msg, GetReceivedData):
            ctx.reply(self.received_data)
        # If it's a Received message from UDP, collect it
        elif hasattr(msg, "data") and hasattr(msg, "from_addr"):
            self.received_data.append((msg.data, msg.from_addr))
        # Otherwise just echo it
        else:
            ctx.reply(msg)


async def test_udp_server_bind(system: ActorSystem):
    """Test UDP server bind."""
    server = await system.spawn(udp.Server)

    # Test successful bind
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    assert isinstance(bound, udp.Bound)
    assert bound.host == "127.0.0.1"
    assert bound.port > 0


async def test_udp_client_connect(system: ActorSystem):
    """Test UDP client connect."""
    server = await system.spawn(udp.Server)

    # Bind server
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    server_port = bound.port

    # Create client and connect
    client = await system.spawn(udp.Client)
    connected = await client.ask(udp.Connect("127.0.0.1", server_port))
    assert isinstance(connected, udp.Connected)
    assert connected.remote_addr == ("127.0.0.1", server_port)


async def test_udp_client_send(system: ActorSystem):
    """Test UDP client send to connected peer."""
    server = await system.spawn(udp.Server)

    # Bind server
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    server_port = bound.port

    # Connect client
    client = await system.spawn(udp.Client)
    await client.ask(udp.Connect("127.0.0.1", server_port))

    # Send data (no error = success)
    await client.send(udp.Send(b"Hello UDP"))

    await asyncio.sleep(0.1)  # Let datagram arrive


async def test_udp_client_sendto(system: ActorSystem):
    """Test UDP client SendTo specific address."""
    server = await system.spawn(udp.Server)

    # Bind server
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    server_port = bound.port

    # Create client (don't connect)
    client = await system.spawn(udp.Client)

    # Disconnect in unconnected state should fail
    result = await client.ask(udp.Disconnect())
    assert isinstance(result, udp.CommandFailed)

    # SendTo should also fail in unconnected state
    result = await client.ask(udp.SendTo("127.0.0.1", server_port, b"test"))
    assert isinstance(result, udp.CommandFailed)


async def test_udp_client_to_server_communication(system: ActorSystem):
    """Test client->server communication with data validation."""
    # Create handler that collects data
    handler = await system.spawn(DataCollectorActor)

    # Create and bind server
    server = await system.spawn(udp.Server)
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    server_port = bound.port

    # Register handler on server to receive datagrams
    await server.ask(udp.RegisterHandler(handler))

    # Create client and connect to server
    client = await system.spawn(udp.Client)
    await client.ask(udp.Connect("127.0.0.1", server_port))

    # Send test data from client
    test_data = b"Hello from UDP client"
    await client.send(udp.Send(test_data))

    # Wait for datagram to arrive
    await asyncio.sleep(0.2)

    # Check handler received the data
    received_data = await handler.ask(GetReceivedData())
    assert len(received_data) == 1
    assert received_data[0][0] == test_data


async def test_udp_client_to_server_multiple_messages(system: ActorSystem):
    """Test client->server with multiple messages."""
    # Create handler
    handler = await system.spawn(DataCollectorActor)

    # Create and bind server
    server = await system.spawn(udp.Server)
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    server_port = bound.port

    # Register handler
    await server.ask(udp.RegisterHandler(handler))

    # Create client
    client = await system.spawn(udp.Client)
    await client.ask(udp.Connect("127.0.0.1", server_port))

    # Send multiple messages
    messages = [
        b"Message 1",
        b"Message 2",
        b"Message 3",
    ]

    for msg in messages:
        await client.send(udp.Send(msg))

    # Wait for all datagrams to arrive
    await asyncio.sleep(0.3)

    # Verify all received
    received_data = await handler.ask(GetReceivedData())
    assert len(received_data) == len(messages)
    for i, msg in enumerate(messages):
        assert received_data[i][0] == msg


async def test_udp_server_receives_from_multiple_clients(system: ActorSystem):
    """Test server receiving from multiple clients."""
    # Create handler
    handler = await system.spawn(DataCollectorActor)

    # Create and bind server
    server = await system.spawn(udp.Server)
    bound = await server.ask(udp.Bind("127.0.0.1", 0))
    server_port = bound.port

    # Register handler
    await server.ask(udp.RegisterHandler(handler))

    # Create 3 clients
    clients = []
    for i in range(3):
        client = await system.spawn(udp.Client)
        await client.ask(udp.Connect("127.0.0.1", server_port))
        clients.append(client)

    # Each client sends a message
    for i, client in enumerate(clients):
        msg = f"Message from client {i}".encode()
        await client.send(udp.Send(msg))

    # Wait for datagrams
    await asyncio.sleep(0.3)

    # Verify all received
    received_data = await handler.ask(GetReceivedData())
    assert len(received_data) == 3
    for i in range(3):
        expected = f"Message from client {i}".encode()
        assert expected in [data[0] for data in received_data]
