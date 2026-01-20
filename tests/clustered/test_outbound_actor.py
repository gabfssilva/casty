import pytest
import asyncio

from casty import ActorSystem


@pytest.mark.asyncio
async def test_outbound_creates_connection():
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Connect, Transmit

    received_data = []

    async def handle_client(reader, writer):
        try:
            length_bytes = await reader.readexactly(4)
            length = int.from_bytes(length_bytes, "big")
            data = await reader.readexactly(length)
            received_data.append(data)
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    async with ActorSystem() as system:
        router_ref = await system.actor(router_actor(), name="router")

        outbound_ref = await system.actor(
            outbound_actor(router_ref),
            name="outbound"
        )

        conn_ref = await outbound_ref.ask(Connect(
            node_id="node-b",
            address=f"127.0.0.1:{port}"
        ))

        assert conn_ref is not None

        await conn_ref.send(Transmit(data=b"test"))
        await asyncio.sleep(0.1)

        assert len(received_data) == 1
        assert received_data[0] == b"test"

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_outbound_reuses_existing_connection():
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Connect

    connection_count = [0]
    client_tasks = []

    async def handle_client(reader, writer):
        connection_count[0] += 1
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    try:
        async with ActorSystem() as system:
            router_ref = await system.actor(router_actor(), name="router")

            outbound_ref = await system.actor(
                outbound_actor(router_ref),
                name="outbound"
            )

            conn_ref1 = await outbound_ref.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port}"
            ))

            conn_ref2 = await outbound_ref.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port}"
            ))

            assert conn_ref1 == conn_ref2
            assert connection_count[0] == 1
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_outbound_get_connection():
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Connect, GetConnection

    async def handle_client(reader, writer):
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    try:
        async with ActorSystem() as system:
            router_ref = await system.actor(router_actor(), name="router")

            outbound_ref = await system.actor(
                outbound_actor(router_ref),
                name="outbound"
            )

            none_ref = await outbound_ref.ask(GetConnection(node_id="node-b"))
            assert none_ref is None

            created_ref = await outbound_ref.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port}"
            ))

            fetched_ref = await outbound_ref.ask(GetConnection(node_id="node-b"))
            assert fetched_ref == created_ref
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_outbound_multiple_nodes():
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.router import router_actor
    from casty.cluster.transport_messages import Connect, Transmit

    received_data_a = []
    received_data_b = []

    async def handle_client_a(reader, writer):
        try:
            length_bytes = await reader.readexactly(4)
            length = int.from_bytes(length_bytes, "big")
            data = await reader.readexactly(length)
            received_data_a.append(data)
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_client_b(reader, writer):
        try:
            length_bytes = await reader.readexactly(4)
            length = int.from_bytes(length_bytes, "big")
            data = await reader.readexactly(length)
            received_data_b.append(data)
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server_a = await asyncio.start_server(handle_client_a, "127.0.0.1", 0)
    server_b = await asyncio.start_server(handle_client_b, "127.0.0.1", 0)
    port_a = server_a.sockets[0].getsockname()[1]
    port_b = server_b.sockets[0].getsockname()[1]

    try:
        async with ActorSystem() as system:
            router_ref = await system.actor(router_actor(), name="router")

            outbound_ref = await system.actor(
                outbound_actor(router_ref),
                name="outbound"
            )

            conn_a = await outbound_ref.ask(Connect(
                node_id="node-a",
                address=f"127.0.0.1:{port_a}"
            ))

            conn_b = await outbound_ref.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port_b}"
            ))

            assert conn_a != conn_b

            await conn_a.send(Transmit(data=b"to-a"))
            await conn_b.send(Transmit(data=b"to-b"))
            await asyncio.sleep(0.1)

            assert received_data_a == [b"to-a"]
            assert received_data_b == [b"to-b"]
    finally:
        server_a.close()
        server_b.close()
        await server_a.wait_closed()
        await server_b.wait_closed()
