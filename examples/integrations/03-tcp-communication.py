"""TCP Echo Server and Client using casty.io.

Demonstrates:
- TCP server binding with Bind message
- TCP client connection with Connect message
- Akka-style Register pattern for connection handling
- Bidirectional communication via Write/Received
- Length-prefixed framing for message boundaries

Run with:
    uv run python examples/io/03-tcp-communication.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox
from casty.io import (
    tcp,
    Bind, Connect, Bound, Connected, Received,
    Register, Write,
    InboundEvent, LengthPrefixedFramer,
)

PORT = 9876


@dataclass
class SendMessage:
    text: str


@actor
async def echo_server(*, mailbox: Mailbox[InboundEvent]):
    async for msg, ctx in mailbox:
        match msg:
            case Bound(addr):
                print(f"[server] listening on {addr[0]}:{addr[1]}")

            case Connected(connection, remote, _):
                print(f"[server] client connected from {remote[0]}:{remote[1]}")
                await connection.send(Register(ctx._self_ref))

            case Received(data):
                text = data.decode()
                print(f"[server] received: {text!r}")
                await ctx.sender.send(Write(f"echo: {text}".encode()))


@actor
async def echo_client(*, mailbox: Mailbox[InboundEvent | SendMessage]):
    connection = None

    async for msg, ctx in mailbox:
        match msg:
            case Connected(conn, _, local):
                print(f"[client] connected from {local[0]}:{local[1]}")
                connection = conn
                await connection.send(Register(ctx._self_ref))

            case Received(data):
                print(f"[client] received: {data.decode()!r}")

            case SendMessage(text):
                if connection:
                    print(f"[client] sending: {text!r}")
                    await connection.send(Write(text.encode()))


async def main():
    async with ActorSystem() as system:
        tcp_mgr = await system.actor(tcp(), name="tcp")

        server = await system.actor(echo_server(), name="server")

        await tcp_mgr.send(Bind(handler=server, port=PORT, framing=LengthPrefixedFramer()))
        await asyncio.sleep(0.05)

        client = await system.actor(echo_client(), name="client")

        await tcp_mgr.send(Connect(
            handler=client,
            host="127.0.0.1",
            port=PORT,
            framing=LengthPrefixedFramer(),
        ))

        await asyncio.sleep(0.05)

        for text in ["hello", "world", "casty"]:
            await client.send(SendMessage(text))
            await asyncio.sleep(0.1)

        print("[main] done")


if __name__ == "__main__":
    asyncio.run(main())
