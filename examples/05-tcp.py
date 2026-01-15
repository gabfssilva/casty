"""
TCP Echo Server and Client Example - Using become for state machines

Demonstrates:
- become() for explicit state transitions
- State: Idle → Listening/Connecting → Connected
"""

import asyncio

from casty import Actor, LocalRef, ActorSystem, Context
from casty.io import tcp


class EchoServer(Actor):
    """Echo server as explicit state machine."""

    server: LocalRef

    async def on_start(self):
        self.server = await self._ctx.spawn(tcp.Server)
        await self.server.send(tcp.Bind("127.0.0.1", 9000))

    # Estado inicial: aguardando Bind confirmar
    async def receive(self, msg, ctx: Context):
        match msg:
            case tcp.Bound(host, port):
                print(f"[server] listening on {host}:{port}")
                ctx.become(self.listening)

            case tcp.CommandFailed(cmd, cause):
                print(f"[server] bind failed: {cmd} - {cause}")

    # Estado: listening - aguardando conexões
    async def listening(self, msg, ctx: Context):
        match msg:
            case tcp.Accepted(conn, remote):
                print(f"[server] new connection from {remote}")
                # Continua em listening, mas agora também processa dados
                ctx.become(self.connected)

            case tcp.CommandFailed(cmd, cause):
                print(f"[server] command failed: {cmd} - {cause}")

    # Estado: connected - echoing data
    async def connected(self, msg, ctx: Context):
        match msg:
            case tcp.Accepted(conn, remote):
                # Ainda pode aceitar novas conexões
                print(f"[server] another connection from {remote}")

            case tcp.Received(data):
                print(f"[server] received {len(data)} bytes, echoing back")
                await ctx.sender.send(tcp.Write(data))

            case tcp.PeerClosed():
                print("[server] client disconnected")
                ctx.unbecome()  # Volta para listening

            case tcp.Closed():
                print("[server] connection closed")
                ctx.unbecome()

            case tcp.CommandFailed(cmd, cause):
                print(f"[server] command failed: {cmd} - {cause}")


class PingClient(Actor):
    """Ping client as explicit state machine."""

    client: LocalRef
    conn: LocalRef

    async def on_start(self):
        self.client = await self._ctx.spawn(tcp.Client)
        await self.client.send(tcp.Connect("127.0.0.1", 9000))

    # Estado inicial: aguardando conexão
    async def receive(self, msg, ctx: Context):
        match msg:
            case tcp.Connected(conn, remote, local):
                print(f"[client] connected to {remote} from {local}")
                self.conn = conn
                await conn.send(tcp.Write(b"hello casty!\n", ack="msg1"))
                ctx.become(self.awaiting_ack)

            case tcp.CommandFailed(cmd, cause):
                print(f"[client] connect failed: {cmd} - {cause}")

    # Estado: esperando confirmação de escrita
    async def awaiting_ack(self, msg, ctx: Context):
        match msg:
            case tcp.WriteAck(token):
                print(f"[client] write acknowledged: {token}")
                ctx.become(self.awaiting_echo)

            case tcp.CommandFailed(cmd, cause):
                print(f"[client] write failed: {cmd} - {cause}")

    # Estado: esperando resposta do echo
    async def awaiting_echo(self, msg, ctx: Context):
        match msg:
            case tcp.Received(data):
                print(f"[client] received echo: {data}")
                await self.conn.send(tcp.Close())
                ctx.become(self.closing)

    # Estado: fechando conexão
    async def closing(self, msg, ctx: Context):
        match msg:
            case tcp.Closed():
                print("[client] connection closed, done!")
                # Poderia ctx.unbecome() ou parar o ator aqui


async def main():
    async with ActorSystem() as system:
        await system.spawn(EchoServer)
        await asyncio.sleep(0.1)

        await system.spawn(PingClient)
        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
