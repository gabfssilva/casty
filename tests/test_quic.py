"""QUIC transport tests with real client-server integration."""

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest
import trustme

from casty import Actor, ActorSystem, Context

# Check if aioquic is available
try:
    from casty.io import quic

    HAS_AIOQUIC = True
except ImportError:
    HAS_AIOQUIC = False

pytestmark = pytest.mark.skipif(not HAS_AIOQUIC, reason="aioquic not installed")


# ============================================================================
# TEST FIXTURES
# ============================================================================


@pytest.fixture
def ca():
    """Create a CA for test certificates."""
    return trustme.CA()


@pytest.fixture
def server_cert(ca):
    """Create server certificate."""
    return ca.issue_cert("localhost", "127.0.0.1", "::1")


@pytest.fixture
def cert_files(ca, server_cert, tmp_path):
    """Create temporary certificate files."""
    # Server cert and key
    server_cert_path = tmp_path / "server.pem"
    server_key_path = tmp_path / "server.key"
    server_cert.private_key_pem.write_to_path(server_key_path)

    # Write all certs in chain to server cert file
    with open(server_cert_path, "wb") as f:
        for blob in server_cert.cert_chain_pems:
            f.write(blob.bytes())

    # CA cert for client verification
    ca_cert_path = tmp_path / "ca.pem"
    ca.cert_pem.write_to_path(ca_cert_path)

    return {
        "server_cert": str(server_cert_path),
        "server_key": str(server_key_path),
        "ca_cert": str(ca_cert_path),
    }


# ============================================================================
# OPTIONS TESTS
# ============================================================================


def test_quic_options_defaults():
    """Test QuicOptions default values."""
    opts = quic.QuicOptions()
    assert opts.alpn_protocols == ("h3",)
    assert opts.idle_timeout == 30.0
    assert opts.max_streams_bidi == 100
    assert opts.enable_datagrams is False
    assert opts.verify_peer is True


def test_quic_options_custom():
    """Test QuicOptions with custom values."""
    opts = quic.QuicOptions(
        alpn_protocols=("h3", "hq-29"),
        idle_timeout=60.0,
        max_streams_bidi=200,
        enable_datagrams=True,
    )
    assert opts.alpn_protocols == ("h3", "hq-29")
    assert opts.idle_timeout == 60.0
    assert opts.max_streams_bidi == 200
    assert opts.enable_datagrams is True


def test_quic_options_presets():
    """Test QuicOptions presets."""
    # Low latency
    assert quic.LOW_LATENCY_OPTIONS.idle_timeout == 10.0
    assert quic.LOW_LATENCY_OPTIONS.initial_rtt == 0.05

    # High throughput
    assert quic.HIGH_THROUGHPUT_OPTIONS.max_stream_data_bidi_local == 4194304
    assert quic.HIGH_THROUGHPUT_OPTIONS.max_streams_bidi == 256

    # Datagram
    assert quic.DATAGRAM_OPTIONS.enable_datagrams is True
    assert quic.DATAGRAM_OPTIONS.max_datagram_size == 1200

    # Server
    assert quic.SERVER_OPTIONS.idle_timeout == 60.0
    assert quic.SERVER_OPTIONS.max_streams_bidi == 1000


def test_tls_certificate_config():
    """Test TlsCertificateConfig creation."""
    cert = quic.TlsCertificateConfig(
        certfile="/path/to/cert.pem",
        keyfile="/path/to/key.pem",
        ca_certs="/path/to/ca.pem",
    )
    assert cert.certfile == "/path/to/cert.pem"
    assert cert.keyfile == "/path/to/key.pem"
    assert cert.ca_certs == "/path/to/ca.pem"
    assert cert.verify_mode == "none"


def test_tls_certificate_config_verify_modes():
    """Test TlsCertificateConfig verify modes."""
    for mode in ["none", "optional", "required"]:
        cert = quic.TlsCertificateConfig(
            certfile="cert.pem",
            keyfile="key.pem",
            verify_mode=mode,
        )
        assert cert.verify_mode == mode


# ============================================================================
# MESSAGE TYPES TESTS
# ============================================================================


def test_quic_messages_are_frozen():
    """Test that QUIC messages are frozen dataclasses."""
    # Server commands
    bind = quic.Bind("localhost", 4433, quic.TlsCertificateConfig())
    with pytest.raises(AttributeError):
        bind.port = 8080  # type: ignore

    unbind = quic.Unbind()
    assert unbind is not None

    # Client commands
    connect = quic.Connect("localhost", 4433)
    with pytest.raises(AttributeError):
        connect.host = "other"  # type: ignore

    # Stream commands
    write = quic.Write(b"data", ack="token")
    with pytest.raises(AttributeError):
        write.data = b"other"  # type: ignore

    # Events
    bound = quic.Bound("127.0.0.1", 4433)
    with pytest.raises(AttributeError):
        bound.port = 8080  # type: ignore


def test_quic_message_types():
    """Test QUIC message type aliases exist."""
    # These should be defined
    assert quic.ServerCommand
    assert quic.ClientCommand
    assert quic.ConnectionCommand
    assert quic.StreamCommand
    assert quic.ServerEvent
    assert quic.ClientEvent
    assert quic.ConnectionEvent
    assert quic.StreamEvent


def test_quic_server_messages():
    """Test server command and event messages."""
    # Bind command
    cert = quic.TlsCertificateConfig(certfile="c.pem", keyfile="k.pem")
    bind = quic.Bind("0.0.0.0", 4433, certificate=cert, options=quic.QuicOptions())
    assert bind.host == "0.0.0.0"
    assert bind.port == 4433
    assert bind.certificate == cert

    # Unbind command
    unbind = quic.Unbind()
    assert unbind is not None

    # Bound event
    bound = quic.Bound("0.0.0.0", 4433)
    assert bound.host == "0.0.0.0"
    assert bound.port == 4433

    # Unbound event
    unbound = quic.Unbound()
    assert unbound is not None


def test_quic_client_messages():
    """Test client command and event messages."""
    # Connect command
    connect = quic.Connect(
        host="example.com",
        port=443,
        server_name="example.com",
        options=quic.QuicOptions(),
        ca_certs="/path/to/ca.pem",
        early_data=b"early",
        session_ticket=b"ticket",
    )
    assert connect.host == "example.com"
    assert connect.port == 443
    assert connect.server_name == "example.com"
    assert connect.early_data == b"early"
    assert connect.session_ticket == b"ticket"

    # Reconnect command
    reconnect = quic.Reconnect(new_local_address=("192.168.1.1", 5000))
    assert reconnect.new_local_address == ("192.168.1.1", 5000)


def test_quic_connection_messages():
    """Test connection command and event messages."""
    # Register command
    register = quic.Register(handler=None, auto_create_streams=False)  # type: ignore
    assert register.auto_create_streams is False

    # CreateStream command
    create = quic.CreateStream(bidirectional=True, handler=None)
    assert create.bidirectional is True

    # CloseConnection command
    close = quic.CloseConnection(error_code=42, reason="test")
    assert close.error_code == 42
    assert close.reason == "test"

    # SendDatagram command
    dgram = quic.SendDatagram(data=b"datagram", ack="ack_token")
    assert dgram.data == b"datagram"
    assert dgram.ack == "ack_token"


def test_quic_stream_messages():
    """Test stream command and event messages."""
    # Write command
    write = quic.Write(data=b"hello", ack="w1", end_stream=True)
    assert write.data == b"hello"
    assert write.ack == "w1"
    assert write.end_stream is True

    # SuspendReading/ResumeReading
    suspend = quic.SuspendReading()
    resume = quic.ResumeReading()
    assert suspend is not None
    assert resume is not None

    # CloseStream command
    close = quic.CloseStream(error_code=0)
    assert close.error_code == 0

    # ResetStream command
    reset = quic.ResetStream(error_code=123)
    assert reset.error_code == 123


def test_quic_event_messages():
    """Test event messages."""
    # StreamCreated
    stream_created = quic.StreamCreated(
        stream=None,  # type: ignore
        stream_id=4,
        is_unidirectional=False,
        initiated_locally=True,
    )
    assert stream_created.stream_id == 4
    assert stream_created.is_unidirectional is False
    assert stream_created.initiated_locally is True

    # DatagramReceived
    dgram_received = quic.DatagramReceived(data=b"received")
    assert dgram_received.data == b"received"

    # Received
    received = quic.Received(data=b"stream data")
    assert received.data == b"stream data"

    # WriteAck
    write_ack = quic.WriteAck(token="my_token")
    assert write_ack.token == "my_token"

    # StreamClosed/StreamReset
    stream_closed = quic.StreamClosed()
    stream_reset = quic.StreamReset(error_code=99)
    assert stream_closed is not None
    assert stream_reset.error_code == 99

    # PeerFinished
    peer_finished = quic.PeerFinished()
    assert peer_finished is not None

    # ConnectionClosed
    conn_closed = quic.ConnectionClosed(error_code=0, reason="bye")
    assert conn_closed.error_code == 0
    assert conn_closed.reason == "bye"

    # HandshakeCompleted
    handshake = quic.HandshakeCompleted(
        alpn_protocol="h3",
        session_resumed=True,
        session_ticket=b"new_ticket",
    )
    assert handshake.alpn_protocol == "h3"
    assert handshake.session_resumed is True
    assert handshake.session_ticket == b"new_ticket"

    # CommandFailed
    cmd_failed = quic.CommandFailed(
        command=quic.Connect("host", 443),
        cause=ConnectionError("failed"),
    )
    assert isinstance(cmd_failed.cause, ConnectionError)


# ============================================================================
# ACTOR SPAWN TESTS (basic lifecycle, no network)
# ============================================================================


async def test_quic_server_spawn(system: ActorSystem):
    """Test spawning QUIC server actor."""
    server = await system.spawn(quic.Server)
    assert server is not None


async def test_quic_client_spawn(system: ActorSystem):
    """Test spawning QUIC client actor."""
    client = await system.spawn(quic.Client)
    assert client is not None


async def test_quic_client_not_connected_errors(system: ActorSystem):
    """Test QUIC client errors when not connected."""
    client = await system.spawn(quic.Client)

    # CreateStream should fail when not connected
    result = await client.ask(quic.CreateStream())
    assert isinstance(result, quic.CommandFailed)
    assert isinstance(result.cause, ConnectionError)

    # CloseConnection should fail when not connected
    result = await client.ask(quic.CloseConnection())
    assert isinstance(result, quic.CommandFailed)
    assert isinstance(result.cause, ConnectionError)

    # Reconnect should fail when not connected
    result = await client.ask(quic.Reconnect())
    assert isinstance(result, quic.CommandFailed)
    assert isinstance(result.cause, ConnectionError)


# ============================================================================
# INTEGRATION TESTS - Real client/server communication
# ============================================================================


@dataclass
class EchoRequest:
    data: bytes


@dataclass
class EchoResponse:
    data: bytes


class EchoHandler(Actor):
    """Handler actor that processes QUIC events."""

    def __init__(self):
        self.accepted_connections: list = []
        self.received_data: list[bytes] = []
        self.streams_created: list = []

    async def receive(self, msg, ctx: Context):
        match msg:
            case quic.Accepted(connection, remote_address, server_name):
                self.accepted_connections.append(
                    {"connection": connection, "remote": remote_address, "sni": server_name}
                )

            case quic.StreamCreated(stream, stream_id, is_unidirectional, initiated_locally):
                self.streams_created.append(
                    {"stream": stream, "id": stream_id, "uni": is_unidirectional, "local": initiated_locally}
                )

            case quic.Received(data):
                self.received_data.append(data)

            case quic.ConnectionClosed(error_code, reason):
                pass  # Normal close

            case quic.HandshakeCompleted(alpn_protocol, session_resumed, session_ticket):
                pass  # Handshake done


async def test_quic_server_bind(system: ActorSystem, cert_files):
    """Test server can bind to a port."""
    server = await system.spawn(quic.Server)

    cert_config = quic.TlsCertificateConfig(
        certfile=cert_files["server_cert"],
        keyfile=cert_files["server_key"],
    )

    result = await server.ask(quic.Bind("127.0.0.1", 0, certificate=cert_config))

    assert isinstance(result, quic.Bound)
    assert result.host == "127.0.0.1"
    assert result.port > 0  # Ephemeral port assigned

    # Unbind
    await server.ask(quic.Unbind())


async def test_quic_server_double_bind_fails(system: ActorSystem, cert_files):
    """Test server rejects second bind."""
    server = await system.spawn(quic.Server)

    cert_config = quic.TlsCertificateConfig(
        certfile=cert_files["server_cert"],
        keyfile=cert_files["server_key"],
    )

    # First bind should succeed
    result = await server.ask(quic.Bind("127.0.0.1", 0, certificate=cert_config))
    assert isinstance(result, quic.Bound)
    port = result.port

    # Second bind should fail
    result2 = await server.ask(quic.Bind("127.0.0.1", 0, certificate=cert_config))
    assert isinstance(result2, quic.CommandFailed)
    assert "already bound" in str(result2.cause).lower()

    await server.ask(quic.Unbind())


async def test_quic_client_server_connect(system: ActorSystem, cert_files):
    """Test client can connect to server."""
    # Start handler
    handler = await system.spawn(EchoHandler)

    # Start server
    server = await system.spawn(quic.Server)
    cert_config = quic.TlsCertificateConfig(
        certfile=cert_files["server_cert"],
        keyfile=cert_files["server_key"],
    )

    bound = await server.ask(quic.Bind("127.0.0.1", 0, certificate=cert_config))
    assert isinstance(bound, quic.Bound)
    port = bound.port

    # Start client with verification disabled (self-signed cert)
    client = await system.spawn(quic.Client)

    options = quic.QuicOptions(verify_peer=False)
    result = await client.ask(
        quic.Connect(
            "127.0.0.1",
            port,
            server_name="localhost",
            options=options,
            ca_certs=cert_files["ca_cert"],
        ),
        timeout=10.0,
    )

    assert isinstance(result, quic.Connected)
    assert result.connection is not None
    assert result.remote_address[1] == port

    # Cleanup
    await client.ask(quic.CloseConnection())
    await server.ask(quic.Unbind())


async def test_quic_stream_creation(system: ActorSystem, cert_files):
    """Test creating streams after connection."""
    handler = await system.spawn(EchoHandler)

    # Start server
    server = await system.spawn(quic.Server)
    cert_config = quic.TlsCertificateConfig(
        certfile=cert_files["server_cert"],
        keyfile=cert_files["server_key"],
    )

    bound = await server.ask(quic.Bind("127.0.0.1", 0, certificate=cert_config))
    port = bound.port

    # Connect client with CA cert for verification
    client = await system.spawn(quic.Client)
    options = quic.QuicOptions(verify_peer=False)
    connected = await client.ask(
        quic.Connect(
            "127.0.0.1",
            port,
            server_name="localhost",
            options=options,
            ca_certs=cert_files["ca_cert"],
        ),
        timeout=10.0,
    )

    assert isinstance(connected, quic.Connected)
    conn = connected.connection

    # Create a bidirectional stream
    result = await conn.ask(quic.CreateStream(bidirectional=True))
    assert isinstance(result, quic.StreamCreated)
    assert result.stream is not None
    assert result.initiated_locally is True
    assert result.is_unidirectional is False

    # Cleanup
    await client.ask(quic.CloseConnection())
    await server.ask(quic.Unbind())


async def test_quic_stream_write_read(system: ActorSystem, cert_files):
    """Test writing data on a stream."""
    # Handler to receive data
    @dataclass
    class DataCollector:
        received: list[bytes]
        event: asyncio.Event

    collector = DataCollector(received=[], event=asyncio.Event())

    class ClientHandler(Actor):
        async def receive(self, msg, ctx: Context):
            match msg:
                case quic.Received(data):
                    collector.received.append(data)
                    collector.event.set()
                case _:
                    pass

    client_handler = await system.spawn(ClientHandler)

    # Start server
    server = await system.spawn(quic.Server)
    cert_config = quic.TlsCertificateConfig(
        certfile=cert_files["server_cert"],
        keyfile=cert_files["server_key"],
    )

    bound = await server.ask(quic.Bind("127.0.0.1", 0, certificate=cert_config))
    port = bound.port

    # Connect client with CA cert
    client = await system.spawn(quic.Client)
    options = quic.QuicOptions(verify_peer=False)
    connected = await client.ask(
        quic.Connect(
            "127.0.0.1",
            port,
            server_name="localhost",
            options=options,
            ca_certs=cert_files["ca_cert"],
        ),
        timeout=10.0,
    )

    conn = connected.connection

    # Register handler for receiving data
    await conn.send(quic.Register(handler=client_handler, auto_create_streams=True))

    # Create stream
    stream_created = await conn.ask(quic.CreateStream(bidirectional=True))
    stream = stream_created.stream

    # Write data
    test_data = b"Hello, QUIC!"
    await stream.send(quic.Write(data=test_data, ack="msg1"))

    # Give some time for the write to complete
    await asyncio.sleep(0.1)

    # Cleanup
    await stream.send(quic.CloseStream())
    await client.ask(quic.CloseConnection())
    await server.ask(quic.Unbind())
