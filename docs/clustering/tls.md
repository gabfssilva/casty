# TLS

All inter-node communication in a Casty cluster travels over TCP. By default,
traffic is unencrypted. Enabling TLS encrypts gossip, heartbeats, shard routing,
and event replication — every TCP connection the cluster opens.

## Enabling TLS

Pass a `Config` with paths to your certificate, CA, and (optionally) private key:

```python
from casty.tls import Config
from casty.sharding import ClusteredActorSystem

async with ClusteredActorSystem(
    name="my-cluster",
    host="10.0.0.1",
    port=25520,
    node_id="node-1",
    seed_nodes=[("10.0.0.2", 25520)],
    tls=Config.from_paths(certfile="certs/node.pem", cafile="certs/ca.pem"),
) as system:
    ...  # all inter-node traffic is TLS-encrypted
```

Every node needs a certificate signed by the same CA. The certificate's Subject
Alternative Name (SAN) must include the IP or hostname other nodes use to reach it.

If the private key is in a separate file, pass `keyfile`:

```python
tls=Config.from_paths(
    certfile="certs/node.crt",
    keyfile="certs/node.key",
    cafile="certs/ca.pem",
)
```

When `tls` is omitted, the cluster runs in plaintext.

## Mutual TLS

TLS in Casty is always mutual. Every connection authenticates both sides: the
client verifies the server's certificate, and the server verifies the client's
certificate. A node that presents an invalid or missing certificate is rejected
at the TCP level — it never reaches the actor system.

Since every Casty node is both a server and a client, the same certificate and
CA are used for both roles. No additional configuration is needed per direction.

This is secure by default. Unlike web servers that accept anonymous clients,
a cluster should only allow authorized nodes. One-way TLS would protect against
eavesdropping but not against an impostor joining the cluster.

## Custom SSLContext

`Config.from_paths` covers the common case. For advanced scenarios — custom cipher
suites, OCSP stapling, hardware-backed keys — build `ssl.SSLContext` objects and
pass them directly:

```python
import ssl
from casty.tls import Config

server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
server_ctx.load_cert_chain("certs/node.pem", "certs/node.key")
server_ctx.verify_mode = ssl.CERT_REQUIRED
server_ctx.load_verify_locations("certs/ca.pem")

client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
client_ctx.load_cert_chain("certs/node.pem", "certs/node.key")
client_ctx.load_verify_locations("certs/ca.pem")

tls = Config(server_context=server_ctx, client_context=client_ctx)

async with ClusteredActorSystem(..., tls=tls) as system:
    ...
```

When using `Config` directly, you are responsible for enabling mutual TLS
(`verify_mode = ssl.CERT_REQUIRED` on the server context). `from_paths` does
this automatically.

## Generating Certificates

For development and testing, generate a CA and node certificate with OpenSSL:

```bash
# Create a CA
openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.pem \
    -days 365 -nodes -subj "/CN=CastyCA"

# Create a node certificate signed by the CA
openssl req -newkey rsa:2048 -keyout node.key -out node.csr \
    -nodes -subj "/CN=10.0.0.1"

openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out node.pem -days 365 \
    -extfile <(echo "subjectAltName=IP:10.0.0.1")
```

The SAN must match the address other nodes use to connect. For multiple nodes,
repeat the second step with each node's IP or hostname.

For production, use your organization's PKI or a tool like
[step-ca](https://smallstep.com/docs/step-ca/) to automate certificate issuance
and rotation.

For **tests**, use [trustme](https://trustme.readthedocs.io/) to generate
throwaway CAs and certificates in memory — no OpenSSL binary, no temp files:

```python
import ssl
import trustme
from casty.tls import Config

ca = trustme.CA()
server_cert = ca.issue_cert("127.0.0.1")

server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
server_cert.configure_cert(server_ctx)
server_ctx.verify_mode = ssl.CERT_REQUIRED
ca.configure_trust(server_ctx)

client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
client_cert = ca.issue_cert("127.0.0.1")
client_cert.configure_cert(client_ctx)
ca.configure_trust(client_ctx)

tls = Config(server_context=server_ctx, client_context=client_ctx)
```

---

**Next:** [Configuration](../configuration/index.md)
