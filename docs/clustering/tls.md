# TLS

All inter-node communication in a Casty cluster travels over TCP. By default,
traffic is unencrypted. Enabling TLS encrypts gossip, heartbeats, shard routing,
and event replication — every TCP connection the cluster opens.

## Enabling TLS

Pass a `Config` with paths to your certificate, CA, and (optionally) private key:

```python
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

### ClusterClient

`ClusterClient` accepts the same `Config` object. Only the `client_context` is
used (the client opens outbound connections only):

```python
tls = Config.from_paths(certfile="certs/client/node.crt",
                        keyfile="certs/client/node.key",
                        cafile="certs/ca.crt")

async with ClusterClient(
    contact_points=[("10.0.0.1", 25520)],
    system_name="my-cluster",
    tls=tls,
) as client:
    ...
```

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

Casty ships a built-in CLI for generating production-grade mTLS certificates.
Install the `cert` extra to get it:

```bash
pip install casty[cert]
```

### 1. Create a Certificate Authority

```bash
casty cert create-ca --out certs/
```

This creates `certs/ca.crt` and `certs/ca.key` using ECDSA P-256 (valid for
365 days by default). Keep `ca.key` safe — it signs all node and client
certificates.

### 2. Create node certificates

Generate one certificate per cluster node. Pass every IP and hostname other
nodes use to reach it — they become Subject Alternative Names (SANs):

```bash
casty cert create-node 10.0.0.1 node-1.internal \
    --ca-dir certs/ --out certs/node-1/

casty cert create-node 10.0.0.2 node-2.internal \
    --ca-dir certs/ --out certs/node-2/
```

Each command creates `node.crt` and `node.key` in the output directory
(valid for 90 days by default).

### 3. Create client certificates (optional)

For external clients connecting via `ClusterClient`, generate a client-only
certificate (no `SERVER_AUTH` extended key usage):

```bash
casty cert create-client my-app --ca-dir certs/ --out certs/client/
```

### Inspecting certificates

```bash
casty cert info certs/node-1/node.crt    # show details for one cert
casty cert list certs/                   # summary of all certs in a directory
```

### Using the generated certificates

Point `Config.from_paths` at the generated files:

```python
tls = Config.from_paths(
    certfile="certs/node-1/node.crt",
    keyfile="certs/node-1/node.key",
    cafile="certs/ca.crt",
)
```

### CLI reference

| Command | Purpose | Default validity |
|---------|---------|-----------------|
| `casty cert create-ca` | Create a self-signed CA | 365 days |
| `casty cert create-node` | Create a node cert with SANs | 90 days |
| `casty cert create-client` | Create a client-only cert | 90 days |
| `casty cert info <file>` | Display certificate details | — |
| `casty cert list <dir>` | List all certificates in a directory | — |

All commands accept `--validity <days>` and `--force` (overwrite existing files).
Run `casty cert <command> --help` for full options.

??? note "Alternative: generating certificates with OpenSSL"

    You can also generate certificates with OpenSSL directly:

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

    The SAN must match the address other nodes use to connect. For multiple
    nodes, repeat the second step with each node's IP or hostname.

For **tests**, use [trustme](https://trustme.readthedocs.io/) to generate
throwaway CAs and certificates in memory — no OpenSSL binary, no temp files:

```python
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

**Next:** [Casty as a Cluster Backend](cluster-backend.md)
