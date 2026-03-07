# TLS Cluster

In this guide you'll deploy a **5-node Casty cluster with mutual TLS** using Docker Compose. You'll generate certificates with the built-in `casty cert` CLI, wire them into each node, and verify that all inter-node traffic — gossip, heartbeats, shard routing — flows over encrypted, authenticated connections.

The end result: a sharded counter incremented 50 times on the seed node, read back as `RESULT=50` from all 5 nodes.

## Prerequisites

Install the `cert` extra for certificate generation:

```bash
pip install casty[cert]
```

## Generating Certificates

The `gen-certs.sh` script wraps three `casty cert` commands — one CA, five nodes:

```bash
--8<-- "examples/14_tls_cluster/gen-certs.sh"
```

Each node certificate includes `node-{N}`, `localhost`, and `127.0.0.1` as Subject Alternative Names (SANs). The SAN must match the hostname other nodes use to connect.

After running, the directory looks like:

```
certs/
├── ca.crt
├── ca.key
├── node-1/
│   ├── node.crt
│   └── node.key
├── node-2/
│   ├── node.crt
│   └── node.key
└── ...
```

## Messages and Entity

The counter protocol is two frozen dataclasses — `Increment` and `GetValue`:

```python
--8<-- "examples/14_tls_cluster/main.py:74:99"
```

State flows through behavior recursion: `active(value + amount)` returns a new behavior with the updated count.

## Wiring TLS into the Node

Each node receives `--certfile`, `--keyfile`, and `--cafile` from Docker Compose. `Config.from_paths` builds the mTLS contexts:

```python
--8<-- "examples/14_tls_cluster/main.py:210:212"
```

The `ClusteredActorSystem` receives the TLS config and encrypts all TCP connections — gossip, heartbeats, shard routing, and event replication:

```python
--8<-- "examples/14_tls_cluster/main.py:115:130"
```

## Docker Compose

The Dockerfile installs `casty[cert]` and generates certificates during build:

```dockerfile
--8<-- "examples/14_tls_cluster/Dockerfile"
```

Each node in `docker-compose.yml` gets its own cert/key pair, all signed by the same CA:

```yaml
--8<-- "examples/14_tls_cluster/docker-compose.yml:1:22"
```

The remaining 4 nodes follow the same pattern, adding `--seed node-1:25520` to join the cluster.

## Running

```bash
cd examples/14_tls_cluster
docker compose up --build
```

You'll see each node start with TLS, form the cluster, and converge on `RESULT=50`:

```
node-1  | 12:00:01  INFO    node-1  Starting (TLS enabled)...
node-2  | 12:00:01  INFO    node-2  Starting (TLS enabled)...
...
node-1  | 12:00:05  INFO    node-1  Cluster ready (5 nodes)
node-1  | 12:00:05  INFO    node-1  Incrementing counter 50 times...
...
node-1  | 12:00:07  INFO    node-1  RESULT=50
node-3  | 12:00:07  INFO    node-3  RESULT=50
```

A node presenting an invalid or missing certificate is rejected at the TCP level — it never reaches the actor system.

## What's Happening Under the Hood

1. **`casty cert create-ca`** generates an ECDSA P-256 CA certificate
2. **`casty cert create-node`** generates a node certificate signed by that CA, with SANs for each address
3. **`Config.from_paths`** loads the PEM files into `ssl.SSLContext` objects — one for server (inbound), one for client (outbound)
4. Every TCP connection the cluster opens performs a mutual TLS handshake: both sides present and verify certificates
5. A node whose certificate wasn't signed by the shared CA is rejected immediately

---

**See also:** [TLS reference](../clustering/tls.md) for `Config` API details, custom `SSLContext`, and CLI command reference.
