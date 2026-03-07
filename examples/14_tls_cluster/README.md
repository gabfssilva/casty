# TLS Cluster — Sharded Counter

5-node cluster with mutual TLS over Docker Compose. Each node gets its own
certificate signed by a shared CA. The seed node increments a sharded counter
50 times, then all nodes read and verify `RESULT=50`.

## Quick start

```bash
cd examples/14_tls_cluster
docker compose up --build
```

Certificates are generated during the Docker build via `casty cert` — no manual steps.

## What happens

1. Docker build installs `casty[cert]` and runs `gen-certs.sh`, creating a CA and 5 node certificates
2. Docker Compose starts 5 nodes, each referencing its own `node.crt`/`node.key`
3. Nodes form a cluster using mutual TLS — all TCP connections are encrypted and verified
4. The seed node (`node-1`) increments a counter 50 times
5. All nodes read the counter and print `RESULT=50`
6. Clean shutdown via barrier synchronization

## Certificate structure

```
certs/
├── ca.crt              # CA certificate (shared by all nodes)
├── ca.key              # CA private key (only needed for signing)
├── node-1/
│   ├── node.crt        # node-1 certificate (SAN: DNS:node-1)
│   └── node.key        # node-1 private key
├── node-2/
│   ├── node.crt
│   └── node.key
├── ...
```
