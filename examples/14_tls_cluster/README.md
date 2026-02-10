# TLS Cluster — Sharded Counter

5-node cluster with mutual TLS over Docker Compose. Each node gets its own
certificate signed by a shared CA. The seed node increments a sharded counter
50 times, then all nodes read and verify `RESULT=50`.

## Quick start

```bash
cd examples/14_tls_cluster
docker compose up --build
```

Certificates are generated during the Docker build — no manual steps.

## What happens

1. Docker build runs `gen-certs.sh`, creating a CA and 5 node certificates
2. Docker Compose starts 5 nodes, each referencing its own `node-{N}.pem`
3. Nodes form a cluster using mutual TLS — all TCP connections are encrypted and verified
4. The seed node (`node-1`) increments a counter 50 times
5. All nodes read the counter and print `RESULT=50`
6. Clean shutdown via barrier synchronization

## Certificate structure

Each node has a combined cert+key PEM file so `Config.from_paths` only needs two arguments:

```
certs/
├── ca.pem          # CA certificate (shared by all nodes)
├── node-1.pem      # node-1 cert + key (SAN: DNS:node-1)
├── node-2.pem      # node-2 cert + key (SAN: DNS:node-2)
├── node-3.pem      # ...
├── node-4.pem
└── node-5.pem
```
