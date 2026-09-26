# Docker cluster

Runs a cluster in containers with three seed nodes and thirty more, plus a client that increments the counters of five hundred pages. Every round, the client prints how many pages answered, how they are spread and the sum of the counters; the number of nodes can be changed while it runs to watch the actors move.

Requires a running Docker and Docker Compose. The image compiles the casty wheel in a build stage and installs only that wheel, so Rust is not needed on the machine. From the root of the repository, start the cluster and run the client:

```sh
cd examples/09-docker-cluster
docker compose up -d --build
docker compose run --rm client
```

The client runs five rounds by default. To watch the redistribution, run more rounds:

```sh
docker compose run --rm -e ROUNDS=30 client
```

While the client runs, in another terminal in the same directory:

```sh
docker compose up -d --scale node=45
```

When done, stop the cluster from the directory of the example:

```sh
docker compose down
```
