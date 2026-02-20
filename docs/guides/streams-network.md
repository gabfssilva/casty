# Reactive Streams over the Network

In this guide you'll build a **sensor pipeline** that streams data across a cluster and consumes it from an external client. Along the way you'll learn how `stream_producer` and `stream_consumer` work across nodes via service discovery, and how a `ClusterClient` can both consume and produce streams.

## The Problem

The [Reactive Streams](streams.md) guide showed how `stream_producer` and `stream_consumer` wire up a backpressured pipeline inside a single actor system. But in a distributed setting, the producer and consumer live on different machines. A sensor node pushes readings into a producer — how does a monitoring client on another machine iterate that stream?

The answer: the stream protocol is just actor messages. `StreamElement`, `StreamDemand`, `StreamCompleted` — they all flow through `ActorRef`, which is network-transparent. If two actors can exchange messages over TCP, they can run a stream between them. Nothing changes in the API.

## The Data

Sensor readings and anomaly alerts, shared between all nodes and the client:

```python
--8<-- "examples/17_stream_pipeline/messages.py:19:37"
```

## Producing on a Cluster Node

A cluster node spawns a `stream_producer`, registers it with a `ServiceKey` for discovery, and obtains a `SinkRef` — all in one place:

```python
--8<-- "examples/17_stream_pipeline/messages.py:40:40"

--8<-- "examples/17_stream_pipeline/node.py:230:236"
```

`Behaviors.discoverable` wraps the producer so the cluster receptionist knows about it. Any node or `ClusterClient` can now discover this producer via `lookup(SENSOR_KEY)`. The `SinkRef` wraps an in-process `asyncio.Queue` — it never leaves this node.

The node pushes readings into the sink:

```python
--8<-- "examples/17_stream_pipeline/node.py:169:177"
```

After all readings are pushed, `sink.complete()` signals the end of the stream.

## Consuming from Another Cluster Node

A second cluster node can discover the producer via `lookup` and spawn a local `stream_consumer` wired to the remote producer's ref. The code is identical to a single-node stream — `stream_consumer` sends `Subscribe` to the remote producer, `StreamElement` messages travel over TCP, and `StreamDemand` messages travel back. `SourceRef` wraps a local queue — iterating it feels the same as a single-node stream.

## Consuming from a ClusterClient

A `ClusterClient` connects from outside the cluster without joining it. It discovers producers via `lookup` and spawns a local `stream_consumer` for each one:

```python
--8<-- "examples/17_stream_pipeline/client.py:212:231"
```

`client.spawn()` creates a local actor inside the client's internal actor system. That actor has a full `ActorRef` addressable over TCP — the cluster-side producer can send `StreamElement` messages to it. The pattern is identical to consuming from within the cluster.

## Streaming Back: Client to Cluster

The client can also be the producer. It spawns a local `stream_producer`, obtains a `SinkRef`, and tells a cluster-side actor to subscribe. The alert monitor is discoverable under its own key:

```python
--8<-- "examples/17_stream_pipeline/messages.py:41:41"

--8<-- "examples/17_stream_pipeline/client.py:187:207"
```

The client discovers the alert monitor via `lookup(ALERT_MONITOR_KEY)` and sends `ConsumeAlerts(producer=alert_producer)`. The cluster-side node receives the client's producer ref and spawns a `stream_consumer` wired to it — the same `stream_consumer` + `SourceRef` + `async for` pattern as every other direction:

```python
--8<-- "examples/17_stream_pipeline/node.py:131:151"
```

## What Stays Local, What Crosses the Wire

`SinkRef` and `SourceRef` wrap `asyncio.Queue`s — they never cross the wire. Each one lives on the same node as its actor:

| Object | Where it lives | Serialized? |
|---|---|---|
| `SinkRef` | Same node as the producer | No — wraps a local `asyncio.Queue` |
| `SourceRef` | Same node as the consumer | No — wraps a local `asyncio.Queue` |
| `ActorRef` | Anywhere — addressable by URI | Yes — `casty://system@host:port/path` |
| `StreamElement`, `StreamDemand`, ... | Actor messages | Yes — via the configured serializer |

This is why you always obtain `SinkRef` and `SourceRef` locally via `GetSink` / `GetSource` on the node where the actor lives. The stream protocol messages handle the rest.

## Docker Compose

The `examples/17_stream_pipeline/` directory contains the full example — 3 cluster nodes producing sensor readings and 1 external client consuming all streams, detecting anomalies, and pushing alerts back:

```yaml
--8<-- "examples/17_stream_pipeline/docker-compose.yml"
```

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty/examples/17_stream_pipeline
docker compose up --build
```

---

**What you learned:**

- **Streams work across nodes transparently** — the same `Subscribe`/`StreamDemand`/`StreamElement` protocol flows over TCP with no extra configuration.
- **`SinkRef` and `SourceRef` stay local** — they wrap `asyncio.Queue`s and are never serialized. Only `ActorRef` messages cross the wire.
- **Service discovery** (`Behaviors.discoverable` + `lookup`) is how consumers find remote producers in the cluster.
- **`ClusterClient.spawn()`** creates locally-addressable actors that cluster nodes can send messages to — enabling bidirectional streaming from outside the cluster.
