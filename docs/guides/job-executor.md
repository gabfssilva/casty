# Job Executor

In this guide you'll build a **distributed job executor** that runs arbitrary Python functions across a 10-node Docker cluster. Along the way you'll learn how workers register via the receptionist, how `CloudPickleSerializer` sends lambdas over the wire, and how runtime `pip install` provisions dependencies on remote nodes.

The end result: 10 eigenvalue computations on 200x200 matrices, each on a different node, completing in the wall-clock time of a single job.

## Messages

The job protocol has two request-reply pairs — one for installing dependencies, one for running functions:

```python
--8<-- "examples/18_job_executor/messages.py:9:39"
```

Every response carries `node: str` so the client can print which worker handled each job.

## Worker Actor

The worker handles two message types. `InstallDeps` shells out to `pip install` via subprocess. `RunJob` executes the received function in a thread to avoid blocking the event loop:

```python
--8<-- "examples/18_job_executor/worker.py:42:67"
```

The function `fn` arrives as a cloudpickle-serialized closure — it can reference any libraries installed on the node, even if they weren't present when the cluster started.

## Node Entrypoint

Each node runs the same script. CLI args control port, seed node, and expected cluster size. The worker is wrapped with `Behaviors.discoverable` so the receptionist advertises it to the cluster:

```python
--8<-- "examples/18_job_executor/worker.py:90:110"
```

`wait_for(args.nodes)` blocks until all 10 nodes are `up` before the node is ready.

## Client

The client connects from outside the cluster via `ClusterClient`. It discovers workers through the receptionist, installs numpy on all of them, then fans out 10 jobs in parallel:

```python
--8<-- "examples/18_job_executor/client.py:53:66"
```

Runtime dependency installation — `client.ask` sends `InstallDeps` to each worker and waits for confirmation:

```python
--8<-- "examples/18_job_executor/client.py:68:86"
```

The job factory creates a closure that imports numpy *inside* the function. This is key — the import happens on the remote node after `pip install`, not on the client:

```python
--8<-- "examples/18_job_executor/client.py:88:103"
```

Fan-out: 10 jobs dispatched simultaneously via `asyncio.gather`, one per worker:

```python
--8<-- "examples/18_job_executor/client.py:105:130"
```

## Docker Setup

The `docker-compose.yml` defines 12 services: one seed node, nine workers, and one client. All worker nodes share the same image and entrypoint — only the hostname differs:

```yaml
--8<-- "examples/18_job_executor/docker-compose.yml:1:19"
```

The client depends on all workers and connects to the seed:

```yaml
--8<-- "examples/18_job_executor/docker-compose.yml:131:143"
```

## Running It

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty/examples/18_job_executor
docker compose up --build
```

Expected output:

```
Waiting for 10 workers...
  found 10 workers

Installing numpy on 10 workers...
  ✓ seed:25520
  ✓ worker-1:25520
  ✓ worker-2:25520
  ...

Submitting 10 jobs in parallel...
  job-0 → seed:25520       eigenvalues: [14.23, 14.18, 14.05]
  job-1 → worker-1:25520   eigenvalues: [14.21, 14.15, 14.09]
  ...

All 10 jobs completed in 0.32s
```

---

**What you learned:**

- **`Behaviors.discoverable`** auto-registers actors with the receptionist for cluster-wide service discovery.
- **`CloudPickleSerializer`** serializes lambdas and closures over the wire — standard pickle can't do this.
- **Runtime `pip install`** via subprocess lets workers provision dependencies on demand, without baking them into the Docker image.
- **`asyncio.gather` + `client.ask`** fans out work across nodes in parallel — total time approaches single-job time.
- **`ClusterClient`** connects from outside the cluster, discovers services, and sends messages without cluster membership.
