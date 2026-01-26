# Casty Cluster Architecture

## Actor Hierarchy

```
ClusteredActorSystem
    |
    +-- cluster_actor (name: "cluster")
            |
            +-- remote_actor (name: "remote")
            |       |
            |       +-- tcp (name: "tcp")
            |       |       |
            |       |       +-- tcp_listener (name: "listener-{n}")
            |       |       +-- tcp_connection (name: "conn-{in/out}-{n}")
            |       |
            |       +-- _remote_listener (name: "listener-{port}")
            |       |       |
            |       |       +-- session_actor (name: "session-{peer}")
            |       |
            |       +-- _remote_connector (name: "connector-{host}-{port}")
            |               |
            |               +-- session_actor (name: "session")
            |
            +-- states_actor (name: "states")
            |
            +-- membership_actor (name: "membership")
            |
            +-- swim_actor (name: "swim")
            |
            +-- gossip_actor (name: "gossip")
            |
            +-- [user actors...]
```

## Startup Flow

```
                          ClusteredActorSystem.start()
                                    |
                                    v
+-----------------------------------------------------------------------------------+
|                            cluster_actor                                           |
+-----------------------------------------------------------------------------------+
        |                           |                           |
        v                           v                           v
  spawn remote_actor          spawn states_actor       spawn membership_actor
        |                                                       |
        v                                                       v
  remote_actor.ask(Listen)                             membership_actor.send(
        |                                               SetLocalAddress)
        v                                                       |
  spawn _remote_listener -----> tcp.send(Bind)                  |
        |                           |                           |
        |                           v                           |
        |                    start_server()                     |
        |                           |                           |
        v                           v                           v
  Listening(address)  <----  Bound(local_addr)          spawn swim_actor
        |                                                       |
        v                                                       |
  GetClusterAddress                                     spawn gossip_actor
        |                                                       |
        v                                                       v
  Expose(swim, gossip, membership, cluster, states)     _connect_to_seed()
```
~~
~~## Connection Flow (Outbound)

```
                    Actor A wants to send to remote Actor B
                                    |
                                    v
+-----------------------------------------------------------------------------------+
|                    initiate_send(address, actor_name, message)                     |
|                         (in swim_actor, gossip_actor, etc.)                        |
+-----------------------------------------------------------------------------------+
        |
        | address in pending_connects?
        |
   +----+----+
   |         |
  YES        NO
   |          |
   v          v
 append    pending_connects[address] = [message]
message           |
   |              v
   |    remote_ref.send(Connect(host, port), sender=self)
   |              |
   |              v
   |    +-----------------------------------------------------------------------------------+
   |    |                           remote_actor                                             |
   |    +-----------------------------------------------------------------------------------+
   |              |
   |              | address in sessions?
   |              |
   |         +----+----+
   |         |         |
   |        YES        NO
   |         |          |
   |         v          | address in pending_connections?
   |    Reply(          |
   | RemoteConnected)   +----+----+
   |                    |         |
   |                   YES        NO
   |                    |          |
   |                    v          v
   |              [continue]    pending_connections[address] = []
   |              NO REPLY!            |
   |                ^^^                v
   |                BUG         spawn _remote_connector
   |                                   |
   |                                   v
   |                    +-----------------------------------------------------------------------------------+
   |                    |                      _remote_connector                                             |
   |                    +-----------------------------------------------------------------------------------+
   |                                   |
   |                                   v
   |                        tcp.send(IoConnect(handler=self, host, port))
   |                                   |
   |                                   v
   |                    +-----------------------------------------------------------------------------------+
   |                    |                           tcp                                                      |
   |                    +-----------------------------------------------------------------------------------+
   |                                   |
   |                         asyncio.open_connection(host, port)
   |                                   |
   |                           +-------+-------+
   |                           |               |
   |                        SUCCESS          FAIL
   |                           |               |
   |                           v               v
   |                    IoConnected      IoConnectFailed
   |                           |               |
   v                           v               v
+-----------------------------------------------------------------------------------+
|                      _remote_connector (continued)                                 |
+-----------------------------------------------------------------------------------+
   |                           |               |
   |                           v               v
   |                    spawn session   remote_ref.send(_ConnectFailed)
   |                           |               |
   |                           v               v
   |            remote_ref.send(        reply_to.send(Reply(
   |             _SessionConnected)      ConnectFailed(peer)))
   |                           |               |
   |                           v               |
   |            reply_to.send(Reply(           |
   |             RemoteConnected))             |
   |                           |               |
   v                           v               v
+-----------------------------------------------------------------------------------+
|                         remote_actor (continued)                                   |
+-----------------------------------------------------------------------------------+
                               |               |
                               v               v
                        sessions[peer] =   pending_connections.pop(peer)
                         session
                               |
                               v
                        pending_connections
                         .pop(peer)
                               |
                               v
                        process queued
                         lookups
```

## SWIM Failure Detection Flow

```
+-----------------------------------------------------------------------------------+
|                              swim_actor                                            |
+-----------------------------------------------------------------------------------+
        |
        | SwimTick (every probe_interval)
        v
+-------+-------------------------------------------------------+
|  1. membership_ref.ask(GetAliveMembers)                       |
|  2. Pick random target from other members                     |
|  3. Schedule ProbeTimeout(target) after probe_timeout         |
|  4. pending_probes[target] = ProbeState(...)                  |
|  5. initiate_send(target_addr, "swim", Ping(sender, members)) |
+---------------------------------------------------------------+
        |
        | Connection established, Lookup succeeds
        v
+-------+-------------------------------------------------------+
|  Target's swim_actor receives Ping                            |
|  1. MergeMembership with received members                     |
|  2. Reply with Ack(sender, members)                           |
+---------------------------------------------------------------+
        |
        v
+-------+-------------------------------------------------------+
|  Original swim_actor receives Ack                             |
|  1. Pop from pending_probes                                   |
|  2. Cancel ProbeTimeout                                       |
|  3. Cancel PingReqTimeout (if any)                            |
|  4. MergeMembership                                           |
|  5. MarkAlive(sender)                                         |
+---------------------------------------------------------------+


                    === TIMEOUT PATH ===

+-------+-------------------------------------------------------+
|  ProbeTimeout fires (no Ack received)                         |
|  1. Get alive members (excluding target)                      |
|  2. Pick random probers (ping_req_fanout count)               |
|  3. Schedule PingReqTimeout(target)                           |
|  4. For each prober:                                          |
|     initiate_send(prober_addr, "swim",                        |
|       PingReq(sender, target, members))                       |
+---------------------------------------------------------------+
        |
        v
+-------+-------------------------------------------------------+
|  Prober's swim_actor receives PingReq                         |
|  1. MergeMembership                                           |
|  2. MarkAlive(sender)                                         |
|  3. initiate_send(target_addr, "swim", Ping(...))             |
|  4. Reply PingReqAck(success=True) <-- BUG: ALWAYS TRUE       |
|                                                               |
|     ^^^ PROBLEM: PingReqAck sent BEFORE knowing if            |
|         target actually responded!                            |
+---------------------------------------------------------------+
        |
        v
+-------+-------------------------------------------------------+
|  Original swim_actor receives PingReqAck                      |
|  If success=True:                                             |
|    1. Pop from pending_probes                                 |
|    2. Cancel PingReqTimeout                                   |
|    --> Target NEVER marked as DOWN!                           |
+---------------------------------------------------------------+


                === CORRECT BEHAVIOR (not implemented) ===

+-------+-------------------------------------------------------+
|  Prober's swim_actor receives PingReq                         |
|  1. Send Ping to target                                       |
|  2. Wait for Ack from target (with timeout)                   |
|  3. If Ack received: Reply PingReqAck(success=True)           |
|  4. If timeout: Reply PingReqAck(success=False)               |
+---------------------------------------------------------------+


                === FINAL TIMEOUT ===

+-------+-------------------------------------------------------+
|  PingReqTimeout fires                                         |
|  If target still in pending_probes:                           |
|    1. Pop from pending_probes                                 |
|    2. membership_ref.send(MarkDown(target))                   |
+---------------------------------------------------------------+
```

## Connection Failure Problem

When a node dies, the following happens:

```
Node-1 (alive) trying to probe Node-0 (dead)
============================================

Time T0: SwimTick fires
    |
    +-> initiate_send("node-0:port", "swim", Ping)
    |       |
    |       +-> pending_connects["node-0:port"] = [Ping]
    |       +-> remote_ref.send(Connect("node-0", port))
    |               |
    |               +-> remote_actor creates pending_connections["node-0:port"] = []
    |               +-> spawn _remote_connector
    |                       |
    |                       +-> tcp.send(IoConnect)
    |                               |
    |                               +-> asyncio.open_connection() [BLOCKS - TCP timeout]

Time T0 + probe_interval: SwimTick fires again
    |
    +-> initiate_send("node-0:port", "swim", Ping)
            |
            +-> pending_connects["node-0:port"].append(Ping)  [message queued]
            +-> remote_ref.send(Connect("node-0", port))
                    |
                    +-> "Connection already pending to peer" [NO REPLY]

Time T0 + probe_timeout: ProbeTimeout fires
    |
    +-> initiate_send to probers (PingReq)
    +-> Probers try to ping node-0 (same problem)
    +-> Probers send PingReqAck(success=True) IMMEDIATELY

Time T0 + probe_timeout + small_delay: PingReqAck received
    |
    +-> pending_probes.pop(target)
    +-> cancel PingReqTimeout
    +-> Node-0 NEVER MARKED AS DOWN!

Eventually: TCP connection fails
    |
    +-> IoConnectFailed
    +-> _remote_connector sends _ConnectFailed to remote_actor
    +-> _remote_connector sends Reply(ConnectFailed) to swim_actor
    +-> pending_connects["node-0:port"] cleared (NEW FIX)
    +-> pending_connections["node-0:port"] cleared (NEW FIX)
    |
    +-> But it's too late! PingReqAck already marked node as "probed successfully"
```

## Problems Identified

### Problem 1: PingReqAck Always Returns success=True

**Location:** `cluster.py`, `swim_actor`, `case PingReq(...)` handler

**Current behavior:**
```python
ack = PingReqAck(sender=node_id, target=req_target, success=True, members=snapshots)
await ctx.reply(ack)
```

PingReqAck is sent immediately with `success=True` before knowing if target responded.

**Expected behavior:**
The prober should wait for actual Ack from target before sending PingReqAck.

### Problem 2: Connection Pending State Not Cleared on Failure

**Location:** `remote.py`, `remote_actor` and `_remote_connector`

**Current behavior (BEFORE FIX):**
When connection fails, `pending_connections` in `remote_actor` was never cleared.

**Fixed:** Now `_remote_connector` sends `_ConnectFailed` to `remote_actor`.

### Problem 3: swim_actor pending_connects Not Cleared on Failure

**Location:** `cluster.py`, `swim_actor`, `case Reply(result=ConnectFailed(...))`

**Current behavior (BEFORE FIX):**
When ConnectFailed received, `pending_connects` was not cleared.

**Fixed:** Now clears `pending_connects[peer]` on ConnectFailed.

### Problem 4: No Reply for Already-Pending Connections

**Location:** `remote.py`, `remote_actor`, `case Connect(...)` handler

**Current behavior:**
```python
if peer_addr in pending_connections:
    logger.debug("Connection already pending to peer", peer=peer_addr)
    continue  # NO REPLY!
```

The sender never gets a response if connection is already pending.

## Gossip Flow

```
+-----------------------------------------------------------------------------------+
|                              gossip_actor                                          |
+-----------------------------------------------------------------------------------+
        |
        | GossipTick (every tick_interval)
        v
+-------+-------------------------------------------------------+
|  1. If no data in store, skip                                 |
|  2. membership_ref.ask(GetAliveMembers)                       |
|  3. Pick random targets (fanout count)                        |
|  4. For each target:                                          |
|     - If not in pending_connects/pending_lookups:             |
|       - Add to pending_connects                               |
|       - remote_ref.send(Connect(...))                         |
+---------------------------------------------------------------+
        |
        v
+-------+-------------------------------------------------------+
|  On RemoteConnected:                                          |
|  1. Move from pending_connects to pending_lookups             |
|  2. remote_ref.send(Lookup("gossip", peer=address))           |
+---------------------------------------------------------------+
        |
        v
+-------+-------------------------------------------------------+
|  On LookupResult(ref):                                        |
|  1. Remove from pending_lookups                               |
|  2. For each key/value in store:                              |
|     ref.send(GossipPut(key, value, version))                  |
+---------------------------------------------------------------+


                    === SUBSCRIPTION ===

+-------+-------------------------------------------------------+
|  Actor sends Subscribe(pattern, subscriber)                   |
|  1. Add subscriber to subscribers[pattern]                    |
+---------------------------------------------------------------+

+-------+-------------------------------------------------------+
|  On GossipPut with updated data:                              |
|  1. Update store if version higher                            |
|  2. For matching subscribers:                                 |
|     subscriber.send(MembershipChanged(...))                   |
+---------------------------------------------------------------+
```

## Membership Actor State

```
+-----------------------------------------------------------------------------------+
|                           membership_actor                                         |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  State:                                                                           |
|  - members: dict[str, MemberInfo]                                                 |
|      - node_id: str                                                               |
|      - address: str                                                               |
|      - state: MemberState (ALIVE | DOWN)                                          |
|      - incarnation: int                                                           |
|  - hash_ring: HashRing                                                            |
|  - local_address: str                                                             |
|                                                                                   |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  Messages handled:                                                                |
|  - Join(node_id, address) -> Add/update member as ALIVE                           |
|  - MarkDown(node_id) -> Set member state to DOWN, remove from hash_ring           |
|  - MarkAlive(node_id, address) -> Set member state to ALIVE, add to hash_ring     |
|  - MergeMembership(snapshots) -> Merge remote membership info                     |
|  - GetAliveMembers() -> Return dict of alive members                              |
|  - GetAllMembers() -> Return dict of all members                                  |
|  - GetResponsibleNodes(actor_id, count) -> Hash ring lookup                       |
|  - GetLeaderId(actor_id, replicas) -> First alive node in hash ring               |
|  - IsLeader(actor_id, replicas) -> Check if this node is leader                   |
|  - GetAddress(node_id) -> Get member's address                                    |
|  - MembershipChanged(addresses) -> Update from gossip                             |
|                                                                                   |
+-----------------------------------------------------------------------------------+
```

## Session Communication

```
                Node A                                      Node B
                ======                                      ======

            session_actor                               session_actor
                 |                                            |
                 |  SendDeliver(target, payload)              |
                 |  ----------------------------------------> |
                 |                                            |
                 |            RemoteEnvelope                  |
                 |            type="deliver"                  |
                 |            target="actor-name"             |
                 |            payload=serialized_msg          |
                 |                                            |
                 |                                            v
                 |                                     _LocalLookup(target)
                 |                                            |
                 |                                            v
                 |                                     local_ref.send(msg)


                 |  SendAsk(target, payload, correlation_id)  |
                 |  ----------------------------------------> |
                 |                                            |
                 |            RemoteEnvelope                  |
                 |            type="ask"                      |
                 |            correlation_id="xxx"            |
                 |                                            |
                 |                                            v
                 |                                     local_ref.ask(msg)
                 |                                            |
                 |                                            v
                 |            RemoteEnvelope                  |
                 |  <---------------------------------------- |
                 |            type="reply"                    |
                 |            correlation_id="xxx"            |
                 |            payload=response                |
                 |                                            |
                 v                                            |
          pending[correlation_id]                             |
           -> reply_to.send(Reply(payload))                   |


                 |  SendLookup(name, correlation_id)          |
                 |  ----------------------------------------> |
                 |                                            |
                 |            RemoteEnvelope                  |
                 |            type="lookup"                   |
                 |            name="actor-name"               |
                 |                                            |
                 |                                            v
                 |                                     _LocalLookup(name)
                 |                                            |
                 |            RemoteEnvelope                  |
                 |  <---------------------------------------- |
                 |            type="lookup_result"            |
                 |            payload=b"1" or b"0"            |
```

## Recommended Fixes

### Fix 1: PingReq Should Wait for Actual Response

```python
# In swim_actor, when handling PingReq:

case PingReq(sender=req_sender, target=req_target, members=remote_members):
    # ... merge membership ...

    # Send Ping to target and track it
    ping_pending[req_target] = PingReqPendingState(
        requester=req_sender,
        reply_to=ctx.reply_to or ctx.sender,
    )

    if req_target in members:
        target_info = members[req_target]
        await initiate_send(target_info.address, SWIM_NAME,
            Ping(sender=node_id, members=snapshots))

    # Schedule timeout for this indirect probe
    await ctx.schedule(IndirectProbeTimeout(req_target), delay=probe_timeout)

    # DON'T send PingReqAck here!

case Ack(sender=ack_sender, ...):
    # ... existing handling ...

    # If this was for an indirect probe, notify requester
    if ack_sender in ping_pending:
        pending = ping_pending.pop(ack_sender)
        ack = PingReqAck(sender=node_id, target=ack_sender, success=True, members=snapshots)
        if pending.reply_to:
            await pending.reply_to.send(ack)

case IndirectProbeTimeout(target):
    if target in ping_pending:
        pending = ping_pending.pop(target)
        ack = PingReqAck(sender=node_id, target=target, success=False, members=snapshots)
        if pending.reply_to:
            await pending.reply_to.send(ack)
```

### Fix 2: Handle Already-Pending Connections

```python
# In remote_actor, when handling Connect:

case Connect(host, port):
    peer_addr = f"{host}:{port}"
    if peer_addr in sessions:
        await ctx.reply(RemoteConnected(...))
        continue

    if peer_addr in pending_connections:
        # Queue the reply_to so we can notify when connection completes
        pending_connections[peer_addr].append(ctx.sender)  # Store sender for later
        continue  # Still no immediate reply, but we'll reply later

    pending_connections[peer_addr] = [ctx.sender]  # Include initial sender
    # ... spawn connector ...

# When connection succeeds (_SessionConnected):
case _SessionConnected(peer_id, session):
    sessions[peer_id] = session
    waiting = pending_connections.pop(peer_id, [])
    for waiter in waiting:
        if waiter:
            await waiter.send(Reply(result=RemoteConnected(...)))

# When connection fails (_ConnectFailed):
case _ConnectFailed(peer_id, reason):
    waiting = pending_connections.pop(peer_id, [])
    for waiter in waiting:
        if waiter:
            await waiter.send(Reply(result=ConnectFailed(reason, peer=peer_id)))
```
