# minikafka

A small, educational implementation of Apache Kafka's core ideas in pure
Python (stdlib only — nothing to install), plus a **chaos consumer-lag
simulator** with a real-time monitoring dashboard on top.

```
                        ┌──────────────────────────────┐
                        │  Zookeeper (optional)        │
                        │  HTTP registry, port 2181    │
                        │   ├─ live brokers            │
                        │   └─ topic → leader/replicas │
                        └───────────────┬──────────────┘
                     register / sync    │   (--zk-url)
        ┌───────────────────────────────┼───────────────────────────────┐
        ▼                               ▼                               ▼
┌──────────────────┐  replicate ┌──────────────────┐  replicate ┌──────────────────┐
│ Broker b1        │◀──────────▶│ Broker b2        │◀──────────▶│ Broker b3        │
│  leader: part 0  │            │  leader: part 1  │            │  follower(s)     │
└──────────────────┘            └──────────────────┘            └──────────────────┘
        ▲  ▲                                                        │ --data-dir
        │  │ POST /produce   GET /fetch   POST /groups/*            ▼
┌───────┴┐ └─────────┐                                 *.jsonl segment logs
│Producer│ │ Consumer│                                 + __consumer_offsets.json
└────────┘ │ (group) │      ┌─────────────┐  GET /metrics
           └─────────┘      │ Lag monitor │◀───────────────  per-partition lag
                            │ (port 9600) │
                            └─────────────┘
```

Run a single broker and it behaves as a standalone Kafka; point several
brokers at a Zookeeper registry and topics gain leaders, followers, and an
in-sync-replica (ISR) set.

## What's implemented

- **Broker / topics / partitions** — topics are split into N append-only
  partition logs with dense integer offsets.
- **Producer & consumer APIs** — publish (batched, keyed or round-robin
  partitioning) and subscribe (long-polling fetch) over HTTP/JSON.
- **Persistent storage & segments** — in-memory by default (with optional
  retention caps); pass `--data-dir` for persistent JSONL logs that
  survive restarts, including truncation of torn writes after an unclean
  shutdown. Logs roll into new segment files at `--segment-bytes`.
- **Log compaction** — `POST /topics/compact` rewrites the inactive
  segments keeping only the latest value per key (the active segment is
  left untouched, as in Kafka).
- **Replication & ISR** — multiple brokers register with a lightweight
  Zookeeper registry (`--zk-url`); each partition gets a leader and
  follower replicas that pull from the leader, and the leader tracks the
  in-sync-replica set and reports it via `/metrics` and `/fetch`.
- **Consumer groups & offsets** — join/heartbeat/leave, automatic
  round-robin rebalancing on membership or topic changes, session
  timeouts that evict dead members, generation numbers that fence zombie
  commits, and persisted committed offsets → at-least-once delivery.

## Quickstart

```bash
cd minikafka

# run the tests (pure stdlib, unittest)
python3 -m unittest discover -s tests

# start a single persistent broker (--broker-id is required)
python3 -m minikafka.server --broker-id b1 --port 9092 --data-dir ./data
```

```python
from minikafka.client import BrokerClient, Consumer, Producer

BrokerClient("http://127.0.0.1:9092").create_topic("orders", partitions=4)

p = Producer("http://127.0.0.1:9092")
p.send("orders", {"amount": 9.99}, key="user-1")   # same key → same partition
p.close()

c = Consumer("http://127.0.0.1:9092", group="billing", topics=["orders"])
for record in c.poll(timeout=2.0):
    print(record["partition"], record["offset"], record["value"])
c.commit()   # progress survives restarts; uncommitted work is redelivered
c.close()
```

## Running a replicated cluster

Start the Zookeeper registry, then point brokers at it with `--zk-url`.
Each broker needs a unique `--broker-id` and its own `--data-dir`.

```bash
# 1. registry
python3 -m minikafka.zookeeper --port 2181

# 2. three brokers
python3 -m minikafka.server --broker-id b1 --port 9091 \
    --zk-url http://127.0.0.1:2181 --data-dir ./b1_data
python3 -m minikafka.server --broker-id b2 --port 9092 \
    --zk-url http://127.0.0.1:2181 --data-dir ./b2_data
python3 -m minikafka.server --broker-id b3 --port 9093 \
    --zk-url http://127.0.0.1:2181 --data-dir ./b3_data

# 3. create a replicated topic (any broker forwards to the registry)
curl -X POST http://127.0.0.1:9091/topics \
    -d '{"name": "events", "partitions": 3, "replication_factor": 3}'
```

Produce to a partition's **leader** (check `GET /topics` on the registry, or
`/metrics` on a broker, to find it); followers pull from the leader and join
the ISR once caught up.

## Chaos consumer-lag simulator

One command runs the whole experiment:

```bash
./run_demo.sh          # broker + monitor + producer + 3 chaotic consumers
```

then open **http://127.0.0.1:9600**. Or run the pieces yourself:

```bash
python3 -m minikafka.server --broker-id b1 --port 9092 --data-dir ./data

# firehose: millions of keyed events, rate-limited
python3 -m chaos.producer --total 2000000 --rate 4000

# 3 consumers that crash on purpose (hard exit, no commit) and get
# resurrected by the supervisor after a delay
python3 -m chaos.supervisor --consumers 3 --crash-mean-s 30 --no-producer

# real-time per-partition lag dashboard + alerting
python3 chaos/monitor/server.py --group chaos --threshold 5000
```

What you'll see on the dashboard: lag per partition sawtooths — it climbs
while a consumer is dead (its partitions sit idle until the session
timeout reassigns them), the alert fires past the threshold (banner, feed,
stdout, optional `--alert-webhook`), then lag drains after the supervisor
restarts the consumer and the alert resolves (with hysteresis at 80% of
the threshold).

## HTTP API

| Method & path            | Purpose                                              |
| ------------------------ | ---------------------------------------------------- |
| `GET /healthz`           | liveness check                                       |
| `POST /topics`           | create topic `{"name", "partitions", "replication_factor"}` |
| `GET /topics`            | list topics                                          |
| `DELETE /topics?name=`   | delete a topic and its logs                          |
| `POST /topics/compact`   | compact a partition `{"topic", "partition"}`         |
| `POST /produce`          | append records `{"topic", "records": [...], "partition"?}` |
| `GET /fetch`             | read records (`offset`, `max_records`, `wait_ms` long-poll) |
| `POST /groups/join`      | join group → generation, assignment, offsets         |
| `POST /groups/heartbeat` | stay alive; learn about rebalances                   |
| `POST /groups/commit`    | commit offsets (generation-fenced)                   |
| `POST /groups/leave`     | leave gracefully                                     |
| `GET /groups` `/groups/offsets` | group membership / committed offsets          |
| `GET /metrics`           | end offsets, ISR, groups, members, per-partition lag |

## Server flags

| Flag                       | Meaning                                              |
| -------------------------- | ---------------------------------------------------- |
| `--broker-id` (required)   | unique id for this broker (e.g. `b1`)                |
| `--host` / `--port`        | listen address (default `127.0.0.1:9092`)            |
| `--zk-url`                 | Zookeeper registry URL → enables clustering/replication |
| `--data-dir`               | enable persistence; logs + offsets survive restarts  |
| `--segment-bytes`          | roll to a new segment file past this size            |
| `--retention-max-records`  | in-memory mode: cap records kept per partition       |
| `--session-timeout`        | seconds without heartbeat before a consumer is evicted |
| `--fsync`                  | fsync after every append (durable, slower)           |

## Simplifications vs. real Kafka

HTTP+JSON instead of the binary protocol; a small HTTP "Zookeeper" registry
instead of a real quorum; a server-side partitioner; committed offsets in a
JSON file rather than an internal `__consumer_offsets` topic; rebalances
computed instantly by the coordinator rather than negotiated between
members; and follower replication that pulls whole records over `/fetch`
rather than a replicated byte log. The *semantics* consumers see — ordered
partitions, leaders/followers with an ISR set, segment rolling and
compaction, consumer groups, generations, session timeouts, at-least-once
redelivery — match the real thing.
