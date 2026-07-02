# minikafka

A small, educational implementation of Apache Kafka's core ideas in pure
Python (stdlib only — nothing to install), plus a **chaos consumer-lag
simulator** with a real-time monitoring dashboard on top.

```
┌─────────────┐   POST /produce    ┌──────────────────────────────┐
│  Producer   │ ─────────────────▶ │  Broker (HTTP, port 9092)    │
└─────────────┘                    │                              │
                                   │  topic "events"              │
┌─────────────┐   GET /fetch       │   ├─ partition 0  [record…]  │
│  Consumer   │ ◀───────────────── │   ├─ partition 1  [record…]  │
│  (group     │   POST /groups/*   │   └─ partition N  [record…]  │
│   member)   │ ─────────────────▶ │                              │
└─────────────┘  join/heartbeat/   │  GroupCoordinator            │
                 commit            │   ├─ membership + heartbeats │
┌─────────────┐   GET /metrics     │   ├─ rebalancing (gens)      │
│ Lag monitor │ ◀───────────────── │   └─ committed offsets       │
│ (port 9600) │                    └──────────────────────────────┘
└─────────────┘                        │ optional --data-dir
                                       ▼
                            *.jsonl logs + topics.json
                            + __consumer_offsets.json
```

## What's implemented

- **Broker / topics / partitions** — topics are split into N append-only
  partition logs with dense integer offsets.
- **Producer & consumer APIs** — publish (batched, keyed or round-robin
  partitioning) and subscribe (long-polling fetch) over HTTP/JSON.
- **Message storage** — in-memory by default (with optional retention
  caps); pass `--data-dir` for persistent JSONL logs that survive
  restarts, including truncation of torn writes after unclean shutdown.
- **Consumer groups & offsets** — join/heartbeat/leave, automatic
  round-robin rebalancing on membership or topic changes, session
  timeouts that evict dead members, generation numbers that fence zombie
  commits, and persisted committed offsets → at-least-once delivery.

## Quickstart

```bash
cd minikafka

# run the tests (pure stdlib, unittest)
python3 -m unittest discover -s tests

# start a persistent broker
python3 -m minikafka.server --port 9092 --data-dir ./data
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

## Chaos consumer-lag simulator

One command runs the whole experiment:

```bash
./run_demo.sh          # broker + monitor + producer + 3 chaotic consumers
```

then open **http://127.0.0.1:9600**. Or run the pieces yourself:

```bash
python3 -m minikafka.server --port 9092 --data-dir ./data

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

| Method & path          | Purpose                                          |
| ---------------------- | ------------------------------------------------ |
| `POST /topics`         | create topic `{"name", "partitions"}`            |
| `GET /topics`          | list topics                                      |
| `POST /produce`        | append records `{"topic", "records": [...]}`     |
| `GET /fetch`           | read records (`offset`, `max_records`, `wait_ms` long-poll) |
| `POST /groups/join`    | join group → generation, assignment, offsets     |
| `POST /groups/heartbeat` | stay alive; learn about rebalances             |
| `POST /groups/commit`  | commit offsets (generation-fenced)               |
| `POST /groups/leave`   | leave gracefully                                 |
| `GET /groups/offsets`  | committed offsets for a group                    |
| `GET /metrics`         | end offsets, groups, members, per-partition lag  |

## Simplifications vs. real Kafka

Single broker (no replication/ISR), HTTP+JSON instead of the binary
protocol, server-side partitioner, one JSONL segment per partition (no
segment rolling/compaction), offsets in a JSON file instead of an
internal topic, and rebalances are computed instantly by the coordinator
rather than negotiated between members. The *semantics* consumers see —
ordered partitions, consumer groups, generations, session timeouts,
at-least-once redelivery — match the real thing.
