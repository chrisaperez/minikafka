"""Chaos producer: pumps a firehose of keyed events at the broker.

Rate-limited with a simple token-bucket-style pacing loop; pass --rate 0
to go as fast as the broker will take them. Designed to run for millions
of events:

    python3 -m chaos.producer --total 2000000 --rate 5000
"""

import argparse
import random
import time

from minikafka.client import BrokerClient, Producer

ACTIONS = ["click", "view", "purchase", "refund", "login", "logout"]


def main(argv=None):
    ap = argparse.ArgumentParser(description="minikafka chaos producer")
    ap.add_argument("--broker", default="http://127.0.0.1:9092")
    ap.add_argument("--topic", default="events")
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--rate", type=float, default=3000,
                    help="target events/sec (0 = unthrottled)")
    ap.add_argument("--total", type=int, default=1_000_000)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--users", type=int, default=500,
                    help="distinct keys (spread across partitions)")
    ap.add_argument("--report-every", type=float, default=2.0)
    args = ap.parse_args(argv)

    BrokerClient(args.broker).create_topic(
        args.topic, args.partitions, exist_ok=True)
    producer = Producer(args.broker, batch_size=args.batch)

    print(f"[producer] pumping {args.total:,} events into "
          f"{args.topic!r} at ~{args.rate or 'max'}/s", flush=True)

    start = time.monotonic()
    sent = 0
    last_report_t, last_report_n = start, 0
    try:
        while sent < args.total:
            if args.rate > 0:
                due = min(args.total,
                          int((time.monotonic() - start) * args.rate))
                if due <= sent:
                    time.sleep(0.02)
                    continue
                chunk = min(due - sent, args.batch)
            else:
                chunk = min(args.total - sent, args.batch)
            for _ in range(chunk):
                uid = f"user-{sent % args.users}"
                producer.send(args.topic, {
                    "event_id": sent,
                    "user_id": uid,
                    "action": random.choice(ACTIONS),
                    "amount": round(random.uniform(1, 250), 2),
                }, key=uid)
                sent += 1
            now = time.monotonic()
            if now - last_report_t >= args.report_every:
                rate = (sent - last_report_n) / (now - last_report_t)
                print(f"[producer] {sent:,}/{args.total:,} sent "
                      f"({rate:,.0f}/s)", flush=True)
                last_report_t, last_report_n = now, sent
    except KeyboardInterrupt:
        print("[producer] interrupted", flush=True)
    finally:
        producer.close()
    elapsed = time.monotonic() - start
    print(f"[producer] done: {sent:,} events in {elapsed:,.1f}s "
          f"({sent / max(elapsed, 1e-9):,.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
