"""Chaos consumer: processes events slowly, commits periodically, and —
crucially — dies without warning.

Crashes are hard exits (os._exit) at a random exponentially-distributed
time, so nothing is flushed or committed on the way down. Uncommitted
records get redelivered after restart: watch lag spike while a member is
dead, then drain when the supervisor resurrects it.
"""

import argparse
import os
import random
import signal
import sys
import time

from minikafka.client import Consumer

CRASH_EXIT_CODE = 137  # mimic a SIGKILLed process


def main(argv=None):
    ap = argparse.ArgumentParser(description="minikafka chaos consumer")
    ap.add_argument("--broker", default="http://127.0.0.1:9092")
    ap.add_argument("--topic", default="events")
    ap.add_argument("--group", default="chaos")
    ap.add_argument("--id", dest="consumer_id", required=True)
    ap.add_argument("--process-ms", type=float, default=1.0,
                    help="simulated work per record (caps throughput)")
    ap.add_argument("--commit-every", type=int, default=1000,
                    help="records between offset commits")
    ap.add_argument("--commit-interval-s", type=float, default=5.0,
                    help="also commit at least this often, so the tail "
                         "of a burst doesn't sit uncommitted forever")
    ap.add_argument("--crash-mean-s", type=float, default=30.0,
                    help="mean seconds between crashes (0 disables chaos)")
    ap.add_argument("--min-uptime-s", type=float, default=5.0)
    ap.add_argument("--report-every", type=float, default=5.0)
    args = ap.parse_args(argv)

    me = args.consumer_id
    consumer = Consumer(args.broker, args.group, [args.topic],
                        consumer_id=me)

    def graceful(_sig, _frame):
        print(f"[{me}] SIGTERM: committing and leaving group", flush=True)
        consumer.commit()
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful)

    crash_at = None
    if args.crash_mean_s > 0:
        crash_at = time.monotonic() + max(
            args.min_uptime_s, random.expovariate(1.0 / args.crash_mean_s))
        print(f"[{me}] up (pid {os.getpid()}); scheduled to crash in "
              f"{crash_at - time.monotonic():.0f}s", flush=True)
    else:
        print(f"[{me}] up (pid {os.getpid()}); chaos disabled", flush=True)

    processed = 0
    uncommitted = 0
    last_report = time.monotonic()
    last_commit = time.monotonic()
    while True:
        if crash_at is not None and time.monotonic() >= crash_at:
            print(f"[{me}] 💥 CHAOS: hard crash after {processed:,} records "
                  f"({uncommitted} uncommitted — will be redelivered)",
                  flush=True)
            os._exit(CRASH_EXIT_CODE)  # no commit, no goodbye

        records = consumer.poll(timeout=1.0, max_records=500)
        if records:
            # simulate per-record work without a syscall per record
            time.sleep(len(records) * args.process_ms / 1000.0)
            processed += len(records)
            uncommitted += len(records)
        due = (uncommitted >= args.commit_every
               or (uncommitted > 0 and time.monotonic() - last_commit
                   >= args.commit_interval_s))
        if due and consumer.commit():
            uncommitted = 0
            last_commit = time.monotonic()

        now = time.monotonic()
        if now - last_report >= args.report_every:
            parts = sorted(p for _, p in consumer.assignment)
            print(f"[{me}] processed={processed:,} "
                  f"partitions={parts}", flush=True)
            last_report = now


if __name__ == "__main__":
    main()
