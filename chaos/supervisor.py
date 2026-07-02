"""Chaos supervisor: runs the whole experiment and keeps it running.

Spawns the firehose producer plus N chaos consumers as subprocesses.
When a consumer crashes (which it will, on purpose), the supervisor logs
the death and resurrects it after --restart-delay seconds — the window
in which that member's partitions sit idle and lag climbs until the
session timeout reassigns them, then drains after restart.

    python3 -m chaos.supervisor --consumers 3 --total 1000000
"""

import argparse
import signal
import subprocess
import sys
import time


def ts():
    return time.strftime("%H:%M:%S")


def spawn_consumer(args, consumer_id):
    return subprocess.Popen([
        sys.executable, "-u", "-m", "chaos.consumer",
        "--broker", args.broker,
        "--topic", args.topic,
        "--group", args.group,
        "--id", consumer_id,
        "--process-ms", str(args.process_ms),
        "--crash-mean-s", str(args.crash_mean_s),
        "--commit-every", str(args.commit_every),
    ])


def spawn_producer(args):
    return subprocess.Popen([
        sys.executable, "-u", "-m", "chaos.producer",
        "--broker", args.broker,
        "--topic", args.topic,
        "--partitions", str(args.partitions),
        "--rate", str(args.rate),
        "--total", str(args.total),
    ])


def main(argv=None):
    ap = argparse.ArgumentParser(description="minikafka chaos supervisor")
    ap.add_argument("--broker", default="http://127.0.0.1:9092")
    ap.add_argument("--topic", default="events")
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--group", default="chaos")
    ap.add_argument("--consumers", type=int, default=3)
    ap.add_argument("--restart-delay", type=float, default=6.0,
                    help="seconds a crashed consumer stays dead")
    ap.add_argument("--no-producer", action="store_true")
    ap.add_argument("--rate", type=float, default=3000)
    ap.add_argument("--total", type=int, default=1_000_000)
    ap.add_argument("--process-ms", type=float, default=1.0)
    ap.add_argument("--crash-mean-s", type=float, default=30.0)
    ap.add_argument("--commit-every", type=int, default=1000)
    args = ap.parse_args(argv)

    producer = None if args.no_producer else spawn_producer(args)
    consumers = {}   # consumer_id -> Popen
    pending = {}     # consumer_id -> restart deadline
    deaths = 0
    for i in range(args.consumers):
        cid = f"consumer-{i}"
        consumers[cid] = spawn_consumer(args, cid)
    print(f"[supervisor {ts()}] started "
          f"{'producer + ' if producer else ''}{args.consumers} consumers "
          f"(crash mean {args.crash_mean_s}s, restart delay "
          f"{args.restart_delay}s)", flush=True)

    stopping = False

    def shutdown(_sig=None, _frame=None):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while not stopping:
            time.sleep(0.5)
            now = time.monotonic()

            for cid, proc in list(consumers.items()):
                rc = proc.poll()
                if rc is not None:
                    deaths += 1
                    del consumers[cid]
                    pending[cid] = now + args.restart_delay
                    print(f"[supervisor {ts()}] ☠️  {cid} died "
                          f"(exit {rc}, death #{deaths}) — restarting in "
                          f"{args.restart_delay:.0f}s", flush=True)

            for cid, deadline in list(pending.items()):
                if now >= deadline:
                    del pending[cid]
                    consumers[cid] = spawn_consumer(args, cid)
                    print(f"[supervisor {ts()}] ♻️  {cid} restarted",
                          flush=True)

            if producer is not None and producer.poll() is not None:
                rc = producer.returncode
                print(f"[supervisor {ts()}] producer finished (exit {rc}); "
                      f"consumers keep draining", flush=True)
                producer = None
    finally:
        print(f"[supervisor {ts()}] shutting down "
              f"({deaths} chaos deaths total)", flush=True)
        procs = list(consumers.values())
        if producer is not None:
            procs.append(producer)
        for p in procs:
            if p.poll() is None:
                p.terminate()
        deadline = time.monotonic() + 5
        for p in procs:
            try:
                p.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
