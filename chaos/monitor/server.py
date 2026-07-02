"""Consumer-lag monitor: samples the broker, tracks per-partition lag
history, fires alerts past a threshold, and serves a live dashboard.

Deliberately stdlib-only and broker-API-only (talks plain HTTP), so it
can be pointed at any minikafka broker:

    python3 chaos/monitor/server.py --broker http://127.0.0.1:9092 \
        --group chaos --threshold 5000 --port 9600

Alerts fire when a partition's lag exceeds --threshold and resolve once
it falls back under 80% of it (hysteresis, so a flapping partition does
not spam). Fired/resolved events go to stdout and, optionally, to
--alert-webhook as JSON POSTs.
"""

import argparse
import json
import os
import threading
import time
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INDEX_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "index.html")


class LagMonitor:
    def __init__(self, broker_url, group, threshold, interval=1.0,
                 history_len=600, webhook=None):
        self.broker_url = broker_url.rstrip("/")
        self.group = group
        self.threshold = threshold
        self.interval = interval
        self.webhook = webhook
        self._lock = threading.Lock()
        self.history = deque(maxlen=history_len)  # {"ts", "lags": {tp: n}}
        self.alerts = deque(maxlen=200)           # newest last
        self.active = {}    # tp-key -> lag at fire time
        self.fired_total = 0
        self.broker_up = False
        self.last_sample = None
        self._closed = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="lag-sampler", daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._closed.set()

    # -- sampling ------------------------------------------------------------

    def _loop(self):
        while not self._closed.wait(self.interval):
            try:
                with urllib.request.urlopen(
                        f"{self.broker_url}/metrics", timeout=5) as resp:
                    metrics = json.loads(resp.read())
                self._ingest(metrics)
                with self._lock:
                    self.broker_up = True
            except Exception:
                with self._lock:
                    self.broker_up = False

    def _ingest(self, metrics):
        group = metrics.get("groups", {}).get(self.group, {})
        committed = group.get("offsets", {})
        owners = {}
        for m in group.get("members", []):
            for t, p in m.get("assignment", []):
                owners[f"{t}[{p}]"] = m["id"]

        partitions = []
        lags = {}
        for topic, parts in sorted(metrics.get("topics", {}).items()):
            for p, rng in sorted(parts.items(), key=lambda kv: int(kv[0])):
                tp = f"{topic}[{p}]"
                have = committed.get(topic, {}).get(p, rng["start"])
                lag = max(rng["end"] - have, 0)
                lags[tp] = lag
                partitions.append({
                    "tp": tp,
                    "topic": topic,
                    "partition": int(p),
                    "end": rng["end"],
                    "committed": have,
                    "lag": lag,
                    "owner": owners.get(tp),
                })

        now = time.time()
        events = []
        with self._lock:
            self.history.append({"ts": now, "lags": lags})
            for row in partitions:
                tp, lag = row["tp"], row["lag"]
                if lag > self.threshold and tp not in self.active:
                    self.active[tp] = lag
                    self.fired_total += 1
                    events.append({"ts": now, "type": "fired", "tp": tp,
                                   "lag": lag, "threshold": self.threshold})
                elif tp in self.active and lag <= self.threshold * 0.8:
                    del self.active[tp]
                    events.append({"ts": now, "type": "resolved", "tp": tp,
                                   "lag": lag, "threshold": self.threshold})
                row["alert"] = tp in self.active
            self.alerts.extend(events)
            self.last_sample = {
                "ts": now,
                "partitions": partitions,
                "group": {
                    "name": self.group,
                    "generation": group.get("generation"),
                    "members": [m["id"] for m in group.get("members", [])],
                },
            }
        for e in events:
            self._emit(e)

    def _emit(self, event):
        icon = "🔥 ALERT" if event["type"] == "fired" else "✅ RESOLVED"
        print(f"[monitor {time.strftime('%H:%M:%S')}] {icon} "
              f"{event['tp']} lag={event['lag']:,} "
              f"(threshold {event['threshold']:,})", flush=True)
        if self.webhook:
            try:
                req = urllib.request.Request(
                    self.webhook, data=json.dumps(event).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST")
                urllib.request.urlopen(req, timeout=5).close()
            except Exception as e:
                print(f"[monitor] webhook delivery failed: {e}", flush=True)

    # -- state for the UI ---------------------------------------------------

    def state(self):
        with self._lock:
            sample = self.last_sample or {"ts": None, "partitions": [],
                                          "group": {"name": self.group,
                                                    "generation": None,
                                                    "members": []}}
            return {
                "now": time.time(),
                "broker_up": self.broker_up,
                "broker_url": self.broker_url,
                "threshold": self.threshold,
                "fired_total": self.fired_total,
                "active_alerts": sorted(self.active),
                "sample": sample,
                "history": list(self.history),
                "alerts": list(self.alerts)[-50:][::-1],  # newest first
            }


def make_handler(monitor):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _send(self, status, body, ctype):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            try:
                if self.path == "/" or self.path.startswith("/index"):
                    with open(INDEX_HTML, "rb") as f:
                        self._send(200, f.read(), "text/html; charset=utf-8")
                elif self.path.startswith("/api/state"):
                    self._send(200, json.dumps(monitor.state()).encode(),
                               "application/json")
                else:
                    self._send(404, b'{"error": "not_found"}',
                               "application/json")
            except (BrokenPipeError, ConnectionResetError):
                pass

    return Handler


def main(argv=None):
    ap = argparse.ArgumentParser(description="minikafka lag monitor")
    ap.add_argument("--broker", default="http://127.0.0.1:9092")
    ap.add_argument("--group", default="chaos")
    ap.add_argument("--port", type=int, default=9600)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--threshold", type=int, default=5000,
                    help="per-partition lag that fires an alert")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--alert-webhook", default=None,
                    help="optional URL to POST alert events to")
    args = ap.parse_args(argv)

    monitor = LagMonitor(args.broker, args.group, args.threshold,
                         interval=args.interval, webhook=args.alert_webhook)
    monitor.start()
    httpd = ThreadingHTTPServer((args.host, args.port),
                                make_handler(monitor))
    httpd.daemon_threads = True
    print(f"[monitor] dashboard on http://{args.host}:{args.port} "
          f"(watching group {args.group!r} on {args.broker}, "
          f"alert threshold {args.threshold:,})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop()


if __name__ == "__main__":
    main()
