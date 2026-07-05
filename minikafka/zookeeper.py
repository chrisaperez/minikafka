"""A simple HTTP-based metadata registry acting as a lightweight Zookeeper."""

import argparse
import json
import re
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# Same constraint the broker enforces. Because brokers turn topic names into
# on-disk paths (``<data_dir>/<name>-<p>``), the registry must reject names
# that could escape the data directory (slashes, "..", etc.).
_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,255}$")


class Zookeeper:
    def __init__(self, session_timeout=10.0):
        self.session_timeout = session_timeout
        self.lock = threading.RLock()
        self.brokers = {}  # id -> {"host": host, "port": port, "last_heartbeat": ts}
        self.topics = {}   # name -> [{"partition": p, "leader": id, "replicas": [ids], "isr": [ids]}]

    def register_broker(self, broker_id, host, port):
        with self.lock:
            self.brokers[broker_id] = {
                "host": host,
                "port": port,
                "last_heartbeat": time.time()
            }
        return {"ok": True}

    def get_brokers(self):
        with self.lock:
            now = time.time()
            # Filter out dead brokers
            active = {
                bid: info for bid, info in self.brokers.items()
                if now - info["last_heartbeat"] <= self.session_timeout
            }
            return active

    def create_topic(self, name, partitions, replication_factor):
        with self.lock:
            if not isinstance(name, str) or not _TOPIC_RE.match(name):
                return {"error": f"invalid topic name: {name!r}"}
            if not isinstance(partitions, int) or isinstance(partitions, bool) or partitions < 1:
                return {"error": "partitions must be a positive integer"}
            if (not isinstance(replication_factor, int) or isinstance(replication_factor, bool)
                    or replication_factor < 1):
                return {"error": "replication_factor must be a positive integer"}
            if name in self.topics:
                return {"error": "topic already exists"}
            active_brokers = list(self.get_brokers().keys())
            if not active_brokers:
                return {"error": "no active brokers"}
            
            if replication_factor > len(active_brokers):
                return {"error": "replication factor exceeds number of active brokers"}

            parts = []
            for p in range(partitions):
                replicas = secrets.SystemRandom().sample(active_brokers, replication_factor)
                leader = replicas[0]
                parts.append({
                    "partition": p,
                    "leader": leader,
                    "replicas": replicas,
                    "isr": replicas.copy()
                })
            self.topics[name] = parts
            return {"name": name, "partitions": parts}

    def get_topics(self):
        with self.lock:
            return self.topics

    def delete_topic(self, name):
        with self.lock:
            if name in self.topics:
                del self.topics[name]
                return {"ok": True}
            return {"error": "topic not found"}

    def update_isr(self, topic, partition, isr):
        with self.lock:
            if topic not in self.topics:
                return {"error": "topic not found"}
            if not (0 <= partition < len(self.topics[topic])):
                return {"error": "partition not found"}
            self.topics[topic][partition]["isr"] = isr
            return {"ok": True}


class _Handler(BaseHTTPRequestHandler):
    zk = None

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/brokers":
            self._send(200, self.zk.get_brokers())
        elif url.path == "/topics":
            self._send(200, self.zk.get_topics())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        url = urlparse(self.path)
        body = self._body()
        if url.path == "/brokers":
            res = self.zk.register_broker(body.get("id"), body.get("host"), body.get("port"))
            self._send(200, res)
        elif url.path == "/topics":
            res = self.zk.create_topic(body.get("name"), body.get("partitions", 1), body.get("replication_factor", 1))
            self._send(200 if "error" not in res else 400, res)
        elif url.path == "/topics/isr":
            res = self.zk.update_isr(body.get("topic"), body.get("partition"), body.get("isr"))
            self._send(200 if "error" not in res else 400, res)
        else:
            self._send(404, {"error": "not found"})

    def do_DELETE(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        if url.path == "/topics":
            name = q.get("name", [""])[0]
            res = self.zk.delete_topic(name)
            self._send(200 if "error" not in res else 404, res)
        else:
            self._send(404, {"error": "not found"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2181)
    args = ap.parse_args()

    zk = Zookeeper()
    handler = type("BoundHandler", (_Handler,), {"zk": zk})
    httpd = ThreadingHTTPServer((args.host, args.port), handler)
    httpd.daemon_threads = True

    print(f"Zookeeper listening on http://{args.host}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()

if __name__ == "__main__":
    main()
