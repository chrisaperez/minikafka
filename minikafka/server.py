"""HTTP/JSON front-end for the broker.

Real Kafka speaks a custom binary protocol; this uses HTTP+JSON to stay
readable and debuggable with curl. API:

  GET  /healthz
  GET  /metrics                     broker-wide offsets, groups, lag
  GET  /topics
  POST /topics                      {"name": ..., "partitions": N}
  POST /produce                     {"topic", "records": [{"key","value"}],
                                     "partition": optional}
  GET  /fetch?topic&partition&offset&max_records&wait_ms   (long-poll)
  POST /groups/join                 {"group","consumer_id","topics":[...]}
  POST /groups/heartbeat            {"group","consumer_id","generation"}
  POST /groups/leave                {"group","consumer_id"}
  POST /groups/commit               {"group","consumer_id","generation",
                                     "offsets": {topic: {"0": off}}}
  GET  /groups/offsets?group=...
"""

import argparse
import json
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .broker import Broker
from .errors import BadRequest, BrokerError


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    broker = None  # bound to a Broker by make_server()

    def log_message(self, fmt, *args):  # silence per-request stderr noise
        pass

    # -- plumbing -----------------------------------------------------------

    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        try:
            body = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            raise BadRequest("request body is not valid JSON")
        if not isinstance(body, dict):
            raise BadRequest("request body must be a JSON object")
        return body

    def _dispatch(self, fn):
        try:
            self._send(*fn())
        except BrokerError as e:
            self._send(e.status, e.to_dict())
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            traceback.print_exc()
            try:
                self._send(500, {"error": "internal", "message": "internal error"})
            except (BrokenPipeError, ConnectionResetError):
                pass

    # -- routes ------------------------------------------------------------

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        b = self.broker

        def route():
            if url.path == "/healthz":
                return 200, {"ok": True}
            if url.path == "/metrics":
                return 200, b.metrics()
            if url.path == "/topics":
                return 200, {"topics": b.topics()}
            if url.path == "/fetch":
                return 200, b.fetch(
                    q.get("topic", ""),
                    _as_int(q, "partition"),
                    _as_int(q, "offset"),
                    max_records=_as_int(q, "max_records", 500),
                    wait_ms=_as_int(q, "wait_ms", 0),
                    replica_id=q.get("replica_id")
                )
            if url.path == "/groups":
                return 200, {"groups": b.coordinator.describe()}
            if url.path == "/groups/offsets":
                group = q.get("group")
                if not group:
                    raise BadRequest("group query parameter is required")
                return 200, {"group": group,
                             "offsets": b.coordinator.committed(group)}
            return 404, {"error": "not_found", "message": url.path}

        self._dispatch(route)

    def do_POST(self):
        url = urlparse(self.path)
        b = self.broker

        def route():
            body = self._body()
            if url.path == "/topics":
                return 201, b.create_topic(
                    body.get("name"), 
                    body.get("partitions", 1),
                    body.get("replication_factor", 1)
                )
            if url.path == "/topics/compact":
                return 200, b.compact(
                    body.get("topic", ""),
                    body.get("partition", 0)
                )
            if url.path == "/produce":
                results = b.produce(
                    body.get("topic", ""),
                    body.get("records"),
                    partition=body.get("partition"),
                )
                return 200, {"results": results}
            if url.path == "/groups/join":
                return 200, b.coordinator.join(
                    body.get("group"), body.get("consumer_id"),
                    body.get("topics", []))
            if url.path == "/groups/heartbeat":
                return 200, b.coordinator.heartbeat(
                    body.get("group"), body.get("consumer_id"),
                    body.get("generation"))
            if url.path == "/groups/leave":
                return 200, b.coordinator.leave(
                    body.get("group"), body.get("consumer_id"))
            if url.path == "/groups/commit":
                return 200, b.coordinator.commit(
                    body.get("group"), body.get("consumer_id"),
                    body.get("generation"), body.get("offsets") or {})
            return 404, {"error": "not_found", "message": url.path}
        
        self._dispatch(route)

    def do_DELETE(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        b = self.broker
        def route():
            if url.path == "/topics":
                name = q.get("name")
                if not name:
                    raise BadRequest("name query parameter is required")
                return 200, b.delete_topic(name)
            return 404, {"error": "not_found", "message": url.path}
        self._dispatch(route)

def _as_int(q, name, default=None):
    raw = q.get(name)
    if raw is None:
        if default is None:
            raise BadRequest(f"query parameter {name!r} is required")
        return default
    try:
        return int(raw)
    except ValueError:
        raise BadRequest(f"query parameter {name!r} must be an integer")


def make_server(broker, host="127.0.0.1", port=9092):
    handler = type("BoundHandler", (_Handler,), {"broker": broker})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    return httpd


def main(argv=None):
    ap = argparse.ArgumentParser(description="minikafka broker")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9092)
    ap.add_argument("--broker-id", required=True, help="unique broker id (e.g. broker-1)")
    ap.add_argument("--zk-url", default=None, help="url of the Zookeeper registry (e.g. http://127.0.0.1:2181)")
    ap.add_argument("--data-dir", default=None,
                    help="enable persistence; logs+offsets survive restarts")
    ap.add_argument("--retention-max-records", type=int, default=None,
                    help="in-memory mode: cap records kept per partition")
    ap.add_argument("--session-timeout", type=float, default=10.0,
                    help="seconds without heartbeat before a consumer is "
                         "evicted from its group")
    ap.add_argument("--fsync", action="store_true",
                    help="fsync after every append (durable but slower)")
    ap.add_argument("--segment-bytes", type=int, default=256*1024*1024,
                    help="max bytes per segment file")
    args = ap.parse_args(argv)

    broker = Broker(
        broker_id=args.broker_id,
        host=args.host,
        port=args.port,
        zk_url=args.zk_url,
        data_dir=args.data_dir,
        retention_max_records=args.retention_max_records,
        session_timeout=args.session_timeout,
        fsync=args.fsync,
        segment_bytes=args.segment_bytes,
    )
    broker.coordinator.start_reaper()
    httpd = make_server(broker, args.host, args.port)
    mode = f"persistent (data dir: {args.data_dir})" if args.data_dir \
        else "in-memory"
    print(f"minikafka broker {args.broker_id} listening on http://{args.host}:{args.port} "
          f"[{mode}]", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        broker.close()
        print("broker stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
