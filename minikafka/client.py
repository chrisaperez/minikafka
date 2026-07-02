"""Producer / Consumer clients for the minikafka HTTP API (stdlib only)."""

import json
import threading
import time
import urllib.error
import urllib.request
import uuid


class ClientError(Exception):
    """An error response from the broker."""

    def __init__(self, status, payload):
        self.status = status
        self.code = payload.get("error", "unknown")
        self.payload = payload
        super().__init__(f"{self.code}: {payload.get('message')}")


def _request(method, url, body=None, timeout=30.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read())
        except Exception:
            payload = {"error": "http_error", "message": str(e)}
        raise ClientError(e.code, payload) from None


class BrokerClient:
    """Thin admin/metadata client."""

    def __init__(self, broker_url):
        self.url = broker_url.rstrip("/")

    def create_topic(self, name, partitions=1, exist_ok=False):
        try:
            return _request("POST", f"{self.url}/topics",
                            {"name": name, "partitions": partitions})
        except ClientError as e:
            if exist_ok and e.code == "topic_already_exists":
                return {"name": name, "partitions": e.payload.get("partitions")}
            raise

    def topics(self):
        return _request("GET", f"{self.url}/topics")["topics"]

    def metrics(self):
        return _request("GET", f"{self.url}/metrics")


class Producer:
    """Batching producer.

    send() buffers; a batch is flushed when it reaches ``batch_size`` or a
    background thread flushes lingering records every ``linger_s``. Call
    flush()/close() to drain. Transient connection errors are retried with
    backoff so a broker restart doesn't lose buffered records.
    """

    def __init__(self, broker_url, batch_size=500, linger_s=0.25,
                 retry_backoff_s=0.5, max_retries=60):
        self.url = broker_url.rstrip("/")
        self.batch_size = batch_size
        self.retry_backoff_s = retry_backoff_s
        self.max_retries = max_retries
        self._lock = threading.Lock()
        self._buffers = {}  # topic -> list of {"key","value"}
        self._closed = threading.Event()
        self._linger = threading.Thread(
            target=self._linger_loop, args=(linger_s,),
            name="producer-linger", daemon=True)
        self._linger.start()

    def send(self, topic, value, key=None):
        batch = None
        with self._lock:
            buf = self._buffers.setdefault(topic, [])
            buf.append({"key": key, "value": value})
            if len(buf) >= self.batch_size:
                batch = self._buffers.pop(topic)
        if batch:
            self._post(topic, batch)

    def flush(self):
        with self._lock:
            drained = self._buffers
            self._buffers = {}
        for topic, batch in drained.items():
            if batch:
                self._post(topic, batch)

    def close(self):
        self._closed.set()
        self._linger.join(timeout=2.0)
        self.flush()

    def _linger_loop(self, linger_s):
        while not self._closed.wait(linger_s):
            try:
                self.flush()
            except Exception as e:
                print(f"[producer] background flush failed: {e}", flush=True)

    def _post(self, topic, records):
        attempt = 0
        while True:
            try:
                return _request("POST", f"{self.url}/produce",
                                {"topic": topic, "records": records})
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                attempt += 1
                if self._closed.is_set() or attempt > self.max_retries:
                    raise
                time.sleep(self.retry_backoff_s)


class Consumer:
    """Group consumer: joins a group, polls its assigned partitions,
    heartbeats in the background, and commits offsets on request.

    Fault-tolerance behaviour, mirroring Kafka:
    - A background thread heartbeats every ``heartbeat_interval``; if the
      broker reports a rebalance (stale generation / unknown member), the
      consumer transparently rejoins and picks up its new assignment.
    - Positions resume from committed offsets on (re)join; uncommitted
      work is redelivered — at-least-once delivery.
    - Fetching an out-of-range offset resets per ``auto_offset_reset``
      ("earliest" or "latest").
    - Broker outages surface as empty polls, not exceptions; the consumer
      reconnects when the broker returns.
    """

    def __init__(self, broker_url, group, topics, consumer_id=None,
                 auto_offset_reset="earliest", heartbeat_interval=3.0,
                 fetch_max_records=500):
        assert auto_offset_reset in ("earliest", "latest")
        self.url = broker_url.rstrip("/")
        self.group = group
        self.topics = list(topics)
        self.consumer_id = consumer_id or f"consumer-{uuid.uuid4().hex[:8]}"
        self.auto_offset_reset = auto_offset_reset
        self.fetch_max_records = fetch_max_records
        self._lock = threading.Lock()
        self._generation = None
        self._assignment = []      # list of (topic, partition)
        self._positions = {}       # (topic, partition) -> next offset or None
        self._needs_rejoin = True
        self._closed = threading.Event()
        self._rr = 0
        try:
            self._join()
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass  # broker not reachable yet; poll() retries the join
        self._hb = threading.Thread(
            target=self._heartbeat_loop, args=(heartbeat_interval,),
            name=f"hb-{self.consumer_id}", daemon=True)
        self._hb.start()

    # -- group membership ----------------------------------------------------

    @property
    def assignment(self):
        with self._lock:
            return list(self._assignment)

    @property
    def generation(self):
        with self._lock:
            return self._generation

    def _join(self):
        resp = _request("POST", f"{self.url}/groups/join", {
            "group": self.group,
            "consumer_id": self.consumer_id,
            "topics": self.topics,
        })
        with self._lock:
            self._generation = resp["generation"]
            self._assignment = [(t, p) for t, p in resp["assignment"]]
            committed = resp.get("offsets", {})
            self._positions = {}
            for t, p in self._assignment:
                off = committed.get(t, {}).get(str(p))
                self._positions[(t, p)] = off  # None -> resolve on first fetch
            self._needs_rejoin = False

    def _heartbeat_loop(self, interval):
        while not self._closed.wait(interval):
            with self._lock:
                if self._needs_rejoin or self._generation is None:
                    continue
                gen = self._generation
            try:
                _request("POST", f"{self.url}/groups/heartbeat", {
                    "group": self.group,
                    "consumer_id": self.consumer_id,
                    "generation": gen,
                }, timeout=10.0)
            except ClientError as e:
                if e.code in ("stale_generation", "unknown_member"):
                    with self._lock:
                        self._needs_rejoin = True
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                pass  # broker unreachable; poll() copes and we retry next tick

    # -- consuming -------------------------------------------------------------

    def poll(self, timeout=1.0, max_records=500):
        """Fetch available records across assigned partitions.

        Long-polls the broker for up to ``timeout`` seconds when nothing
        is immediately available. Returns a list of record dicts:
        {"topic", "partition", "offset", "key", "value", "ts"}.
        """
        try:
            with self._lock:
                needs = self._needs_rejoin
            if needs:
                self._join()
            assignment = self.assignment
            if not assignment:
                if timeout > 0:
                    time.sleep(min(timeout, 0.5))
                return []
            out = []
            n = len(assignment)
            start = self._rr % n
            self._rr += 1
            order = assignment[start:] + assignment[:start]
            # Fast pass: grab whatever is ready on each partition.
            for tp in order:
                if len(out) >= max_records:
                    break
                out.extend(self._fetch_one(tp, max_records - len(out), 0))
            # Nothing ready anywhere: long-poll one partition.
            if not out and timeout > 0:
                out.extend(self._fetch_one(
                    order[0], max_records, int(timeout * 1000)))
            return out
        except ClientError as e:
            if e.code in ("stale_generation", "unknown_member"):
                with self._lock:
                    self._needs_rejoin = True
                return []
            raise
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(min(timeout, 0.5) if timeout > 0 else 0.1)
            return []

    def _fetch_one(self, tp, max_records, wait_ms):
        topic, partition = tp
        with self._lock:
            offset = self._positions.get(tp)
        if offset is None:
            offset = self._resolve_initial_offset(tp)
        try:
            resp = _request(
                "GET",
                f"{self.url}/fetch?topic={topic}&partition={partition}"
                f"&offset={offset}&max_records={max_records}"
                f"&wait_ms={wait_ms}",
                timeout=max(30.0, wait_ms / 1000.0 + 10.0))
        except ClientError as e:
            if e.code == "offset_out_of_range":
                reset = (e.payload["earliest"]
                         if self.auto_offset_reset == "earliest"
                         else e.payload["latest"])
                with self._lock:
                    self._positions[tp] = reset
                return []
            raise
        records = resp["records"]
        if records:
            with self._lock:
                self._positions[tp] = resp["next_offset"]
        for r in records:
            r["topic"] = topic
            r["partition"] = partition
        return records

    def _resolve_initial_offset(self, tp):
        topic, partition = tp
        resp = _request(
            "GET",
            f"{self.url}/fetch?topic={topic}&partition={partition}"
            f"&offset=0&max_records=0")
        offset = (resp["start_offset"]
                  if self.auto_offset_reset == "earliest"
                  else resp["end_offset"])
        with self._lock:
            self._positions[tp] = offset
        return offset

    # -- offsets -----------------------------------------------------------------

    def positions(self):
        with self._lock:
            return dict(self._positions)

    def commit(self):
        """Commit current positions (everything poll() has returned).

        Returns True on success; False if the group rebalanced underneath
        us (our claim on those partitions is gone — records will be
        redelivered to their new owner).
        """
        with self._lock:
            if self._needs_rejoin or self._generation is None:
                return False
            gen = self._generation
            offsets = {}
            for (t, p), off in self._positions.items():
                if off is not None:
                    offsets.setdefault(t, {})[str(p)] = off
        if not offsets:
            return True
        try:
            _request("POST", f"{self.url}/groups/commit", {
                "group": self.group,
                "consumer_id": self.consumer_id,
                "generation": gen,
                "offsets": offsets,
            })
            return True
        except ClientError as e:
            if e.code in ("stale_generation", "unknown_member"):
                with self._lock:
                    self._needs_rejoin = True
                return False
            raise

    def close(self, leave_group=True):
        self._closed.set()
        self._hb.join(timeout=2.0)
        if leave_group:
            try:
                _request("POST", f"{self.url}/groups/leave", {
                    "group": self.group, "consumer_id": self.consumer_id,
                }, timeout=5.0)
            except Exception:
                pass
