"""The broker: topics made of partition logs, plus the group coordinator."""

import hashlib
import os
import json
import re
import threading
import time

from .coordinator import GroupCoordinator
from .errors import (
    BadRequest, PartitionNotFound, TopicAlreadyExists, TopicNotFound)
from .log import PartitionLog

TOPICS_FILE = "topics.json"
_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,255}$")


class Broker:
    """Owns topics/partitions and routes produce/fetch requests.

    With ``data_dir`` set, partition logs are file-backed and topic
    metadata plus committed group offsets are persisted, so everything
    survives a restart. Without it the broker is purely in-memory and
    ``retention_max_records`` bounds each partition.
    """

    def __init__(self, data_dir=None, retention_max_records=None,
                 session_timeout=10.0, fsync=False):
        self.data_dir = data_dir
        self.retention_max_records = retention_max_records
        self.fsync = fsync
        self.started_at = time.time()
        self._lock = threading.RLock()
        self._topics = {}   # name -> list[PartitionLog]
        self._rr = {}       # name -> round-robin counter for keyless records
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
            self._load_topics()
        self.coordinator = GroupCoordinator(
            self, data_dir=data_dir, session_timeout=session_timeout)

    # -- topic management ----------------------------------------------------

    def _log_path(self, topic, partition):
        return os.path.join(self.data_dir, f"{topic}-{partition}.jsonl")

    def _load_topics(self):
        path = os.path.join(self.data_dir, TOPICS_FILE)
        if not os.path.exists(path):
            return
        with open(path) as f:
            meta = json.load(f)
        for name, n in meta.get("topics", {}).items():
            self._topics[name] = [
                PartitionLog(self._log_path(name, p), fsync=self.fsync)
                for p in range(n)
            ]

    def _save_topics_locked(self):
        if not self.data_dir:
            return
        path = os.path.join(self.data_dir, TOPICS_FILE)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(
                {"topics": {t: len(ps) for t, ps in self._topics.items()}}, f)
        os.replace(tmp, path)

    def create_topic(self, name, partitions=1):
        if not isinstance(name, str) or not _TOPIC_RE.match(name):
            raise BadRequest(f"invalid topic name: {name!r}")
        if not isinstance(partitions, int) or partitions < 1:
            raise BadRequest("partitions must be a positive integer")
        with self._lock:
            if name in self._topics:
                raise TopicAlreadyExists(
                    f"topic {name!r} already exists",
                    partitions=len(self._topics[name]))
            if self.data_dir:
                logs = [PartitionLog(self._log_path(name, p), fsync=self.fsync)
                        for p in range(partitions)]
            else:
                logs = [PartitionLog(
                    retention_max_records=self.retention_max_records)
                    for _ in range(partitions)]
            self._topics[name] = logs
            self._save_topics_locked()
        return {"name": name, "partitions": partitions}

    def topics(self):
        with self._lock:
            return {t: len(ps) for t, ps in self._topics.items()}

    def partition_count(self, topic):
        with self._lock:
            logs = self._topics.get(topic)
            return len(logs) if logs else 0

    def _partitions(self, topic):
        with self._lock:
            logs = self._topics.get(topic)
            if logs is None:
                raise TopicNotFound(f"topic {topic!r} does not exist")
            return logs

    def _log(self, topic, partition):
        logs = self._partitions(topic)
        if not isinstance(partition, int) or not 0 <= partition < len(logs):
            raise PartitionNotFound(
                f"partition {partition} of topic {topic!r} does not exist",
                partitions=len(logs))
        return logs[partition]

    # -- produce ---------------------------------------------------------------

    def _partition_for(self, topic, entry, n):
        key = entry.get("key")
        if key is not None:
            # md5, not hash(): stable across processes and restarts, so a
            # key always lands on the same partition (ordering per key).
            digest = hashlib.md5(str(key).encode()).digest()
            return int.from_bytes(digest[:8], "big") % n
        with self._lock:
            self._rr[topic] = self._rr.get(topic, -1) + 1
            return self._rr[topic] % n

    def produce(self, topic, entries, partition=None):
        """entries: list of {"key": ..., "value": ...}. Returns per-partition
        append results: [{"partition": p, "base_offset": o, "count": n}]."""
        logs = self._partitions(topic)
        if not isinstance(entries, list) or not entries:
            raise BadRequest("records must be a non-empty list")
        by_partition = {}
        for e in entries:
            if not isinstance(e, dict):
                raise BadRequest("each record must be an object")
            if partition is not None:
                p = partition
            else:
                p = self._partition_for(topic, e, len(logs))
            if not isinstance(p, int) or not 0 <= p < len(logs):
                raise PartitionNotFound(
                    f"partition {p} of topic {topic!r} does not exist",
                    partitions=len(logs))
            by_partition.setdefault(p, []).append(e)
        results = []
        for p in sorted(by_partition):
            base = logs[p].append(by_partition[p])
            results.append({
                "partition": p,
                "base_offset": base,
                "count": len(by_partition[p]),
            })
        return results

    # -- fetch -----------------------------------------------------------------

    def fetch(self, topic, partition, offset, max_records=500, wait_ms=0):
        log = self._log(topic, partition)
        records = log.read(offset, max_records)
        if not records and max_records > 0 and wait_ms > 0:
            log.wait_for_data(offset, wait_ms / 1000.0)
            records = log.read(offset, max_records)
        return {
            "records": records,
            "next_offset": records[-1]["offset"] + 1 if records else offset,
            "start_offset": log.start_offset,
            "end_offset": log.end_offset,
        }

    # -- metrics -----------------------------------------------------------------

    def metrics(self):
        with self._lock:
            topics = {
                name: {
                    str(p): {"start": log.start_offset, "end": log.end_offset}
                    for p, log in enumerate(logs)
                }
                for name, logs in self._topics.items()
            }
        groups = self.coordinator.describe()
        for g in groups.values():
            lag = {}
            for topic, parts in topics.items():
                committed = g["offsets"].get(topic, {})
                for p, rng in parts.items():
                    have = committed.get(p, rng["start"])
                    lag.setdefault(topic, {})[p] = max(rng["end"] - have, 0)
            g["lag"] = lag
        return {
            "ts": time.time(),
            "uptime_s": round(time.time() - self.started_at, 1),
            "persistent": bool(self.data_dir),
            "topics": topics,
            "groups": groups,
        }

    def close(self):
        self.coordinator.close()
        with self._lock:
            for logs in self._topics.values():
                for log in logs:
                    log.close()
