"""The broker: topics made of partition logs, plus the group coordinator and replication."""

import hashlib
import os
import json
import re
import threading
import time
import urllib.request
import urllib.error

from .coordinator import GroupCoordinator
from .errors import (
    BadRequest, PartitionNotFound, TopicAlreadyExists, TopicNotFound, NotLeaderForPartition, BrokerError)
from .log import PartitionLog

_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,255}$")

class Broker:
    """Owns topics/partitions and routes produce/fetch requests.
    Integrates with Zookeeper for cluster metadata.
    """

    def __init__(self, broker_id, host, port, zk_url=None, data_dir=None, retention_max_records=None,
                 session_timeout=10.0, fsync=False, segment_bytes=256*1024*1024):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        if zk_url and not zk_url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid zk_url scheme: {zk_url}")
        self.zk_url = zk_url
        self.data_dir = data_dir
        self.retention_max_records = retention_max_records
        self.fsync = fsync
        self.segment_bytes = segment_bytes
        self.started_at = time.time()
        
        self._lock = threading.RLock()
        self._topics = {}   # name -> list of { "log": PartitionLog or None, "leader": id, "replicas": [], "isr": set() }
        self._rr = {}       # name -> round-robin counter for keyless records
        
        # Follower states
        self._follower_threads = {} # (topic, p) -> thread
        self._follower_stop = threading.Event()
        
        # Leader ISR tracking: (topic, p) -> { replica_id: last_fetch_time }
        self._replica_fetch_times = {}

        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
            
        self.coordinator = GroupCoordinator(
            self, data_dir=data_dir, session_timeout=session_timeout)

        # Start ZK background sync
        if self.zk_url:
            self._zk_thread = threading.Thread(target=self._zk_loop, daemon=True)
            self._zk_thread.start()

    # -- zookeeper integration -----------------------------------------------

    def _zk_request(self, method, path, body=None):
        url = f"{self.zk_url}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=2.0) as res:  # nosec B310
                return json.loads(res.read())
        except Exception:
            return None

    def _zk_loop(self):
        while not self._follower_stop.is_set():
            # 1. Heartbeat to ZK
            self._zk_request("POST", "/brokers", {
                "id": self.broker_id, "host": self.host, "port": self.port
            })
            
            # 2. Sync Topics
            topics_meta = self._zk_request("GET", "/topics")
            if topics_meta is not None:
                self._sync_topics(topics_meta)
            
            time.sleep(3.0)

    def _sync_topics(self, topics_meta):
        with self._lock:
            # Add or update topics
            for name, partitions in topics_meta.items():
                # Names become on-disk paths; never trust the registry blindly.
                if not isinstance(name, str) or not _TOPIC_RE.match(name):
                    continue
                if name not in self._topics:
                    self._topics[name] = [None] * len(partitions)
                
                for p_meta in partitions:
                    p = p_meta["partition"]
                    leader = p_meta["leader"]
                    replicas = p_meta["replicas"]
                    
                    if p >= len(self._topics[name]):
                        self._topics[name].append(None)
                        
                    current = self._topics[name][p]
                    if current is None:
                        # Need to instantiate this partition locally if we are a replica
                        is_replica = self.broker_id in replicas
                        log = None
                        if is_replica:
                            log_path = os.path.join(self.data_dir, f"{name}-{p}") if self.data_dir else None
                            log = PartitionLog(path=log_path, fsync=self.fsync, segment_bytes=self.segment_bytes)
                        current = {
                            "log": log,
                            "leader": leader,
                            "replicas": replicas,
                            "isr": set(p_meta["isr"])
                        }
                        self._topics[name][p] = current
                    else:
                        # Update leadership/replica info
                        current["leader"] = leader
                        current["replicas"] = replicas
                        # We only overwrite ISR from ZK if we are NOT the leader. If we are the leader, we track it locally.
                        if leader != self.broker_id:
                            current["isr"] = set(p_meta["isr"])
                    
                    # Manage follower threads
                    if current["log"] is not None:
                        is_leader = (leader == self.broker_id)
                        tp = (name, p)
                        if is_leader:
                            # Stop follower thread if it exists
                            if tp in self._follower_threads:
                                self._follower_threads[tp]["stop"] = True
                                del self._follower_threads[tp]
                        else:
                            # We are a follower, ensure thread is running
                            if tp not in self._follower_threads:
                                stop_flag = {"stop": False}
                                t = threading.Thread(target=self._follower_loop, args=(name, p, stop_flag), daemon=True)
                                self._follower_threads[tp] = stop_flag
                                t.start()

            # Handle deleted topics
            # (If a topic is in self._topics but not in topics_meta, it was deleted)
            for name in list(self._topics.keys()):
                if name not in topics_meta:
                    self._local_delete_topic(name)

    # -- follower replication ------------------------------------------------

    def _follower_loop(self, topic, partition, stop_flag):
        while not stop_flag["stop"] and not self._follower_stop.is_set():
            time.sleep(0.5)
            with self._lock:
                if topic not in self._topics or partition >= len(self._topics[topic]):
                    break
                p_info = self._topics[topic][partition]
                if not p_info or not p_info["log"]:
                    continue
                leader_id = p_info["leader"]
                if leader_id == self.broker_id:
                    break # Became leader
                log = p_info["log"]
                offset = log.end_offset
            
            # Find leader host/port
            brokers = self._zk_request("GET", "/brokers")
            if not brokers or leader_id not in brokers:
                continue
            
            leader_info = brokers[leader_id]
            url = f"http://{leader_info['host']}:{leader_info['port']}/fetch?topic={topic}&partition={partition}&offset={offset}&max_records=500&wait_ms=500&replica_id={self.broker_id}"
            
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=2.0) as res:  # nosec B310
                    data = json.loads(res.read())
                    records = data.get("records", [])
                    if records:
                        # Append to local log
                        # Note: we need to append preserving exact offsets and timestamps, 
                        # but our log.append assigns offsets. We should technically bypass it 
                        # or have a raw append for followers.
                        # For simplicity, we just use the normal append since the leader sends them in order,
                        # but this assumes exact offset match.
                        log.append(records)
            except Exception as e:
                print(f"[Follower {self.broker_id}] fetch loop error for {topic}-{partition}: {e}")
                time.sleep(1.0)


    # -- topic management ----------------------------------------------------

    def create_topic(self, name, partitions=1, replication_factor=1):
        if not isinstance(name, str) or not _TOPIC_RE.match(name):
            raise BadRequest(f"invalid topic name: {name!r}")
        if not isinstance(partitions, int) or isinstance(partitions, bool) or partitions < 1:
            raise BadRequest(f"partitions must be a positive integer, got {partitions!r}")
        if self.zk_url:
            res = self._zk_request("POST", "/topics", {
                "name": name, "partitions": partitions, "replication_factor": replication_factor
            })
            if not res or "error" in res:
                err = BrokerError(res.get("error", "zk error") if res else "zk error")
                err.status = 400
                raise err
            return res
        else:
            # Standalone mode
            with self._lock:
                if name in self._topics:
                    raise TopicAlreadyExists(f"topic {name!r} already exists", partitions=len(self._topics[name]))
                self._topics[name] = []
                for p in range(partitions):
                    log_path = os.path.join(self.data_dir, f"{name}-{p}") if self.data_dir else None
                    log = PartitionLog(path=log_path, fsync=self.fsync, segment_bytes=self.segment_bytes)
                    self._topics[name].append({
                        "log": log, "leader": self.broker_id, "replicas": [self.broker_id], "isr": {self.broker_id}
                    })
            return {"name": name, "partitions": partitions}

    def delete_topic(self, name):
        if self.zk_url:
            res = self._zk_request("DELETE", f"/topics?name={name}")
            if not res or "error" in res:
                err = BrokerError(res.get("error", "zk error") if res else "zk error")
                err.status = 404
                raise err
        else:
            self._local_delete_topic(name)
        return {"ok": True}

    def _local_delete_topic(self, name):
        with self._lock:
            if name in self._topics:
                # Close and delete logs
                for p_info in self._topics[name]:
                    if p_info and p_info["log"]:
                        p_info["log"].delete()
                del self._topics[name]
                
                # Clean up offsets
                for group in self.coordinator._groups.values():
                    keys_to_delete = [k for k in group.offsets.keys() if k[0] == name]
                    for k in keys_to_delete:
                        del group.offsets[k]
                self.coordinator._save_offsets_locked()

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

    def _get_partition_info(self, topic, partition):
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
            digest = hashlib.sha256(str(key).encode()).digest()
            return int.from_bytes(digest[:8], "big") % n
        with self._lock:
            self._rr[topic] = self._rr.get(topic, -1) + 1
            return self._rr[topic] % n

    def produce(self, topic, entries, partition=None):
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
            p_info = logs[p]
            if p_info["leader"] != self.broker_id:
                raise NotLeaderForPartition(f"Not leader for partition {p}")
            if p_info["log"] is None:
                err = BrokerError("Log not initialized")
                err.status = 500
                raise err
            
            base = p_info["log"].append(by_partition[p])
            results.append({
                "partition": p,
                "base_offset": base,
                "count": len(by_partition[p]),
            })
        return results

    # -- fetch -----------------------------------------------------------------

    def fetch(self, topic, partition, offset, max_records=500, wait_ms=0, replica_id=None):
        p_info = self._get_partition_info(topic, partition)
        if p_info["leader"] != self.broker_id:
            # Could serve from follower, but typically clients fetch from leader.
            # Replicas MUST fetch from leader.
            raise NotLeaderForPartition(f"Not leader for partition {partition}")
        
        log = p_info["log"]
        if log is None:
            err = BrokerError("Log not initialized")
            err.status = 500
            raise err

        # Update ISR tracking if this is a replica fetch
        if replica_id is not None:
            with self._lock:
                tp = (topic, partition)
                if tp not in self._replica_fetch_times:
                    self._replica_fetch_times[tp] = {}
                self._replica_fetch_times[tp][replica_id] = time.time()
                
                # Check for ISR drops and adds
                now = time.time()
                changed = False
                for rid, last in list(self._replica_fetch_times[tp].items()):
                    if now - last > 10.0:
                        if rid in p_info["isr"]:
                            p_info["isr"].remove(rid)
                            changed = True
                    else:
                        # Replica is fetching. Is it caught up?
                        # Simplification: if it's fetching near the end, we consider it in-sync
                        # Kafka uses lag or time. We just use "has fetched recently" for this toy.
                        if rid not in p_info["isr"]:
                            p_info["isr"].add(rid)
                            changed = True
                
                if changed and self.zk_url:
                    # Async push to ZK
                    threading.Thread(target=self._zk_request, args=("POST", "/topics/isr", {
                        "topic": topic, "partition": partition, "isr": list(p_info["isr"])
                    }), daemon=True).start()

        records = log.read(offset, max_records)
        if not records and max_records > 0 and wait_ms > 0:
            log.wait_for_data(offset, wait_ms / 1000.0)
            records = log.read(offset, max_records)
            
        return {
            "records": records,
            "next_offset": records[-1]["offset"] + 1 if records else offset,
            "start_offset": log.start_offset,
            "end_offset": log.end_offset,
            "isr": list(p_info["isr"])
        }

    # -- compaction ------------------------------------------------------------
    
    def compact(self, topic, partition):
        p_info = self._get_partition_info(topic, partition)
        if p_info["log"]:
            p_info["log"].compact()
        return {"ok": True}

    # -- metrics -----------------------------------------------------------------

    def metrics(self):
        with self._lock:
            topics = {
                name: {
                    str(p): {"start": p_info["log"].start_offset, "end": p_info["log"].end_offset, "isr": list(p_info["isr"])}
                    for p, p_info in enumerate(partitions) if p_info and p_info["log"]
                }
                for name, partitions in self._topics.items()
            }
        groups = self.coordinator.describe()
        return {
            "ts": time.time(),
            "uptime_s": round(time.time() - self.started_at, 1),
            "persistent": bool(self.data_dir),
            "broker_id": self.broker_id,
            "topics": topics,
            "groups": groups,
        }

    def close(self):
        self._follower_stop.set()
        for t in self._follower_threads.values():
            t["stop"] = True
        self.coordinator.close()
        with self._lock:
            for partitions in self._topics.values():
                for p_info in partitions:
                    if p_info and p_info["log"]:
                        p_info["log"].close()

# Avoid circular/undefined error imports by making sure we export them from errors.py
# If NotLeaderForPartition doesn't exist, we'll create it later.
