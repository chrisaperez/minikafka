"""Consumer-group coordinator: membership, rebalancing, committed offsets.

Mirrors the shape of Kafka's group protocol, simplified:

- Consumers join a group; every join, leave, or session timeout bumps the
  group *generation* and recomputes a round-robin assignment of all
  subscribed partitions across the sorted member list.
- Members must heartbeat within ``session_timeout`` seconds or they are
  evicted and their partitions reassigned (fault tolerance).
- Commits and heartbeats carry the generation; a stale generation is
  rejected, which both tells live consumers to rejoin and fences zombies
  from committing offsets for partitions they no longer own.
- Committed offsets are persisted (when the broker has a data dir) so
  progress survives broker restarts; membership is ephemeral by design —
  consumers simply rejoin.
"""

import json
import os
import threading
import time

from .errors import BadRequest, StaleGeneration, UnknownMember

OFFSETS_FILE = "__consumer_offsets.json"


class _Group:
    def __init__(self, name):
        self.name = name
        self.generation = 0
        self.members = {}         # consumer_id -> last heartbeat (monotonic)
        self.subscriptions = {}   # consumer_id -> sorted list of topic names
        self.assignment = {}      # consumer_id -> list of [topic, partition]
        self.offsets = {}         # (topic, partition) -> next offset to read
        self.topic_snapshot = {}  # topic -> partition count at last rebalance


class GroupCoordinator:
    def __init__(self, broker, data_dir=None, session_timeout=10.0):
        self._broker = broker
        self._lock = threading.RLock()
        self._groups = {}
        self.session_timeout = session_timeout
        self._offsets_path = (
            os.path.join(data_dir, OFFSETS_FILE) if data_dir else None)
        self._reaper = None
        self._closed = threading.Event()
        if self._offsets_path and os.path.exists(self._offsets_path):
            self._load_offsets()

    # -- persistence ---------------------------------------------------------

    def _load_offsets(self):
        with open(self._offsets_path) as f:
            data = json.load(f)
        for group_name, topics in data.get("groups", {}).items():
            g = self._groups.setdefault(group_name, _Group(group_name))
            for topic, parts in topics.items():
                for p, off in parts.items():
                    g.offsets[(topic, int(p))] = off

    def _save_offsets_locked(self):
        if not self._offsets_path:
            return
        data = {"groups": {}}
        for name, g in self._groups.items():
            topics = {}
            for (topic, p), off in g.offsets.items():
                topics.setdefault(topic, {})[str(p)] = off
            data["groups"][name] = topics
        tmp = self._offsets_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self._offsets_path)

    # -- group protocol --------------------------------------------------------

    def join(self, group_name, consumer_id, topics):
        if not group_name or not consumer_id:
            raise BadRequest("group and consumer_id are required")
        with self._lock:
            g = self._groups.setdefault(group_name, _Group(group_name))
            g.members[consumer_id] = time.monotonic()
            g.subscriptions[consumer_id] = sorted(set(topics or []))
            self._expire_locked(g)
            self._rebalance_locked(g)
            return self._join_response_locked(g, consumer_id)

    def heartbeat(self, group_name, consumer_id, generation):
        with self._lock:
            g = self._require_member_locked(group_name, consumer_id)
            g.members[consumer_id] = time.monotonic()
            self._expire_locked(g)
            self._check_topics_locked(g)
            if generation != g.generation:
                raise StaleGeneration(
                    "group rebalanced; rejoin", generation=g.generation)
            return {"generation": g.generation}

    def leave(self, group_name, consumer_id):
        with self._lock:
            g = self._groups.get(group_name)
            if g is None or consumer_id not in g.members:
                return {"ok": True}
            del g.members[consumer_id]
            g.subscriptions.pop(consumer_id, None)
            self._rebalance_locked(g)
            return {"ok": True}

    def commit(self, group_name, consumer_id, generation, offsets):
        """offsets: {topic: {str(partition): next_offset}}"""
        with self._lock:
            g = self._require_member_locked(group_name, consumer_id)
            if generation != g.generation:
                raise StaleGeneration(
                    "stale generation; commit rejected",
                    generation=g.generation)
            owned = set(map(tuple, g.assignment.get(consumer_id, [])))
            for topic, parts in offsets.items():
                for p, off in parts.items():
                    tp = (topic, int(p))
                    if tp not in owned:
                        raise StaleGeneration(
                            f"partition {topic}[{p}] not assigned to "
                            f"{consumer_id}", generation=g.generation)
                    g.offsets[tp] = off
            self._save_offsets_locked()
            return {"ok": True}

    def committed(self, group_name):
        with self._lock:
            g = self._groups.get(group_name)
            if g is None:
                return {}
            out = {}
            for (topic, p), off in g.offsets.items():
                out.setdefault(topic, {})[str(p)] = off
            return out

    # -- introspection -----------------------------------------------------

    def describe(self):
        with self._lock:
            now = time.monotonic()
            out = {}
            for name, g in self._groups.items():
                out[name] = {
                    "generation": g.generation,
                    "members": [
                        {
                            "id": m,
                            "heartbeat_age": round(now - ts, 3),
                            "assignment": list(g.assignment.get(m, [])),
                        }
                        for m, ts in sorted(g.members.items())
                    ],
                    "offsets": self.committed(name),
                }
            return out

    # -- internals ---------------------------------------------------------

    def _require_member_locked(self, group_name, consumer_id):
        g = self._groups.get(group_name)
        if g is None or consumer_id not in g.members:
            raise UnknownMember(
                f"{consumer_id} is not a member of group "
                f"{group_name!r}; rejoin")
        return g

    def _expire_locked(self, g):
        now = time.monotonic()
        dead = [m for m, ts in g.members.items()
                if now - ts > self.session_timeout]
        for m in dead:
            del g.members[m]
            g.subscriptions.pop(m, None)
        if dead:
            self._rebalance_locked(g)

    def _check_topics_locked(self, g):
        """Rebalance if subscribed topics gained partitions or appeared."""
        if self._topic_snapshot_locked(g) != g.topic_snapshot:
            self._rebalance_locked(g)

    def _topic_snapshot_locked(self, g):
        snapshot = {}
        for topics in g.subscriptions.values():
            for t in topics:
                n = self._broker.partition_count(t)
                if n:
                    snapshot[t] = n
        return snapshot

    def _rebalance_locked(self, g):
        snapshot = self._topic_snapshot_locked(g)
        partitions = [
            [t, p] for t in sorted(snapshot) for p in range(snapshot[t])
        ]
        members = sorted(g.members)
        assignment = {m: [] for m in members}
        for i, tp in enumerate(partitions):
            if members:
                assignment[members[i % len(members)]].append(tp)
        # Only bump the generation when ownership actually changes. A
        # no-op bump (e.g. a member rejoining with an identical result)
        # would invalidate every other member's generation and trigger a
        # storm of rejoins, each bump begetting the next.
        if assignment == g.assignment and snapshot == g.topic_snapshot:
            return
        g.generation += 1
        g.topic_snapshot = snapshot
        g.assignment = assignment

    def _join_response_locked(self, g, consumer_id):
        assignment = g.assignment.get(consumer_id, [])
        offsets = {}
        for topic, p in assignment:
            off = g.offsets.get((topic, p))
            if off is not None:
                offsets.setdefault(topic, {})[str(p)] = off
        return {
            "generation": g.generation,
            "assignment": assignment,
            "offsets": offsets,
        }

    # -- background reaper ---------------------------------------------------

    def start_reaper(self, interval=1.0):
        """Evict timed-out members even when nobody is calling in."""
        if self._reaper is not None:
            return

        def loop():
            while not self._closed.wait(interval):
                with self._lock:
                    for g in self._groups.values():
                        self._expire_locked(g)
                        self._check_topics_locked(g)

        self._reaper = threading.Thread(
            target=loop, name="group-reaper", daemon=True)
        self._reaper.start()

    def close(self):
        self._closed.set()
