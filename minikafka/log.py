"""Append-only partition log with optional file persistence."""

import json
import os
import threading
import time

from .errors import OffsetOutOfRange


class PartitionLog:
    """A single partition: an ordered, append-only sequence of records.

    Offsets are dense integers starting at 0. Given a ``path`` the log is
    backed by a JSONL file and survives restarts (an in-memory index of
    byte positions is rebuilt by scanning the file on startup, and a torn
    final record from an unclean shutdown is truncated away). Without a
    path the log lives in memory and can be bounded with
    ``retention_max_records``: oldest records are dropped and the start
    offset advances, like Kafka's retention.
    """

    def __init__(self, path=None, retention_max_records=None, fsync=False):
        self._cond = threading.Condition()
        self.path = path
        self.retention_max_records = retention_max_records
        self.fsync = fsync
        self._records = []      # memory mode: the records themselves
        self._positions = []    # file mode: byte position of each record
        self._size = 0          # file mode: logical file size (post-recovery)
        self._start_offset = 0
        self._file = None
        if path:
            self._recover()

    # -- recovery ----------------------------------------------------------

    def _recover(self):
        good_end = 0
        if os.path.exists(self.path):
            with open(self.path, "rb") as f:
                pos = 0
                for line in f:
                    if line.endswith(b"\n"):
                        self._positions.append(pos)
                        good_end = pos + len(line)
                    pos += len(line)
            if good_end < os.path.getsize(self.path):
                with open(self.path, "r+b") as f:
                    f.truncate(good_end)
        self._size = good_end
        self._file = open(self.path, "ab")

    # -- offsets -----------------------------------------------------------

    @property
    def start_offset(self):
        with self._cond:
            return self._start_offset

    @property
    def end_offset(self):
        """The offset the next appended record will receive."""
        with self._cond:
            return self._end_offset_locked()

    def _end_offset_locked(self):
        if self.path is not None:
            return len(self._positions)  # file mode never truncates the front
        return self._start_offset + len(self._records)

    # -- append ------------------------------------------------------------

    def append(self, entries):
        """Append entries (dicts with optional "key" and "value").

        Returns the base offset assigned to the first entry.
        """
        now = time.time()
        with self._cond:
            base = self._end_offset_locked()
            records = [
                {"offset": base + i, "ts": now,
                 "key": e.get("key"), "value": e.get("value")}
                for i, e in enumerate(entries)
            ]
            if self.path is not None:
                buf = bytearray()
                for rec in records:
                    line = json.dumps(rec, separators=(",", ":")).encode() + b"\n"
                    self._positions.append(self._size + len(buf))
                    buf += line
                self._file.write(buf)
                self._file.flush()
                if self.fsync:
                    os.fsync(self._file.fileno())
                self._size += len(buf)
            else:
                self._records.extend(records)
                cap = self.retention_max_records
                if cap is not None and len(self._records) > cap:
                    drop = len(self._records) - cap
                    del self._records[:drop]
                    self._start_offset += drop
            self._cond.notify_all()
            return base

    # -- read --------------------------------------------------------------

    def read(self, offset, max_records=500):
        """Read up to max_records starting at offset.

        Returns [] when offset == end_offset (caught up). Raises
        OffsetOutOfRange when offset is outside [start, end], carrying
        earliest/latest so the caller can reset. max_records <= 0 is a
        metadata probe: always returns [] without a range check.
        """
        with self._cond:
            start = self._start_offset
            end = self._end_offset_locked()
            if max_records <= 0:
                return []
            if offset < start or offset > end:
                raise OffsetOutOfRange(
                    f"offset {offset} not in [{start}, {end}]",
                    earliest=start, latest=end)
            if offset == end:
                return []
            n = min(max_records, end - offset)
            if self.path is None:
                i = offset - start
                return list(self._records[i:i + n])
            first = self._positions[offset]
            last_excl = self._positions[offset + n] if offset + n < end else self._size
        # File I/O happens outside the lock: bytes below the indexed end are
        # immutable, so concurrent appends cannot affect this range.
        with open(self.path, "rb") as f:
            f.seek(first)
            data = f.read(last_excl - first)
        return [json.loads(line) for line in data.splitlines()]

    def wait_for_data(self, offset, timeout):
        """Block until end_offset > offset or timeout (seconds) elapses."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._end_offset_locked() <= offset:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return True

    def close(self):
        with self._cond:
            if self._file is not None:
                self._file.close()
                self._file = None
