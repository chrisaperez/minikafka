"""Append-only partition log with optional file persistence, segment rolling, and compaction."""

import bisect
import json
import os
import threading
import time
import glob

from .errors import OffsetOutOfRange


class Segment:
    """A single log segment backed by a JSONL file.

    Records are addressed by their *logical* offset (parsed from each record),
    not by their position in the file. This lets a segment hold a sparse set of
    offsets after compaction has removed superseded keys.
    """
    def __init__(self, path, base_offset, fsync=False):
        self.path = path
        self.base_offset = base_offset
        self.fsync = fsync
        self.positions = []   # byte offset of each record line
        self.offsets = []     # logical offset of each record line (parallel)
        self.size = 0
        self.file = None
        self._recover()

    def _recover(self):
        good_end = 0
        if os.path.exists(self.path):
            with open(self.path, "rb") as f:
                pos = 0
                for line in f:
                    if line.endswith(b"\n"):
                        # A complete line that doesn't parse (or lacks an
                        # offset) marks corruption; stop and truncate here.
                        try:
                            off = json.loads(line)["offset"]
                        except (ValueError, KeyError, TypeError):
                            break
                        self.positions.append(pos)
                        self.offsets.append(off)
                        good_end = pos + len(line)
                    pos += len(line)
            if good_end < os.path.getsize(self.path):
                with open(self.path, "r+b") as f:
                    f.truncate(good_end)
        self.size = good_end
        self.file = open(self.path, "ab")

    @property
    def end_offset(self):
        return self.offsets[-1] + 1 if self.offsets else self.base_offset

    def append(self, records):
        """Appends pre-formatted record dicts and returns the number of bytes written."""
        buf = bytearray()
        for rec in records:
            line = json.dumps(rec, separators=(",", ":")).encode() + b"\n"
            self.positions.append(self.size + len(buf))
            self.offsets.append(rec["offset"])
            buf += line
        self.file.write(buf)
        self.file.flush()
        if self.fsync:
            os.fsync(self.file.fileno())
        self.size += len(buf)
        return len(buf)

    def read(self, offset, max_records):
        """Reads up to max_records records whose logical offset >= the given
        offset, within this segment. Handles sparse (post-compaction) offsets."""
        if max_records <= 0 or not self.offsets:
            return []

        # First record at or after the requested offset.
        idx = bisect.bisect_left(self.offsets, offset)
        n = min(max_records, len(self.positions) - idx)
        if n <= 0:
            return []

        first = self.positions[idx]
        last_excl = self.positions[idx + n] if idx + n < len(self.positions) else self.size

        with open(self.path, "rb") as f:
            f.seek(first)
            data = f.read(last_excl - first)

        return [json.loads(line) for line in data.splitlines() if line]

    def close(self):
        if self.file is not None:
            self.file.close()
            self.file = None


class PartitionLog:
    """A single partition: an ordered, append-only sequence of records.
    
    If `path` is provided (should be a directory), the log is backed by a series of
    segment files (`0000000000.jsonl`). Rolls to a new segment when the active segment
    exceeds `segment_bytes`.
    """

    def __init__(self, path=None, retention_max_records=None, fsync=False, segment_bytes=256*1024*1024):
        self._cond = threading.Condition()
        self.path = path
        self.retention_max_records = retention_max_records
        self.fsync = fsync
        self.segment_bytes = segment_bytes
        
        self._records = []      # memory mode: the records themselves
        self._segments = []     # file mode: list of Segment objects
        self._start_offset = 0
        
        if path:
            os.makedirs(path, exist_ok=True)
            self._recover()

    # -- recovery ----------------------------------------------------------

    def _recover(self):
        segment_files = glob.glob(os.path.join(self.path, "*.jsonl"))
        if not segment_files:
            # Create initial segment
            seg = Segment(os.path.join(self.path, "0000000000.jsonl"), 0, self.fsync)
            self._segments.append(seg)
            self._start_offset = 0
        else:
            # Load existing segments
            segment_files.sort()
            for sf in segment_files:
                basename = os.path.basename(sf)
                base_offset = int(basename.split(".")[0])
                seg = Segment(sf, base_offset, self.fsync)
                self._segments.append(seg)
            self._start_offset = self._segments[0].base_offset

    # -- offsets -----------------------------------------------------------

    @property
    def start_offset(self):
        with self._cond:
            return self._start_offset

    @property
    def end_offset(self):
        with self._cond:
            return self._end_offset_locked()

    def _end_offset_locked(self):
        if self.path is not None:
            return self._segments[-1].end_offset if self._segments else 0
        return self._start_offset + len(self._records)

    # -- segment management ------------------------------------------------

    def _roll_segment_if_needed(self):
        if not self.path or not self._segments:
            return
        active = self._segments[-1]
        if active.size >= self.segment_bytes:
            new_base = active.end_offset
            new_path = os.path.join(self.path, f"{new_base:010d}.jsonl")
            new_seg = Segment(new_path, new_base, self.fsync)
            self._segments.append(new_seg)

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
                self._roll_segment_if_needed()
                active = self._segments[-1]
                active.append(records)
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

    def _find_segment_index(self, offset):
        # Binary search could be used, but segment list is usually small
        for i in range(len(self._segments) - 1, -1, -1):
            if offset >= self._segments[i].base_offset:
                return i
        return 0

    def read(self, offset, max_records=500):
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

            if self.path is None:
                i = offset - start
                return list(self._records[i:i + max_records])

            idx = self._find_segment_index(offset)
            records = []
            cur = offset
            # Walk forward across segments. A segment may return an empty batch
            # if `cur` falls in a gap left by compaction; in that case we simply
            # advance to the next segment rather than stopping.
            while idx < len(self._segments) and len(records) < max_records:
                seg = self._segments[idx]
                batch = seg.read(cur, max_records - len(records))
                if batch:
                    records.extend(batch)
                    cur = batch[-1]["offset"] + 1
                idx += 1
            return records

    def wait_for_data(self, offset, timeout):
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._end_offset_locked() <= offset:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return True

    # -- compaction --------------------------------------------------------
    
    def compact(self):
        """Compacts all segments EXCEPT the active one, keeping only the latest value for each key."""
        with self._cond:
            if not self.path or len(self._segments) <= 1:
                return
            
            # We compact all but the last segment
            inactive_segments = self._segments[:-1]
            active_segment = self._segments[-1]
            
            latest_keys = {}
            # 1. Build a map of key -> latest offset in inactive segments
            for seg in inactive_segments:
                # Read segment completely (in chunks if we wanted to be memory efficient, but this is fine for now)
                # Actually, segments could be large, but we only have a simplistic Segment.read for now
                records = seg.read(seg.base_offset, 1000000) 
                for rec in records:
                    k = rec.get("key")
                    if k is not None:
                        latest_keys[k] = rec["offset"]

            # 2. Write compacted records to new segments
            compacted_segments = []
            for seg in inactive_segments:
                records = seg.read(seg.base_offset, 1000000)
                retained = []
                for rec in records:
                    k = rec.get("key")
                    # Retain if no key, or if it is the latest offset for this key in the inactive range
                    if k is None or latest_keys.get(k) == rec["offset"]:
                        retained.append(rec)
                
                if retained:
                    # Write to a new temporary segment file
                    tmp_path = seg.path + ".compacted"
                    new_seg = Segment(tmp_path, seg.base_offset, self.fsync)
                    new_seg.append(retained)
                    new_seg.close() # Close to flush and get ready to rename
                    compacted_segments.append((seg, tmp_path, new_seg.base_offset))

            # 3. Replace old segments with compacted ones
            new_segment_list = []
            for old_seg, tmp_path, base_offset in compacted_segments:
                old_seg.close()
                os.replace(tmp_path, old_seg.path)
                # Re-open the compacted segment
                new_segment_list.append(Segment(old_seg.path, base_offset, self.fsync))
            
            # Delete any old segments that ended up completely empty (not retained)
            retained_bases = {b for _, _, b in compacted_segments}
            for old_seg in inactive_segments:
                if old_seg.base_offset not in retained_bases:
                    old_seg.close()
                    try:
                        os.remove(old_seg.path)
                    except OSError:
                        pass
            
            # Update segment list
            self._segments = new_segment_list + [active_segment]
            self._start_offset = self._segments[0].base_offset if self._segments else 0

    def delete(self):
        """Deletes all segments from disk and closes the log."""
        with self._cond:
            for seg in self._segments:
                seg.close()
                try:
                    os.remove(seg.path)
                except OSError:
                    pass
            self._segments = []
            if self.path and os.path.exists(self.path):
                try:
                    os.rmdir(self.path)
                except OSError:
                    pass

    def close(self):
        with self._cond:
            for seg in self._segments:
                seg.close()
            self._segments = []
