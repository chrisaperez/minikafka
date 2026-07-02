import os
import tempfile
import unittest

from minikafka.errors import OffsetOutOfRange
from minikafka.log import PartitionLog


class MemoryLogTest(unittest.TestCase):
    def test_append_and_read(self):
        log = PartitionLog()
        base = log.append([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
        self.assertEqual(base, 0)
        self.assertEqual(log.end_offset, 2)
        records = log.read(0)
        self.assertEqual([r["value"] for r in records], [1, 2])
        self.assertEqual([r["offset"] for r in records], [0, 1])
        self.assertEqual(log.read(2), [])  # caught up, not an error

    def test_max_records_limits_read(self):
        log = PartitionLog()
        log.append([{"value": i} for i in range(10)])
        self.assertEqual(len(log.read(0, max_records=3)), 3)
        self.assertEqual(log.read(4, max_records=3)[0]["offset"], 4)

    def test_retention_advances_start_offset(self):
        log = PartitionLog(retention_max_records=5)
        log.append([{"value": i} for i in range(8)])
        self.assertEqual(log.start_offset, 3)
        self.assertEqual(log.end_offset, 8)
        with self.assertRaises(OffsetOutOfRange):
            log.read(0)
        self.assertEqual(log.read(3)[0]["value"], 3)

    def test_out_of_range(self):
        log = PartitionLog()
        log.append([{"value": 1}])
        with self.assertRaises(OffsetOutOfRange) as ctx:
            log.read(5)
        self.assertEqual(ctx.exception.details["earliest"], 0)
        self.assertEqual(ctx.exception.details["latest"], 1)

    def test_metadata_probe_never_range_errors(self):
        log = PartitionLog()
        self.assertEqual(log.read(999, max_records=0), [])


class FileLogTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "t-0.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    def test_persistence_roundtrip(self):
        log = PartitionLog(self.path)
        log.append([{"key": "k", "value": {"n": i}} for i in range(100)])
        log.close()

        reopened = PartitionLog(self.path)
        self.assertEqual(reopened.end_offset, 100)
        records = reopened.read(95, max_records=100)
        self.assertEqual([r["offset"] for r in records], [95, 96, 97, 98, 99])
        self.assertEqual(records[0]["value"], {"n": 95})
        # appends continue from the recovered offset
        base = reopened.append([{"value": "new"}])
        self.assertEqual(base, 100)
        reopened.close()

    def test_torn_tail_is_truncated_on_recovery(self):
        log = PartitionLog(self.path)
        log.append([{"value": i} for i in range(3)])
        log.close()
        with open(self.path, "ab") as f:
            f.write(b'{"offset": 3, "ts": 0, "key": null, "va')  # torn write

        recovered = PartitionLog(self.path)
        self.assertEqual(recovered.end_offset, 3)
        base = recovered.append([{"value": "after-crash"}])
        self.assertEqual(base, 3)
        records = recovered.read(0, max_records=10)
        self.assertEqual(len(records), 4)
        self.assertEqual(records[3]["value"], "after-crash")
        recovered.close()


if __name__ == "__main__":
    unittest.main()
