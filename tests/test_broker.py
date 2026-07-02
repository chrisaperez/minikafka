import tempfile
import unittest

from minikafka.broker import Broker
from minikafka.errors import (
    BadRequest, OffsetOutOfRange, StaleGeneration, TopicAlreadyExists,
    TopicNotFound, UnknownMember)


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.broker = Broker()

    def tearDown(self):
        self.broker.close()

    def test_topic_lifecycle(self):
        self.broker.create_topic("events", 3)
        self.assertEqual(self.broker.topics(), {"events": 3})
        with self.assertRaises(TopicAlreadyExists):
            self.broker.create_topic("events", 3)
        with self.assertRaises(TopicNotFound):
            self.broker.produce("missing", [{"value": 1}])
        with self.assertRaises(BadRequest):
            self.broker.create_topic("bad name!", 1)

    def test_key_partitioning_is_stable_and_sticky(self):
        self.broker.create_topic("events", 4)
        for _ in range(3):
            self.broker.produce("events", [{"key": "user-42", "value": "x"}])
        homes = [p for p in range(4)
                 if self.broker.fetch("events", p, 0)["records"]]
        self.assertEqual(len(homes), 1)  # same key -> same partition, always

    def test_keyless_round_robin_spreads(self):
        self.broker.create_topic("events", 4)
        self.broker.produce("events", [{"value": i} for i in range(8)])
        counts = [self.broker.fetch("events", p, 0)["end_offset"]
                  for p in range(4)]
        self.assertEqual(counts, [2, 2, 2, 2])

    def test_explicit_partition(self):
        self.broker.create_topic("events", 4)
        self.broker.produce("events", [{"value": 1}], partition=2)
        self.assertEqual(self.broker.fetch("events", 2, 0)["end_offset"], 1)
        self.assertEqual(self.broker.fetch("events", 0, 0)["end_offset"], 0)

    def test_fetch_shape(self):
        self.broker.create_topic("t", 1)
        self.broker.produce("t", [{"value": i} for i in range(5)])
        resp = self.broker.fetch("t", 0, 2, max_records=2)
        self.assertEqual([r["offset"] for r in resp["records"]], [2, 3])
        self.assertEqual(resp["next_offset"], 4)
        self.assertEqual(resp["end_offset"], 5)


class GroupCoordinatorTest(unittest.TestCase):
    def setUp(self):
        self.broker = Broker(session_timeout=0.3)
        self.broker.create_topic("events", 4)
        self.coord = self.broker.coordinator

    def tearDown(self):
        self.broker.close()

    def test_single_member_owns_everything(self):
        resp = self.coord.join("g", "c1", ["events"])
        self.assertEqual(len(resp["assignment"]), 4)

    def test_two_members_split_and_generation_bumps(self):
        r1 = self.coord.join("g", "c1", ["events"])
        r2 = self.coord.join("g", "c2", ["events"])
        self.assertGreater(r2["generation"], r1["generation"])
        with self.assertRaises(StaleGeneration):
            self.coord.heartbeat("g", "c1", r1["generation"])
        r1b = self.coord.join("g", "c1", ["events"])
        owned = {tuple(tp) for tp in r1b["assignment"]}
        owned |= {tuple(tp) for tp in
                  self.coord.describe()["g"]["members"][1]["assignment"]}
        self.assertEqual(owned, {("events", p) for p in range(4)})
        self.assertEqual(len(r1b["assignment"]), 2)

    def test_rejoin_without_change_keeps_generation_stable(self):
        # A member rejoining with an unchanged outcome must NOT bump the
        # generation; otherwise every rejoin invalidates the other
        # members' heartbeats and the group rebalances forever.
        self.coord.join("g", "c1", ["events"])
        r2 = self.coord.join("g", "c2", ["events"])
        gen = r2["generation"]
        r1b = self.coord.join("g", "c1", ["events"])  # c1 catches up
        self.assertEqual(r1b["generation"], gen)
        self.assertEqual(
            self.coord.heartbeat("g", "c2", gen)["generation"], gen)
        # crash/restart of an existing member: same id, same assignment
        r1c = self.coord.join("g", "c1", ["events"])
        self.assertEqual(r1c["generation"], gen)

    def test_session_timeout_evicts_and_reassigns(self):
        self.coord.join("g", "c1", ["events"])
        r2 = self.coord.join("g", "c2", ["events"])
        import time
        deadline = time.time() + 3
        while time.time() < deadline:
            time.sleep(0.1)
            try:  # keep c2 alive; c1 goes silent and should be evicted
                self.coord.heartbeat("g", "c2", r2["generation"])
            except StaleGeneration as e:
                r2 = self.coord.join("g", "c2", ["events"])
            members = [m["id"] for m in self.coord.describe()["g"]["members"]]
            if members == ["c2"]:
                break
        self.assertEqual(members, ["c2"])
        self.assertEqual(
            len(self.coord.describe()["g"]["members"][0]["assignment"]), 4)

    def test_commit_fencing(self):
        r1 = self.coord.join("g", "c1", ["events"])
        self.coord.commit("g", "c1", r1["generation"],
                          {"events": {"0": 10}})
        self.assertEqual(self.coord.committed("g")["events"]["0"], 10)
        self.coord.join("g", "c2", ["events"])  # rebalance -> r1 is stale
        with self.assertRaises(StaleGeneration):
            self.coord.commit("g", "c1", r1["generation"],
                              {"events": {"0": 20}})
        with self.assertRaises(UnknownMember):
            self.coord.commit("g", "ghost", 999, {"events": {"0": 20}})
        self.assertEqual(self.coord.committed("g")["events"]["0"], 10)

    def test_lag_in_metrics(self):
        self.broker.produce("events", [{"value": i} for i in range(20)],
                            partition=0)
        r = self.coord.join("g", "c1", ["events"])
        self.coord.commit("g", "c1", r["generation"], {"events": {"0": 5}})
        m = self.broker.metrics()
        self.assertEqual(m["groups"]["g"]["lag"]["events"]["0"], 15)
        self.assertEqual(m["groups"]["g"]["lag"]["events"]["1"], 0)


class BrokerPersistenceTest(unittest.TestCase):
    def test_everything_survives_restart(self):
        with tempfile.TemporaryDirectory() as data_dir:
            b1 = Broker(data_dir=data_dir)
            b1.create_topic("events", 2)
            b1.produce("events", [{"key": str(i), "value": i}
                                  for i in range(50)])
            r = b1.coordinator.join("g", "c1", ["events"])
            b1.coordinator.commit("g", "c1", r["generation"],
                                  {"events": {"0": 7}})
            ends = {p: b1.fetch("events", p, 0)["end_offset"]
                    for p in range(2)}
            b1.close()

            b2 = Broker(data_dir=data_dir)
            self.assertEqual(b2.topics(), {"events": 2})
            for p in range(2):
                self.assertEqual(b2.fetch("events", p, 0)["end_offset"],
                                 ends[p])
            self.assertEqual(b2.coordinator.committed("g")["events"]["0"], 7)
            # a rejoining consumer resumes from the committed offset
            r2 = b2.coordinator.join("g", "c1", ["events"])
            self.assertEqual(r2["offsets"]["events"]["0"], 7)
            b2.close()


if __name__ == "__main__":
    unittest.main()
