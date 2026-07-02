"""End-to-end tests: real HTTP server + real Producer/Consumer clients."""

import tempfile
import threading
import time
import unittest

from minikafka.broker import Broker
from minikafka.client import BrokerClient, Consumer, Producer
from minikafka.server import make_server


def start_server(broker):
    httpd = make_server(broker, "127.0.0.1", 0)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    return httpd, url


def wait_until(predicate, timeout=8.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self.broker = Broker(session_timeout=2.0)
        self.broker.coordinator.start_reaper(interval=0.2)
        self.httpd, self.url = start_server(self.broker)

    def tearDown(self):
        self.httpd.shutdown()
        self.broker.close()

    def drain(self, consumer, expected, timeout=10.0):
        got = []
        deadline = time.monotonic() + timeout
        while len(got) < expected and time.monotonic() < deadline:
            got.extend(consumer.poll(timeout=0.5))
        return got

    def test_produce_consume_commit_resume(self):
        admin = BrokerClient(self.url)
        admin.create_topic("orders", 3)

        producer = Producer(self.url, batch_size=50)
        for i in range(200):
            producer.send("orders", {"n": i}, key=f"user-{i % 10}")
        producer.close()

        c1 = Consumer(self.url, "g1", ["orders"], consumer_id="c1",
                      heartbeat_interval=0.3)
        got = self.drain(c1, 200)
        self.assertEqual(len(got), 200)
        self.assertEqual(sorted(r["value"]["n"] for r in got),
                         list(range(200)))
        self.assertTrue(c1.commit())
        c1.close()

        # Same group, new consumer instance: resumes past committed work.
        producer2 = Producer(self.url, batch_size=50)
        for i in range(200, 210):
            producer2.send("orders", {"n": i}, key=f"user-{i % 10}")
        producer2.close()

        c2 = Consumer(self.url, "g1", ["orders"], consumer_id="c1",
                      heartbeat_interval=0.3)
        got2 = self.drain(c2, 10, timeout=6.0)
        self.assertEqual(sorted(r["value"]["n"] for r in got2),
                         list(range(200, 210)))
        c2.close()

    def test_uncommitted_records_are_redelivered(self):
        admin = BrokerClient(self.url)
        admin.create_topic("t", 1)
        Producer(self.url, batch_size=10).close()  # no-op producer sanity
        p = Producer(self.url, batch_size=10)
        for i in range(20):
            p.send("t", i)
        p.close()

        c1 = Consumer(self.url, "g", ["t"], consumer_id="c1",
                      heartbeat_interval=0.3)
        got = self.drain(c1, 20)
        self.assertEqual(len(got), 20)
        c1.close(leave_group=False)  # simulated crash: nothing committed

        c2 = Consumer(self.url, "g", ["t"], consumer_id="c1",
                      heartbeat_interval=0.3)
        redelivered = self.drain(c2, 20)
        self.assertEqual(len(redelivered), 20)  # at-least-once
        c2.close()

    def test_group_rebalance_splits_partitions(self):
        BrokerClient(self.url).create_topic("evts", 4)
        c1 = Consumer(self.url, "g", ["evts"], consumer_id="c1",
                      heartbeat_interval=0.2)
        self.assertTrue(wait_until(lambda: len(c1.assignment) == 4))

        c2 = Consumer(self.url, "g", ["evts"], consumer_id="c2",
                      heartbeat_interval=0.2)
        # c1's next heartbeat sees the rebalance and rejoins on poll()
        ok = wait_until(lambda: (c1.poll(timeout=0.1) is not None
                                 and len(c1.assignment) == 2
                                 and len(c2.assignment) == 2))
        self.assertTrue(ok, f"c1={c1.assignment} c2={c2.assignment}")
        self.assertEqual(set(c1.assignment) | set(c2.assignment),
                         {("evts", p) for p in range(4)})

        # c2 vanishes without leaving; session timeout hands it all to c1.
        c2.close(leave_group=False)
        ok = wait_until(lambda: (c1.poll(timeout=0.1) is not None
                                 and len(c1.assignment) == 4), timeout=10)
        self.assertTrue(ok, f"c1={c1.assignment}")
        c1.close()

    def test_latest_reset_skips_backlog(self):
        BrokerClient(self.url).create_topic("t", 1)
        p = Producer(self.url, batch_size=10)
        for i in range(30):
            p.send("t", i)
        p.close()

        c = Consumer(self.url, "fresh", ["t"], consumer_id="c1",
                     auto_offset_reset="latest", heartbeat_interval=0.3)
        self.assertEqual(c.poll(timeout=0.3), [])  # backlog skipped
        p2 = Producer(self.url, batch_size=1)
        p2.send("t", "new")
        p2.close()
        got = self.drain(c, 1, timeout=5.0)
        self.assertEqual([r["value"] for r in got], ["new"])
        c.close()


class ServerRestartTest(unittest.TestCase):
    def test_messages_and_offsets_survive_broker_restart(self):
        with tempfile.TemporaryDirectory() as data_dir:
            broker = Broker(data_dir=data_dir, session_timeout=2.0)
            httpd, url = start_server(broker)
            BrokerClient(url).create_topic("t", 2)
            p = Producer(url, batch_size=25)
            for i in range(100):
                p.send("t", {"n": i}, key=str(i))
            p.close()

            c = Consumer(url, "g", ["t"], consumer_id="c1",
                         heartbeat_interval=0.3)
            got = []
            deadline = time.monotonic() + 10
            while len(got) < 60 and time.monotonic() < deadline:
                got.extend(c.poll(timeout=0.5))
            consumed = len(got)
            self.assertGreaterEqual(consumed, 60)
            self.assertTrue(c.commit())
            c.close()
            httpd.shutdown()
            broker.close()

            # --- broker restarts from disk ---
            broker2 = Broker(data_dir=data_dir, session_timeout=2.0)
            httpd2, url2 = start_server(broker2)
            m = BrokerClient(url2).metrics()
            total = sum(pr["end"] for pr in m["topics"]["t"].values())
            self.assertEqual(total, 100)

            c2 = Consumer(url2, "g", ["t"], consumer_id="c1",
                          heartbeat_interval=0.3)
            rest = []
            deadline = time.monotonic() + 10
            while len(rest) < 100 - consumed and time.monotonic() < deadline:
                rest.extend(c2.poll(timeout=0.5))
            self.assertEqual(len(rest), 100 - consumed)
            all_ns = sorted(r["value"]["n"] for r in got + rest)
            self.assertEqual(all_ns, list(range(100)))
            c2.close()
            httpd2.shutdown()
            broker2.close()


if __name__ == "__main__":
    unittest.main()
