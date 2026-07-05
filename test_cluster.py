import subprocess
import time
import urllib.request
import urllib.error
import json
import os
import shutil

def main():
    print("Cleaning up old data...")
    for d in ["zk_data", "broker1_data", "broker2_data", "broker3_data"]:
        shutil.rmtree(d, ignore_errors=True)

    print("Starting Zookeeper...")
    zk = subprocess.Popen(["python3", "-m", "minikafka.zookeeper", "--port", "2181"])
    time.sleep(1)

    print("Starting Brokers...")
    brokers = []
    for i in range(1, 4):
        port = 9090 + i
        b = subprocess.Popen([
            "python3", "-m", "minikafka.server",
            "--port", str(port),
            "--broker-id", f"b{i}",
            "--zk-url", "http://127.0.0.1:2181",
            "--data-dir", f"broker{i}_data",
            "--segment-bytes", "1024"
        ], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        brokers.append(b)
    
    time.sleep(2)

    try:
        print("Creating topic 'test' with 1 partition and replication factor 3...")
        req = urllib.request.Request("http://127.0.0.1:9091/topics", data=b'{"name": "test", "partitions": 1, "replication_factor": 3}', method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req) as res:
            print(res.read())

        time.sleep(2) # let it sync
        
        # We don't know which broker is leader, let's ask ZK
        with urllib.request.urlopen("http://127.0.0.1:2181/topics") as res:
            zk_topics = json.loads(res.read())
            leader_id = zk_topics["test"][0]["leader"]
            print(f"Leader is {leader_id}")
            
            # Map leader id to port
            port_map = {"b1": 9091, "b2": 9092, "b3": 9093}
            leader_port = port_map[leader_id]

        print("Producing messages to leader...")
        entries = [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}] * 20 # 40 records, should be enough to roll segments
        req = urllib.request.Request(f"http://127.0.0.1:{leader_port}/produce", data=json.dumps({"topic": "test", "records": entries}).encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req) as res:
            print("Produce response:", res.read())

        time.sleep(2) # let followers catch up

        print("Checking metrics on leader to see ISR...")
        with urllib.request.urlopen(f"http://127.0.0.1:{leader_port}/metrics") as res:
            metrics = json.loads(res.read())
            print("Leader metrics:", json.dumps(metrics["topics"], indent=2))

        print("Triggering compaction...")
        req = urllib.request.Request(f"http://127.0.0.1:{leader_port}/topics/compact", data=b'{"topic": "test", "partition": 0}', method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as res:
                print("Compaction response:", res.read())
        except Exception as e:
            print("Compaction failed:", e)

        time.sleep(1)

        print("Deleting topic...")
        req = urllib.request.Request(f"http://127.0.0.1:{leader_port}/topics?name=test", method="DELETE")
        with urllib.request.urlopen(req) as res:
            print("Delete response:", res.read())

        time.sleep(4) # let broker process delete and ZK sync
        print("Checking data directories for deletion...")
        for i in range(1, 4):
            path = f"broker{i}_data/test-0"
            if os.path.exists(path):
                print(f"ERROR: {path} still exists!")
            else:
                print(f"{path} successfully deleted.")

    finally:
        print("Stopping processes...")
        zk.kill()
        for i, b in enumerate(brokers):
            b.kill()
            out, err = b.communicate()
            if err:
                print(f"Broker b{i+1} stderr:")
                print(err.decode())

if __name__ == "__main__":
    main()
