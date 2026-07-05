import subprocess
import time
import urllib.request
import urllib.error
import json
import os
import sys

def main():
    print("Starting ZK")
    zk = subprocess.Popen(["python3", "-m", "minikafka.zookeeper", "--port", "2181"], stdout=sys.stdout, stderr=sys.stderr)
    time.sleep(1)
    
    print("Starting Broker")
    broker = subprocess.Popen([
        "python3", "-m", "minikafka.server",
        "--port", "9091",
        "--broker-id", "b1",
        "--zk-url", "http://127.0.0.1:2181"
    ], stdout=sys.stdout, stderr=sys.stderr)
    time.sleep(2)
    
    try:
        print("Sending POST /topics")
        req = urllib.request.Request("http://127.0.0.1:9091/topics", data=b'{"name": "test", "partitions": 1, "replication_factor": 1}', method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req) as res:
            print("Response:", res.read())
    except Exception as e:
        print("Exception:", e)
    finally:
        print("Killing processes")
        broker.terminate()
        zk.terminate()
        broker.wait()
        zk.wait()

if __name__ == "__main__":
    main()
