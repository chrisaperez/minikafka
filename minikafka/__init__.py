"""minikafka: a small, educational Kafka-style message broker.

Core pieces:
- PartitionLog: append-only record log, optionally file-backed.
- Broker: topics, partitions, produce/fetch.
- GroupCoordinator: consumer groups, rebalancing, committed offsets.
- server: HTTP/JSON front-end for the broker.
- client: Producer and Consumer that speak that API.
"""

__version__ = "0.1.0"

from .broker import Broker
from .client import BrokerClient, Consumer, Producer

__all__ = ["Broker", "BrokerClient", "Consumer", "Producer", "__version__"]
