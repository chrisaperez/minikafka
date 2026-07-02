"""Error types that map onto HTTP responses from the broker API."""


class BrokerError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self):
        d = {"error": self.code, "message": self.message}
        d.update(self.details)
        return d


class BadRequest(BrokerError):
    status = 400
    code = "bad_request"


class TopicNotFound(BrokerError):
    status = 404
    code = "topic_not_found"


class TopicAlreadyExists(BrokerError):
    status = 409
    code = "topic_already_exists"


class PartitionNotFound(BrokerError):
    status = 404
    code = "partition_not_found"


class OffsetOutOfRange(BrokerError):
    """Requested offset is below the log start or above the log end.

    Carries ``earliest`` and ``latest`` so clients can auto-reset,
    mirroring Kafka's auto.offset.reset behaviour.
    """

    status = 409
    code = "offset_out_of_range"


class UnknownMember(BrokerError):
    """Consumer is not (or no longer) a member of the group; it must rejoin."""

    status = 409
    code = "unknown_member"


class StaleGeneration(BrokerError):
    """The group rebalanced since this consumer joined; it must rejoin.

    Generation checks fence 'zombie' consumers so a crashed-and-replaced
    member cannot commit offsets for partitions it no longer owns.
    """

    status = 409
    code = "stale_generation"
