"""Kafka topics interface. (lib confluent_kafka)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import (
    Any,
    NamedTuple,
    Self,
    override,
)

from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_INVALID,
    Consumer,
    KafkaError,
    Message,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    TopicMetadata as ConfluentTopicMetadata,
)

from lazy_kafka.config import KafkaConfiguration

_LOGGER = logging.getLogger(__name__)


def _timestamp_to_str(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp / 1e3, UTC)
    return dt.isoformat()


class NoMessagesError(Exception):
    """No message retrieved by consumer."""

    pass


class MessageRetriableError(Exception):
    """No message retrieved by consumer."""

    pass


class ConsumerPollError(Exception):
    """Too many poll() calls and no message retrieved."""

    pass


class OffsetInvalidError(Exception):
    """Offset is invalid on the partition.

    [ref](https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/get_watermark_offsets.py)
    """

    pass


class LazyKafkaMessage(NamedTuple):
    """LazyKafka internal message type."""

    timestamp: str
    offset: int
    key: bytes
    message: str

    @classmethod
    def from_confluent_kafka(cls, msg: Message) -> Self:
        """Alternative constructor from confluent kafka native Message type."""
        timestamp_type = msg.timestamp()
        # TODO: handle timestamps properly:
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.timestamp
        if timestamp_type[0] != 1:
            raise ValueError("Timestamp type not available")
        timestamp = _timestamp_to_str(timestamp_type[1])
        return cls(timestamp, msg.offset(), msg.key(), str(msg.value()))

    # TODO: this method is not used at the moment,
    #   fix the interface implementations including this one
    def to_table_values(self):
        return {
            "timestamp": self.timestamp,
            "offset": self.offset,
            "key": self.key,
            "message": self.message,
        }


class TopicMetadata(ConfluentTopicMetadata):
    topic: str | None
    partition: Mapping[Any, Any]
    error: Any

    def to_table_values(self):
        return self.__dict__


class Topic(str):
    """Custom type to represent the selected `subject`.

    The class has to support the `unpack` operator to play nice with
    textual `DataTable.add_rows`.


    """

    def to_table_values(self):
        return self

    @override
    def __iter__(self):
        yield str(self)


# TOOD: assigne aget_details and aget_subjects to implement Protocol
class KafkaClient:
    """Confluent Kafka based Client."""

    MESSAGE_POLL_TIMEOUT_SECONDS = 1.0
    WATERMARK_TIMEOUT_SECONDS = 10.0
    MAX_CONSUMER_POLL = 100

    def __init__(self, config: KafkaConfiguration):
        self.config = config
        _LOGGER.debug(f"{self.config=}")
        _LOGGER.debug("Instantiate Consumer")
        logger = logging.getLogger("consumer")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
        )
        logger.addHandler(handler)
        self._client = Consumer(self.config.to_config(), logger=logger)
        _LOGGER.debug("Consumer ready")

    def assign(self, partitions: list[TopicPartition]):
        self._client.assign(partitions)

    def list_topics(self) -> dict[Topic, TopicMetadata]:
        """Using AdminClient get metadata about all topics."""
        admin_client = AdminClient(self.config.to_config())
        raw_topics: list[TopicMetadata] = list(
            admin_client.list_topics(timeout=1000).topics.values()
        )
        _LOGGER.debug(f"{raw_topics=}")
        return {Topic(i.topic): i for i in raw_topics if i.topic is not None}

    def asubjects(self) -> asyncio.Future[dict[Topic, TopicMetadata]]:
        loop = asyncio.get_running_loop()
        assert loop is not None
        return loop.run_in_executor(None, self.list_topics)

    def aget_details(self, topic: Topic):
        loop = asyncio.get_running_loop()
        assert loop is not None
        return loop.run_in_executor(None, self.get_topic_information, topic)

    def get_topic_information(self, topic: str) -> TopicMetadata:
        """Return metadata about single topic."""
        admin_client = AdminClient(self.config.to_config())
        topic_metadata: Mapping[str, TopicMetadata] = admin_client.list_topics(
            topic=topic, timeout=1000
        ).topics
        # topics: Map of topics indexed by the topic name. Value is a TopicMetadata object.
        assert topic in topic_metadata
        return topic_metadata[topic]

    def get_topic_partitions(self, topic: str) -> list[TopicPartition]:
        """Get all partitions for a topic."""
        metadata = self.get_topic_information(topic)

        partitions = []
        for partition_id in metadata.partitions:
            partitions.append(TopicPartition(topic, partition_id))

        return partitions

    def get_watermark_offsets(self, partition: TopicPartition) -> tuple[int, int]:
        """Get the low and high watermark offsets for a partition."""
        return self._client.get_watermark_offsets(
            partition, timeout=self.WATERMARK_TIMEOUT_SECONDS, cached=False
        )

    def poll(self):
        """Poll the client implementation for new messages.

        Returns: kafka message

        Raises:
            NoMessagesError: topic has no new messages
            MessageRetriableError: a recoverable error

        """
        msg: Message | None = self._client.poll(self.MESSAGE_POLL_TIMEOUT_SECONDS)
        if msg is None:
            current_partition_assignment = self.position(self._client.assignment())
            # TODO: verify:  assert len(current_partition_assignment) == 1
            if len(current_partition_assignment) == 0:
                raise NoMessagesError
            if current_partition_assignment[0].offset == OFFSET_INVALID:
                raise OffsetInvalidError
            raise NoMessagesError
        if msg.error() and msg.error().retriable():
            raise MessageRetriableError(f"{msg.error().name()}: {msg.error().str()}")

        return msg

    def step_offset(self):
        """Increase the current offset by one.

        Usually this is not necessary, however in case the offset was invalid or
        not committed it can be necessary to increase the offset by calling this
        method.
        """
        current_partition_assignment = self.position(self._client.assignment())
        assert len(current_partition_assignment) == 1
        current_partition = current_partition_assignment[0]
        current_partition.offset += 1
        self._client.seek(current_partition)

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        """Retrieve current positions (offsets) for the specified partitions."""
        return self._client.position(partitions)

    def aget_last_messages(self, topic):
        """Retrieve a single message from `topic`.

        Args:
            topic (): name of the topic

        Raises:
            ConsumerPollError: Too many polling trials.
            ValueError: Timestamp did not have a valid value.

        Returns:
            future (tuple of timestamp, key, offset and payload)
        """
        # get event loop
        loop = asyncio.get_running_loop()
        assert loop is not None
        return loop.run_in_executor(None, self._consume, topic)

    def _consume(self, topic: str) -> list[Message]:
        """Consume messages from a topic.

        This consume uses subscribe to watch for changes.
        Guard against infinite polling.
        Mutates _client state subscribe -> close.

        Args:
            topic: name of the topic

        Returns:
            list of confluent_kafka Message objects

        Raises:
            ConsumerPollError: Too many polling attempts without messages
        """
        self._client.subscribe([topic])
        messages: list[Message] = []
        poll_count = 0
        try:
            while len(messages) == 0:
                poll_count += 1
                if poll_count > self.MAX_CONSUMER_POLL:
                    raise ConsumerPollError("Too many polling attempts.")
                try:
                    msg = self.poll()
                    messages.append(msg)
                except (NoMessagesError, MessageRetriableError):
                    # These errors are ok and we want to retry
                    continue
        finally:
            self._client.close()

        return messages

    def get_partition_offsets(
        self, partitions: list[TopicPartition], n: int
    ) -> dict[TopicPartition, tuple[int, int]]:
        """Get partitions offsets based on naive approach.

        partitions (list[TopicPartition]): list of partitions
        n (int): number of partitions

        Returns:
            high and low watermark offsets for each partition
        """
        partition_offsets = {}
        committed = self._client.committed(partitions, timeout=1000)
        _LOGGER.debug(f"{committed=}")
        for partition in committed:
            # Get the beginning and end offsets for this partition
            low_offset, high_offset = self.get_watermark_offsets(partition)
            # Calculate how many messages to read from this partition
            # Simple strategy: divide N evenly across partitions
            partition_n = max(1, n // len(partitions))
            # Calculate the start offset (ensuring we don't go before the beginning)
            start_offset = max(low_offset, high_offset - partition_n)
            # start offset cannot be negative-  why?
            assert start_offset >= 0
            # Store the offset and how many messages we expect from this partition
            partition_offsets[partition] = (start_offset, high_offset)
            _LOGGER.debug(f"{partition=}")
            _LOGGER.debug(f"{start_offset=} - {high_offset=}")
        return partition_offsets

    def better_consume_n(self, topic: str, n: int) -> list[LazyKafkaMessage]:
        """Read `n` messages from a kafka topic.

        Reading *"batch"* from a Kafka topic has a few caveats, more precisely,
        making sure that all partitions are being read.

        After that a non-trivial sorting can take place to collate the results.

        This method takes a naive approach and reads equal number of messages,
        from each partition.
        """
        partitions = self.get_topic_partitions(topic)
        # Set up offsets for each partition to read the last N messages
        partition_offsets = self.get_partition_offsets(partitions, n)
        # Assign consumer to all these partitions at the calculated offsets
        partition_assignments = []
        for partition, (start_offset, high_offset) in partition_offsets.items():
            if start_offset == 0 and high_offset == 0:
                # Do not append it to topic list
                continue
            tp = TopicPartition(partition.topic, partition.partition, start_offset)
            partition_assignments.append(tp)

        self.assign(partition_assignments)

        # Collect messages
        messages: list[LazyKafkaMessage] = []
        message_count = 0
        max_messages_total = n

        while message_count < max_messages_total:
            # Poll for message
            try:
                msg = self.poll()
            except (NoMessagesError, MessageRetriableError):
                # No message within timeout - check if we've reached the end of all partitions
                # TODO: move this into a member function -> all_done vs not
                #       can be challenging, should partition state be attached to the KafkaClient???
                all_done = True
                for partition in partition_assignments:
                    current_position = self.position([partition])[0]
                    assert isinstance(current_position, TopicPartition)
                    _, high_offset = partition_offsets[
                        TopicPartition(partition.topic, partition.partition, 0)
                    ]
                    _LOGGER.debug(
                        f"Partition offset: {current_position=}, {high_offset=}"
                    )
                    current_offset = current_position.offset
                    if current_offset < high_offset:
                        all_done = False
                        break

                # NOTE: break out of the loop here
                if all_done:
                    break
                continue

            if msg.error():
                print("---Message---")
                print(f"{msg.value()}")
                print(f"{msg.error().reason()}")
                print("")
                error_code = msg.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    # End of partition, not an error
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            # Process message
            messages.append(LazyKafkaMessage.from_confluent_kafka(msg))
            message_count += 1

        # Sort messages by timestamp if available
        _LOGGER.debug("%s", f"{len(messages)=}")
        messages.sort(key=lambda m: m.timestamp)

        # Return the latest N messages
        return messages[-n:] if len(messages) > n else messages

    def aget_last_n_messages(
        self, topic: str, last_n_messages: int = 10
    ) -> asyncio.Future[list[LazyKafkaMessage]]:
        """Retrieve last n messages from `topic`.

        Consumer instantiated.
        """
        loop = asyncio.get_running_loop()
        assert loop is not None
        return loop.run_in_executor(None, self.better_consume_n, topic, last_n_messages)

    def get_last_messages(self, topic):
        MESSAGE_POLL_TIMEOUT_SECONDS = 1.0

        _LOGGER.debug("reading message")
        consumer = Consumer(self.config.to_config())
        # compensate with 1, because we cnt-- at start
        consumer.subscribe([topic])
        msg = consumer.poll(MESSAGE_POLL_TIMEOUT_SECONDS)
        if msg is None:
            _LOGGER.debug("msg was None")
            raise NoMessagesError
        timestamp_type = msg.timestamp()
        if timestamp_type[0] != 1:
            raise ValueError("Timestamp type not available")
        timestamp = _timestamp_to_str(timestamp_type[1])
        return (timestamp, msg.offset(), msg.key(), str(msg.value()))


class KafkaTopicDetailsClient(KafkaClient):
    def __init__(self, config: KafkaConfiguration, *args, **kwargs):
        super().__init__(config)
        # re-assign the methods to comply with the service interface:
        # asubjects and aget_details
        self.asubjects = self.aget_last_n_messages

        async def _msg_converter(message):
            import json

            _LOGGER.debug(message)
            try:
                _msg = message.message
            except AttributeError:
                return "{}"

            if _msg is None or _msg == "None":
                return "{}"
            try:
                details = json.loads(
                    "{" + str(_msg).split(sep="{")[1].rsplit("}")[0] + "}"
                )
            except json.JSONDecodeError as e:
                _LOGGER.error(e)
                details = str(_msg)
            return details

        self.aget_details = _msg_converter


@dataclass
class TopicData:
    topic: str
    partitions: object


def topic_data_to_dict(topic: TopicData):
    d = {
        "topic": topic.topic,
        "partitions": {
            k: {
                "id": v.id,
                "leader": v.leader,
                "replicas": v.replicas,
                "isrs": v.isrs,
                "error": v.error,
            }
            for k, v in topic.partitions.items()
        },
    }
    return d


# todo create into a class with config etc. which will serve as the facade
# todo instead of returning list, turn this into generator?


if __name__ == "__main__":
    from lazy_kafka.config import Configuration

    cfg = Configuration()
    MESSAGE_POLL_TIMEOUT_SECONDS = 1.0
    WATERMARK_TIMEOUT_SECONDS = 10.0
    TOPIC = "user-topic"
    READ_LAST_N_MESSAGES = 10

    settings = {
        "bootstrap.servers": cfg.kafka.bootstrap_servers,
        "group.id": "my-work-group-testicle-farts",
        "auto.offset.reset": "earliest",
        "security.protocol": "plaintext",
        "enable.auto.commit": "false",
    }

    consumer = Consumer(settings)

    def get_last_n_offset(high_mark, last_n):
        if last_n > high_mark:
            # offset beginning is -2
            return OFFSET_BEGINNING
        else:
            return high_mark - last_n

    t = TopicPartition(TOPIC, partition=0, offset=-2)
    (lo, hi) = consumer.get_watermark_offsets(
        t, timeout=WATERMARK_TIMEOUT_SECONDS, cached=False
    )
    _LOGGER.debug(f"{lo=} {hi=}")

    offset = get_last_n_offset(hi, READ_LAST_N_MESSAGES)
    _LOGGER.debug("reading from offset=%s", offset)
    consumer.assign([t])
    _LOGGER.debug("%s", f"{consumer.assignment()=}")

    # compensate with 1, because we cnt-- at start
    i = READ_LAST_N_MESSAGES + 1
    while i := i - 1:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(MESSAGE_POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            _LOGGER.debug(
                "%s", f"{msg.offset():<5}, {str(msg.value()):>10}, {msg.topic():>}"
            )
        except KeyboardInterrupt:
            break
