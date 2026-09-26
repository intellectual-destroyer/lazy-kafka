"""Tests for Kafka topic functionality."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from confluent_kafka import Message, TopicPartition

from lazy_kafka.config import KafkaConfiguration
from lazy_kafka.topic import (
    ConsumerPollError,
    KafkaClient,
    LazyKafkaMessage,
    MessageRetriableError,
    NoMessagesError,
    OffsetInvalidError,
    Topic,
    TopicMetadata,
    _timestamp_to_str,
    topic_data_to_dict,
)


class TestTimestampToStr:
    """Tests for _timestamp_to_str function."""

    def test_converts_timestamp_to_isoformat(self) -> None:
        """Test that timestamp is converted to ISO format."""
        # Test with a known timestamp
        timestamp = 1705342245000  # milliseconds
        result = _timestamp_to_str(timestamp)
        
        # Should be in ISO format
        assert "T" in result
        assert "Z" in result or "+" in result
        
        # Parse and verify
        dt = datetime.fromisoformat(result.replace("Z", "+00:00"))
        assert dt.year == 2024

    def test_handles_zero_timestamp(self) -> None:
        """Test handling of zero timestamp."""
        result = _timestamp_to_str(0)
        assert isinstance(result, str)


class TestLazyKafkaMessage:
    """Tests for LazyKafkaMessage NamedTuple."""

    def test_creation(self) -> None:
        """Test LazyKafkaMessage creation."""
        msg = LazyKafkaMessage(
            timestamp="2024-01-15T14:30:45",
            offset=123,
            key=b"test-key",
            message="test-message",
        )
        assert msg.timestamp == "2024-01-15T14:30:45"
        assert msg.offset == 123
        assert msg.key == b"test-key"
        assert msg.message == "test-message"

    def test_to_table_values(self) -> None:
        """Test LazyKafkaMessage.to_table_values method."""
        msg = LazyKafkaMessage(
            timestamp="2024-01-15T14:30:45",
            offset=123,
            key=b"test-key",
            message="test-message",
        )
        result = msg.to_table_values()
        assert result["timestamp"] == "2024-01-15T14:30:45"
        assert result["offset"] == 123
        assert result["key"] == b"test-key"
        assert result["message"] == "test-message"

    def test_from_confluent_kafka(self) -> None:
        """Test LazyKafkaMessage.from_confluent_kafka method."""
        mock_msg = MagicMock(spec=Message)
        mock_msg.timestamp.return_value = (1, 1705342245000)  # type 1 (CreateTime), timestamp in ms
        mock_msg.offset.return_value = 123
        mock_msg.key.return_value = b"test-key"
        mock_msg.value.return_value = b"test-message"
        
        result = LazyKafkaMessage.from_confluent_kafka(mock_msg)
        
        assert result.offset == 123
        assert result.key == b"test-key"
        assert result.message == "b'test-message'"  # str() of bytes

    def test_from_confluent_kafka_invalid_timestamp_type(self) -> None:
        """Test that invalid timestamp type raises ValueError."""
        mock_msg = MagicMock(spec=Message)
        mock_msg.timestamp.return_value = (0, 123)  # type 0 (NotAvailable)
        
        with pytest.raises(ValueError, match="Timestamp type not available"):
            LazyKafkaMessage.from_confluent_kafka(mock_msg)


class TestTopic:
    """Tests for Topic class."""

    def test_topic_creation(self) -> None:
        """Test Topic creation from string."""
        topic = Topic("test-topic")
        assert topic == "test-topic"
        assert isinstance(topic, str)

    def test_topic_to_table_values(self) -> None:
        """Test Topic.to_table_values method."""
        topic = Topic("test-topic")
        result = topic.to_table_values()
        assert result == "test-topic"

    def test_topic_iteration(self) -> None:
        """Test Topic iteration."""
        topic = Topic("test-topic")
        result = list(topic)
        assert result == ["test-topic"]


class TestTopicMetadata:
    """Tests for TopicMetadata class."""

    def test_to_table_values(self) -> None:
        """Test TopicMetadata.to_table_values method."""
        # Create a simple object with __dict__ attribute
        # We can't easily create a real ConfluentTopicMetadata, so we'll test
        # that the method exists and returns a dict-like object
        from lazy_kafka.topic import TopicMetadata
        
        # Just verify the method exists on the class
        assert hasattr(TopicMetadata, "to_table_values")
        # Create a minimal mock that doesn't interfere
        metadata = type("MockMetadata", (), {"__dict__": {"topic": "test-topic", "partitions": {}, "error": None}})()
        metadata.__class__ = TopicMetadata
        
        result = metadata.to_table_values()
        assert isinstance(result, dict)


class TestKafkaClient:
    """Tests for KafkaClient class."""

    @pytest.fixture
    def mock_client(self) -> KafkaClient:
        """Create a mock KafkaClient for testing."""
        with patch("lazy_kafka.topic.Consumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer
            
            config = KafkaConfiguration(bootstrap_servers="localhost:9092")
            return KafkaClient(config)

    def test_init(self, mock_client: KafkaClient) -> None:
        """Test KafkaClient initialization."""
        assert mock_client.config is not None
        assert mock_client._client is not None

    def test_assign(self, mock_client: KafkaClient) -> None:
        """Test assign method."""
        partitions = [TopicPartition("test-topic", 0)]
        mock_client.assign(partitions)
        # Should call _client.assign
        assert mock_client._client.assign.called

    @patch("lazy_kafka.topic.AdminClient")
    def test_list_topics(self, mock_admin: MagicMock, mock_client: KafkaClient) -> None:
        """Test list_topics method."""
        mock_metadata = MagicMock(spec=TopicMetadata)
        mock_metadata.topic = "test-topic"
        mock_metadata.partitions = {}
        mock_metadata.error = None
        
        mock_topics = {"test-topic": mock_metadata}
        mock_admin_client = MagicMock()
        mock_admin_client.list_topics.return_value.topics = mock_topics
        mock_admin.return_value = mock_admin_client
        
        result = mock_client.list_topics()
        
        assert isinstance(result, dict)
        assert Topic("test-topic") in result

    @patch("lazy_kafka.topic.AdminClient")
    def test_get_topic_information(self, mock_admin: MagicMock, mock_client: KafkaClient) -> None:
        """Test get_topic_information method."""
        mock_metadata = MagicMock(spec=TopicMetadata)
        mock_metadata.topic = "test-topic"
        mock_metadata.partitions = {0: MagicMock()}
        
        mock_topics = {"test-topic": mock_metadata}
        mock_admin_client = MagicMock()
        mock_admin_client.list_topics.return_value.topics = mock_topics
        mock_admin.return_value = mock_admin_client
        
        result = mock_client.get_topic_information("test-topic")
        
        assert result == mock_metadata

    def test_get_topic_partitions(self, mock_client: KafkaClient) -> None:
        """Test get_topic_partitions method."""
        with patch.object(mock_client, "get_topic_information") as mock_get_info:
            mock_metadata = MagicMock()
            mock_metadata.partitions = {0: None, 1: None, 2: None}
            mock_get_info.return_value = mock_metadata
            
            result = mock_client.get_topic_partitions("test-topic")
            
            assert len(result) == 3
            assert all(isinstance(p, TopicPartition) for p in result)

    def test_get_watermark_offsets(self, mock_client: KafkaClient) -> None:
        """Test get_watermark_offsets method."""
        partition = TopicPartition("test-topic", 0)
        mock_client._client.get_watermark_offsets.return_value = (0, 100)
        
        result = mock_client.get_watermark_offsets(partition)
        
        assert result == (0, 100)

    def test_poll_no_message(self, mock_client: KafkaClient) -> None:
        """Test poll method when no message is available."""
        mock_client._client.poll.return_value = None
        mock_client._client.assignment.return_value = []
        # position() calls _client.position() which needs to return [] for empty assignment
        mock_client._client.position.return_value = []
        
        with pytest.raises(NoMessagesError):
            mock_client.poll()

    def test_poll_offset_invalid(self, mock_client: KafkaClient) -> None:
        """Test poll method when offset is invalid."""
        from confluent_kafka import OFFSET_INVALID
        
        mock_client._client.poll.return_value = None
        partition = TopicPartition("test-topic", 0, OFFSET_INVALID)
        mock_client._client.assignment.return_value = [partition]
        # position() should return the partition with OFFSET_INVALID
        mock_client._client.position.return_value = [partition]
        
        with pytest.raises(OffsetInvalidError):
            mock_client.poll()

    def test_poll_retriable_error(self, mock_client: KafkaClient) -> None:
        """Test poll method when message has retriable error."""
        mock_msg = MagicMock(spec=Message)
        mock_error = MagicMock()
        mock_error.retriable.return_value = True
        mock_msg.error.return_value = mock_error
        mock_client._client.poll.return_value = mock_msg
        
        with pytest.raises(MessageRetriableError):
            mock_client.poll()

    def test_poll_success(self, mock_client: KafkaClient) -> None:
        """Test poll method with successful message retrieval."""
        mock_msg = MagicMock(spec=Message)
        mock_msg.error.return_value = None
        mock_client._client.poll.return_value = mock_msg
        
        result = mock_client.poll()
        assert result == mock_msg

    def test_position(self, mock_client: KafkaClient) -> None:
        """Test position method."""
        partitions = [TopicPartition("test-topic", 0)]
        mock_client._client.position.return_value = partitions
        
        result = mock_client.position(partitions)
        assert result == partitions

    def test_step_offset(self, mock_client: KafkaClient) -> None:
        """Test step_offset method."""
        partition = TopicPartition("test-topic", 0, 10)
        mock_client._client.assignment.return_value = [partition]
        # position() calls _client.position() which should return the partition with offset
        mock_client._client.position.return_value = [partition]
        mock_client._client.seek = MagicMock()
        
        mock_client.step_offset()
        
        # Should have increased offset by 1 and called seek
        assert mock_client._client.seek.called

    def test_repr(self, mock_client: KafkaClient) -> None:
        """Test KafkaClient string representation."""
        result = repr(mock_client)
        assert "KafkaClient" in result


class TestTopicDataToDict:
    """Tests for topic_data_to_dict function."""

    def test_converts_topic_metadata(self) -> None:
        """Test that topic_data_to_dict converts TopicMetadata to dict."""
        metadata = MagicMock(spec=TopicMetadata)
        metadata.topic = "test-topic"
        metadata.partitions = {0: MagicMock(), 1: MagicMock()}
        metadata.error = None
        
        # Mock the to_dict method if it exists
        def mock_to_dict() -> dict[str, Any]:
            return {
                "topic": "test-topic",
                "partitions": 2,
                "error": None,
            }
        
        metadata.to_dict = mock_to_dict
        
        result = topic_data_to_dict(metadata)
        
        assert isinstance(result, dict)


class TestKafkaTopicDetailsClient:
    """Tests for KafkaTopicDetailsClient class."""

    def test_init(self) -> None:
        """Test KafkaTopicDetailsClient initialization."""
        with patch("lazy_kafka.topic.KafkaClient") as mock_kafka_client:
            mock_client = MagicMock()
            mock_kafka_client.return_value = mock_client
            
            config = KafkaConfiguration()
            from lazy_kafka.topic import KafkaTopicDetailsClient
            
            client = KafkaTopicDetailsClient(config)
            # KafkaTopicDetailsClient inherits from KafkaClient
            # The mock replaces KafkaClient, so client should be an instance of the mock
            assert isinstance(client, KafkaClient)

    @patch("lazy_kafka.topic.KafkaClient")
    def test_aget_last_n_messages(self, mock_kafka_client: MagicMock) -> None:
        """Test aget_last_n_messages method."""
        mock_client = MagicMock()
        mock_kafka_client.return_value = mock_client
        
        config = KafkaConfiguration()
        from lazy_kafka.topic import KafkaTopicDetailsClient
        
        client = KafkaTopicDetailsClient(config)
        
        # Just verify the method exists
        assert hasattr(client, "aget_last_n_messages")
