"""Tests for schema registry functionality."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from lazy_kafka.config import RegistryConfiguration
from lazy_kafka.registry import (
    MetaEnum,
    SchemaRegistry,
    SchemaTypes,
    Subject,
    SubjectDetails,
    SubjectNew,
)


class TestSubject:
    """Tests for Subject class."""

    def test_subject_creation(self) -> None:
        """Test Subject creation from string."""
        subject = Subject("test-subject")
        assert subject == "test-subject"
        assert isinstance(subject, str)

    def test_subject_with_value(self) -> None:
        """Test Subject with value."""
        subject = Subject("my-topic-value")
        assert str(subject) == "my-topic-value"


class TestSchemaTypes:
    """Tests for SchemaTypes enum."""

    def test_enum_values(self) -> None:
        """Test that SchemaTypes has expected values."""
        assert SchemaTypes.AVRO == "AVRO"
        assert SchemaTypes.PROTOBUF == "PROTOBUF"
        assert SchemaTypes.JSON == "JSON"

    def test_enum_members(self) -> None:
        """Test that SchemaTypes has expected members."""
        assert "AVRO" in SchemaTypes.__members__
        assert "PROTOBUF" in SchemaTypes.__members__
        assert "JSON" in SchemaTypes.__members__

    def test_enum_case_insensitive(self) -> None:
        """Test that SchemaTypes is case-insensitive."""
        assert SchemaTypes("avro") == SchemaTypes.AVRO
        assert SchemaTypes("AvRo") == SchemaTypes.AVRO


class TestSubjectDetails:
    """Tests for SubjectDetails TypedDict."""

    def test_subject_details_structure(self) -> None:
        """Test SubjectDetails structure."""
        details: SubjectDetails = {
            "subject": "test-subject",
            "version": 1,
            "id": 123,
            "schema": "{}",
            "schemaType": "AVRO",
        }
        assert details["subject"] == "test-subject"
        assert details["version"] == 1
        assert details["id"] == 123


class TestSubjectNew:
    """Tests for SubjectNew TypedDict."""

    def test_subject_new_structure(self) -> None:
        """Test SubjectNew structure."""
        new_subject: SubjectNew = {
            "schema": "{}",
            "schemaType": "AVRO",
            "references": [],
        }
        assert new_subject["schema"] == "{}"
        assert new_subject["schemaType"] == "AVRO"
        assert new_subject["references"] == []


class TestSchemaRegistry:
    """Tests for SchemaRegistry class."""

    def test_init_default(self) -> None:
        """Test SchemaRegistry initialization with default config."""
        config = RegistryConfiguration()
        registry = SchemaRegistry(config)
        assert registry.host == "http://localhost:8081"

    def test_init_custom_config(self) -> None:
        """Test SchemaRegistry initialization with custom config."""
        config = RegistryConfiguration(host="http://custom:8081")
        registry = SchemaRegistry(config)
        assert registry.host == "http://custom:8081"

    def test_repr(self) -> None:
        """Test SchemaRegistry string representation."""
        config = RegistryConfiguration(host="http://test:8081")
        registry = SchemaRegistry(config)
        assert repr(registry) == "SchemaRegistry@http://test:8081"

    def test_serialize_json_schema(self) -> None:
        """Test JSON schema serialization."""
        config = RegistryConfiguration()
        registry = SchemaRegistry(config)
        schema = {"type": "record", "name": "Test"}
        result = registry._serialize_json_schema(schema)
        assert result == '{"name": "Test", "type": "record"}'

    @patch("httpx.get")
    def test_connection_error(self, mock_get: MagicMock) -> None:
        """Test that connection error is raised when service is unavailable."""
        mock_get.side_effect = httpx.HTTPError("Connection refused")
        config = RegistryConfiguration(host="http://unavailable:8081")
        
        with pytest.raises(ConnectionRefusedError):
            SchemaRegistry(config)

    @patch("httpx.get")
    def test_connection_success(self, mock_get: MagicMock) -> None:
        """Test successful connection to registry."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        config = RegistryConfiguration(host="http://localhost:8081")
        registry = SchemaRegistry(config)
        assert registry.host == "http://localhost:8081"


class TestSchemaRegistryAsync:
    """Tests for async methods of SchemaRegistry."""

    @pytest.fixture
    def mock_registry(self) -> SchemaRegistry:
        """Create a mock SchemaRegistry for async tests."""
        with patch("httpx.get") as mock_get_sync, \
             patch("httpx.AsyncClient") as mock_async_client_class:
            # Mock sync get for __init__ check
            mock_response_sync = MagicMock()
            mock_response_sync.status_code = 200
            mock_get_sync.return_value = mock_response_sync
            
            # Mock AsyncClient
            mock_async_client = AsyncMock()
            mock_async_client_class.return_value = mock_async_client
            
            config = RegistryConfiguration(host="http://test:8081")
            return SchemaRegistry(config)

    async def test_asubject_versions(self, mock_registry: SchemaRegistry) -> None:
        """Test asubject_versions method."""
        # Mock the _client's get method
        mock_response = MagicMock()
        mock_response.json.return_value = [1, 2, 3]
        mock_response.raise_for_status = MagicMock()
        mock_registry._client.get = AsyncMock(return_value=mock_response)
        
        subject = Subject("test-subject")
        result = await mock_registry.asubject_versions(subject)
        
        assert mock_registry._client.get.called
        call_args = mock_registry._client.get.call_args
        url_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
        assert "subjects/test-subject/versions" in url_arg

    async def test_asubject_latest(self, mock_registry: SchemaRegistry) -> None:
        """Test asubject_latest method."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "subject": "test-subject",
            "version": 1,
            "id": 123,
            "schema": "{}",
        }
        mock_response.raise_for_status = MagicMock()
        mock_registry._client.get = AsyncMock(return_value=mock_response)
        
        subject = Subject("test-subject")
        result = await mock_registry.asubject_latest((subject,))
        
        assert mock_registry._client.get.called
        call_args = mock_registry._client.get.call_args
        url_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
        # The URL contains the tuple representation
        assert "test-subject" in url_arg

    async def test_asubjects_delete(self, mock_registry: SchemaRegistry) -> None:
        """Test asubjects_delete method."""
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_registry._client.delete = AsyncMock(return_value=mock_response)
        
        subject = Subject("test-subject")
        await mock_registry.asubjects_delete(subject, version=1)
        
        assert mock_registry._client.delete.called
        call_args = mock_registry._client.delete.call_args
        url_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
        assert "subjects/test-subject/versions/1" in url_arg

    async def test_asubjects_create(self, mock_registry: SchemaRegistry) -> None:
        """Test asubjects_create method."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 123}
        mock_response.raise_for_status = MagicMock()
        mock_registry._client.post = AsyncMock(return_value=mock_response)
        
        subject = Subject("test-subject")
        data: SubjectNew = {
            "schema": "{}",
            "schemaType": "AVRO",
        }
        await mock_registry.asubjects_create(subject, data)
        
        assert mock_registry._client.post.called
        call_args = mock_registry._client.post.call_args
        url_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
        assert "subjects/test-subject" in url_arg

    async def test_aschema_versions(self, mock_registry: SchemaRegistry) -> None:
        """Test aschema_versions method."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"schema": {"versions": [1, 2, 3]}}
        mock_response.raise_for_status = MagicMock()
        mock_registry._client.get = AsyncMock(return_value=mock_response)
        
        subject = Subject("test-subject")
        await mock_registry.aschema_versions(subject)
        
        assert mock_registry._client.get.called
        call_args = mock_registry._client.get.call_args
        url_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
        assert "subjects/test-subject/versions" in url_arg


class TestSubjectTableValues:
    """Tests for to_table_values method."""

    def test_subject_to_table_values(self) -> None:
        """Test Subject.to_table_values method."""
        subject = Subject("test-subject")
        result = subject.to_table_values()
        assert result == ("test-subject",)


class TestMetaEnum:
    """Tests for MetaEnum."""

    def test_metaenum_case_insensitive(self) -> None:
        """Test that MetaEnum allows case-insensitive access."""
        assert SchemaTypes("avro") == SchemaTypes.AVRO
        assert SchemaTypes("AvRo") == SchemaTypes.AVRO
        assert SchemaTypes("PROTOBUF") == SchemaTypes.PROTOBUF
