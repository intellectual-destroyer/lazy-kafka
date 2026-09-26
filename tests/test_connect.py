"""Tests for Kafka Connect functionality."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from lazy_kafka.config import ConnectConfiguration
from lazy_kafka.connect import ConnectionRefusedError, ConnectorData, Connect


class TestConnectorData:
    """Tests for ConnectorData dataclass."""

    def test_default_values(self) -> None:
        """Test ConnectorData with default values."""
        data = ConnectorData()
        assert data.name is None
        assert data.state is None
        assert data.worker_id is None
        assert data.type is None

    def test_custom_values(self) -> None:
        """Test ConnectorData with custom values."""
        data = ConnectorData(
            name="test-connector",
            state="RUNNING",
            worker_id="worker-1",
            type="sink",
        )
        assert data.name == "test-connector"
        assert data.state == "RUNNING"
        assert data.worker_id == "worker-1"
        assert data.type == "sink"

    def test_to_dict(self) -> None:
        """Test ConnectorData.to_dict method."""
        data = ConnectorData(
            name="test-connector",
            state="RUNNING",
            worker_id="worker-1",
            type="sink",
        )
        result = data.to_dict()
        assert result == {
            "name": "test-connector",
            "state": "RUNNING",
            "worker_id": "worker-1",
            "type": "sink",
        }

    def test_to_tuple(self) -> None:
        """Test ConnectorData.to_tuple method."""
        data = ConnectorData(
            name="test-connector",
            state="RUNNING",
            worker_id="worker-1",
            type="sink",
        )
        result = data.to_tuple()
        assert result == ("test-connector", "RUNNING", "worker-1", "sink")

    def test_to_table_values(self) -> None:
        """Test ConnectorData.to_table_values method."""
        data = ConnectorData(
            name="test-connector",
            state="RUNNING",
            worker_id="worker-1",
            type="sink",
        )
        result = list(data.to_table_values())
        assert result == ["test-connector", "RUNNING", "worker-1", "sink"]

    def test_iter(self) -> None:
        """Test ConnectorData iteration."""
        data = ConnectorData(
            name="test-connector",
            state="RUNNING",
            worker_id="worker-1",
            type="sink",
        )
        result = list(data)
        assert result == ["test-connector", "RUNNING", "worker-1", "sink"]

    def test_from_response(self) -> None:
        """Test ConnectorData.from_response method."""
        response = {
            "test-connector": {
                "status": {
                    "connector": {
                        "state": "RUNNING",
                        "worker_id": "worker-1",
                    },
                    "type": "sink",
                }
            }
        }
        result = ConnectorData.from_response(response)
        
        assert len(result) == 1
        assert "test-connector" in result
        assert result["test-connector"].name == "test-connector"
        assert result["test-connector"].state == "RUNNING"
        assert result["test-connector"].worker_id == "worker-1"
        assert result["test-connector"].type == "sink"


class TestConnect:
    """Tests for Connect class."""

    def test_init_default(self, mock_httpx_get: MagicMock) -> None:
        """Test Connect initialization with default config."""
        config = ConnectConfiguration()
        connect = Connect(config)
        assert connect.host == "http://localhost:8083"

    def test_init_custom_host(self, mock_httpx_get: MagicMock) -> None:
        """Test Connect initialization with custom host."""
        config = ConnectConfiguration(host="http://custom:8083")
        connect = Connect(config)
        assert connect.host == "http://custom:8083"

    def test_init_with_auth(self, mock_httpx_get: MagicMock) -> None:
        """Test Connect initialization with authentication."""
        config = ConnectConfiguration(
            host="http://localhost:8083",
            username="user",
            password="pass",
        )
        connect = Connect(config)
        assert connect.host == "http://localhost:8083"

    def test_repr(self, mock_httpx_get: MagicMock) -> None:
        """Test Connect string representation."""
        config = ConnectConfiguration(host="http://test:8083")
        connect = Connect(config)
        assert repr(connect) == "Connect@http://test:8083"

    def test_connection_error(self) -> None:
        """Test that connection error is raised when service is unavailable."""
        with patch("httpx.get") as mock_get:
            mock_get.side_effect = httpx.HTTPError("Connection refused")
            config = ConnectConfiguration(host="http://unavailable:8083")
            
            with pytest.raises(ConnectionRefusedError):
                Connect(config)

    def test_non_200_status(self) -> None:
        """Test that non-200 status raises assertion error."""
        with patch("httpx.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            config = ConnectConfiguration(host="http://localhost:8083")
            
            with pytest.raises(AssertionError):
                Connect(config)


class TestConnectAsync:
    """Tests for async methods of Connect."""

    @pytest.fixture
    def mock_connect(self, mock_httpx_get: MagicMock) -> Connect:
        """Create a Connect instance with mocked _client for async tests."""
        from lazy_kafka.connect import Connect
        config = ConnectConfiguration(host="http://test:8083")
        connect = Connect.__new__(Connect)
        connect.host = "http://test:8083"
        # Create a mock AsyncClient
        mock_async_client = AsyncMock()
        connect._client = mock_async_client
        return connect

    async def test_connectors(self, mock_connect: Connect) -> None:
        """Test connectors method to get all connectors."""
        # Set up mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "test-connector-1": {
                "status": {
                    "connector": {"state": "RUNNING", "worker_id": "worker-1"},
                    "type": "sink",
                }
            },
            "test-connector-2": {
                "status": {
                    "connector": {"state": "FAILED", "worker_id": "worker-2"},
                    "type": "source",
                }
            },
        }
        mock_connect._client.get.return_value = mock_response
        
        result = await mock_connect.connectors()
        
        assert mock_connect._client.get.called
        assert len(result) == 2
        assert "test-connector-1" in result
        assert "test-connector-2" in result

    async def test_connector(self, mock_connect: Connect) -> None:
        """Test connector method to get specific connector."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "name": "test-connector",
            "status": {
                "connector": {"state": "RUNNING", "worker_id": "worker-1"},
                "type": "sink",
            },
        }
        mock_response.raise_for_status = AsyncMock()
        mock_connect._client.get.return_value = mock_response
        
        result = await mock_connect.connector("test-connector")
        
        assert mock_connect._client.get.called
        call_args = mock_connect._client.get.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector" in call_args.args[0]

    async def test_connector_status(self, mock_connect: Connect) -> None:
        """Test connector_status method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "connector": {"state": "RUNNING", "worker_id": "worker-1"},
            "tasks": [],
            "type": "sink",
        }
        mock_response.raise_for_status = AsyncMock()
        mock_connect._client.get.return_value = mock_response
        
        result = await mock_connect.connector_status("test-connector")
        
        assert mock_connect._client.get.called
        call_args = mock_connect._client.get.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector/status" in call_args.args[0]

    async def test_connector_create(self, mock_connect: Connect) -> None:
        """Test connector_create method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {"name": "test-connector"}
        mock_response.raise_for_status = AsyncMock()
        mock_connect._client.post.return_value = mock_response
        
        # connector_create expects name and config as separate parameters
        config = {"config": {}}
        result = await mock_connect.connector_create("test-connector", config)
        
        assert mock_connect._client.post.called
        call_args = mock_connect._client.post.call_args
        # URL is passed as first positional argument
        assert "connectors" in call_args.args[0]

    async def test_connector_update(self, mock_connect: Connect) -> None:
        """Test connector_update method."""
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_connect._client.put.return_value = mock_response
        
        config = {"config": {}}
        await mock_connect.connector_update("test-connector", config)
        
        assert mock_connect._client.put.called
        call_args = mock_connect._client.put.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector/config" in call_args.args[0]

    async def test_connector_delete(self, mock_connect: Connect) -> None:
        """Test connector_delete method."""
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_connect._client.delete.return_value = mock_response
        
        await mock_connect.connector_delete("test-connector")
        
        assert mock_connect._client.delete.called
        call_args = mock_connect._client.delete.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector" in call_args.args[0]

    async def test_connector_pause(self, mock_connect: Connect) -> None:
        """Test connector_pause method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = AsyncMock()
        # aconnector_pause uses PUT, not POST
        mock_connect._client.put.return_value = mock_response
        
        await mock_connect.connector_pause("test-connector")
        
        assert mock_connect._client.put.called
        call_args = mock_connect._client.put.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector/pause" in call_args.args[0]

    async def test_connector_resume(self, mock_connect: Connect) -> None:
        """Test connector_resume method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = AsyncMock()
        # aconnector_resume uses PUT, not POST
        mock_connect._client.put.return_value = mock_response
        
        await mock_connect.connector_resume("test-connector")
        
        assert mock_connect._client.put.called
        call_args = mock_connect._client.put.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector/resume" in call_args.args[0]

    async def test_connector_restart(self, mock_connect: Connect) -> None:
        """Test connector_restart method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = AsyncMock()
        mock_connect._client.post.return_value = mock_response
        
        await mock_connect.connector_restart("test-connector")
        
        assert mock_connect._client.post.called
        call_args = mock_connect._client.post.call_args
        # URL is passed as first positional argument
        assert "connectors/test-connector/restart" in call_args.args[0]
