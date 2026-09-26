"""Pytest configuration and fixtures for lazy-kafka tests."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Generator
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest

from lazy_kafka.config import Configuration, ConnectConfiguration, KafkaConfiguration, RegistryConfiguration


# Fixtures for configuration
@pytest.fixture
def kafka_config() -> KafkaConfiguration:
    """Provide a default KafkaConfiguration for testing."""
    return KafkaConfiguration(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        auto_offset_reset="earliest",
    )


@pytest.fixture
def registry_config() -> RegistryConfiguration:
    """Provide a default RegistryConfiguration for testing."""
    return RegistryConfiguration(host="http://localhost:8081")


@pytest.fixture
def connect_config() -> ConnectConfiguration:
    """Provide a default ConnectConfiguration for testing."""
    return ConnectConfiguration(host="http://localhost:8083")


@pytest.fixture
def full_config(kafka_config: KafkaConfiguration, registry_config: RegistryConfiguration, connect_config: ConnectConfiguration) -> Configuration:
    """Provide a full Configuration for testing."""
    return Configuration(
        kafka=kafka_config,
        registry=registry_config,
        connect=connect_config,
    )


@pytest.fixture
def temp_config_file() -> Generator[Path, None, None]:
    """Create a temporary configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write("""
[kafka]
bootstrap_servers = "localhost:9092"
group_id = "test-group"
auto_offset_reset = "earliest"

[registry]
host = "http://localhost:8081"

[connect]
host = "http://localhost:8083"
""")
        f.flush()
        yield Path(f.name)
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def temp_json_config() -> Generator[Path, None, None]:
    """Create a temporary JSON configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "group_id": "test-group",
            },
            "registry": {
                "host": "http://localhost:8081",
            },
            "connect": {
                "host": "http://localhost:8083",
            },
        }, f)
        f.flush()
        yield Path(f.name)
    Path(f.name).unlink(missing_ok=True)


# Fixtures for HTTP mocking
@pytest.fixture
def mock_http_client() -> Generator[AsyncMock, None, None]:
    """Provide a mock async HTTP client."""
    with patch("httpx.AsyncClient") as mock_client:
        mock_instance = AsyncMock()
        mock_client.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_httpx_get() -> Generator[MagicMock, None, None]:
    """Provide a mock for httpx.get."""
    with patch("httpx.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response
        yield mock_get


@pytest.fixture
def mock_httpx_async_get() -> Generator[AsyncMock, None, None]:
    """Provide a mock for httpx.AsyncClient.get."""
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.raise_for_status = AsyncMock()
        mock_get.return_value = mock_response
        yield mock_get


# Fixtures for Kafka mocking
@pytest.fixture
def mock_consumer() -> Generator[MagicMock, None, None]:
    """Provide a mock Kafka consumer."""
    mock = MagicMock()
    mock.list_topics.return_value = (None, ["test-topic"])
    mock.get_watermark_offsets.return_value = (0, 10)
    mock.poll.return_value = None
    yield mock


@pytest.fixture
def mock_producer() -> Generator[MagicMock, None, None]:
    """Provide a mock Kafka producer."""
    mock = MagicMock()
    yield mock


# Autouse fixture to prevent actual network calls
@pytest.fixture(autouse=True)
def prevent_network_calls(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Prevent actual network calls during tests."""
    def mock_get(*args: Any, **kwargs: Any) -> Mock:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        return mock_response
    
    def mock_async_get(*args: Any, **kwargs: Any) -> AsyncMock:
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.raise_for_status = AsyncMock()
        return mock_response
    
    monkeypatch.setattr("httpx.get", mock_get)
    monkeypatch.setattr("httpx.AsyncClient.get", mock_async_get)
    monkeypatch.setattr("httpx.AsyncClient.post", mock_async_get)
    monkeypatch.setattr("httpx.AsyncClient.delete", mock_async_get)
    yield
