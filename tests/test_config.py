"""Tests for configuration management."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from lazy_kafka.config import Configuration, ConnectConfiguration, KafkaConfiguration, RegistryConfiguration


class TestKafkaConfiguration:
    """Tests for KafkaConfiguration."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = KafkaConfiguration()
        assert config.bootstrap_servers == "localhost:9092"
        assert config.group_id == "my-work-group"
        assert config.auto_offset_reset == "earliest"
        assert config.security_protocol == "plaintext"
        assert config.enable_auto_commit == "false"
        assert config.sasl_username == ""
        assert config.sasl_password == ""
        assert config.sasl_mechanism == ""

    def test_custom_values(self) -> None:
        """Test that custom values can be set."""
        config = KafkaConfiguration(
            bootstrap_servers="custom:9092",
            group_id="custom-group",
            auto_offset_reset="latest",
        )
        assert config.bootstrap_servers == "custom:9092"
        assert config.group_id == "custom-group"
        assert config.auto_offset_reset == "latest"

    def test_to_config_librdkafka(self) -> None:
        """Test conversion to librdkafka format."""
        config = KafkaConfiguration(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            auto_offset_reset="earliest",
        )
        result = config.to_config(flavor="librdkafka")
        assert result["bootstrap.servers"] == "localhost:9092"
        assert result["group.id"] == "test-group"
        assert result["auto.offset.reset"] == "earliest"

    def test_to_config_confluent(self) -> None:
        """Test conversion to confluent format."""
        config = KafkaConfiguration(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        result = config.to_config(flavor="confluent")
        assert result["bootstrap.servers"] == "localhost:9092"
        assert result["group.id"] == "test-group"

    def test_to_config_empty_sasl_mechanism(self) -> None:
        """Test that empty sasl_mechanism is removed from config."""
        config = KafkaConfiguration(sasl_mechanism="")
        result = config.to_config(flavor="librdkafka")
        assert "sasl.mechanism" not in result

    def test_to_config_invalid_flavor(self) -> None:
        """Test that invalid flavor raises ValueError."""
        config = KafkaConfiguration()
        with pytest.raises(ValueError, match="Unsupported configuration flavor"):
            config.to_config(flavor="invalid")

    def test_frozen_dataclass(self) -> None:
        """Test that KafkaConfiguration is immutable."""
        config = KafkaConfiguration()
        with pytest.raises(AttributeError):
            config.bootstrap_servers = "new-value"  # type: ignore[attr-defined]


class TestRegistryConfiguration:
    """Tests for RegistryConfiguration."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = RegistryConfiguration()
        assert config.host == "http://localhost:8081"
        assert config.username is None
        assert config.password is None

    def test_custom_values(self) -> None:
        """Test that custom values can be set."""
        config = RegistryConfiguration(
            host="http://custom:8081",
            username="user",
            password="pass",
        )
        assert config.host == "http://custom:8081"
        assert config.username == "user"
        assert config.password == "pass"

    def test_to_config_librdkafka(self) -> None:
        """Test conversion to librdkafka format."""
        config = RegistryConfiguration(
            host="http://localhost:8081",
            username="user",
            password="pass",
        )
        result = config.to_config(flavor="librdkafka")
        assert result["url"] == "http://localhost:8081"
        assert result["basic.auth.user.info"] == "user:pass"

    def test_to_config_without_auth(self) -> None:
        """Test conversion without authentication."""
        config = RegistryConfiguration(host="http://localhost:8081")
        result = config.to_config(flavor="librdkafka")
        assert result["url"] == "http://localhost:8081"
        assert result["basic.auth.user.info"] == ":"


class TestConnectConfiguration:
    """Tests for ConnectConfiguration."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = ConnectConfiguration()
        assert config.host == "http://localhost:8083"
        assert config.username is None
        assert config.password is None

    def test_custom_values(self) -> None:
        """Test that custom values can be set."""
        config = ConnectConfiguration(
            host="http://custom:8083",
            username="user",
            password="pass",
        )
        assert config.host == "http://custom:8083"
        assert config.username == "user"
        assert config.password == "pass"


class TestConfiguration:
    """Tests for Configuration."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = Configuration()
        assert isinstance(config.kafka, KafkaConfiguration)
        assert isinstance(config.registry, RegistryConfiguration)
        assert isinstance(config.connect, ConnectConfiguration)
        assert config.request_time_out == 1000
        assert config.log_level == "INFO"
        assert config.dev_mode is False

    def test_custom_nested_configs(self) -> None:
        """Test that custom nested configurations are properly converted."""
        config = Configuration(
            kafka={"bootstrap_servers": "custom:9092"},
            registry={"host": "http://custom:8081"},
            connect={"host": "http://custom:8083"},
        )
        assert isinstance(config.kafka, KafkaConfiguration)
        assert config.kafka.bootstrap_servers == "custom:9092"
        assert isinstance(config.registry, RegistryConfiguration)
        assert config.registry.host == "http://custom:8081"
        assert isinstance(config.connect, ConnectConfiguration)
        assert config.connect.host == "http://custom:8083"

    def test_from_toml(self, temp_config_file: Path) -> None:
        """Test loading configuration from TOML file."""
        config = Configuration.from_toml(temp_config_file)
        assert config.kafka.bootstrap_servers == "localhost:9092"
        assert config.kafka.group_id == "test-group"
        assert config.registry.host == "http://localhost:8081"
        assert config.connect.host == "http://localhost:8083"

    def test_from_json(self, temp_json_config: Path) -> None:
        """Test loading configuration from JSON file."""
        config = Configuration.from_json(temp_json_config)
        assert config.kafka.bootstrap_servers == "localhost:9092"
        assert config.kafka.group_id == "test-group"
        assert config.registry.host == "http://localhost:8081"
        assert config.connect.host == "http://localhost:8083"

    def test_from_file_invalid_path(self) -> None:
        """Test that loading from non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            Configuration.from_toml(Path("/nonexistent/path/config.toml"))

    def test_default_config_file_path(self) -> None:
        """Test the default configuration file path."""
        path = Configuration.default_config_file_path()
        assert path == Path.home() / ".config" / ".lazy-kafka.toml"

    def test_from_local_config_no_file(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading from local config when file doesn't exist."""
        def mock_default_path() -> Path:
            return Path("/nonexistent/path/config.toml")
        
        monkeypatch.setattr(Configuration, "default_config_file_path", mock_default_path)
        config = Configuration.from_local_config()
        assert isinstance(config, Configuration)

    def test_post_init_converts_dicts(self) -> None:
        """Test that dicts are converted to proper dataclass instances in __post_init__."""
        config = Configuration(
            kafka={"bootstrap_servers": "test:9092", "group_id": "test-group"},
            registry={"host": "http://test:8081"},
            connect={"host": "http://test:8083"},
        )
        assert isinstance(config.kafka, KafkaConfiguration)
        assert isinstance(config.registry, RegistryConfiguration)
        assert isinstance(config.connect, ConnectConfiguration)
        assert config.kafka.bootstrap_servers == "test:9092"
        assert config.registry.host == "http://test:8081"
        assert config.connect.host == "http://test:8083"


class TestConfigurationImmutability:
    """Tests for configuration immutability."""

    def test_configuration_is_frozen(self) -> None:
        """Test that Configuration is immutable."""
        config = Configuration()
        with pytest.raises(AttributeError):
            config.kafka = KafkaConfiguration()  # type: ignore[attr-defined]

    def test_nested_configs_are_frozen(self) -> None:
        """Test that nested configurations are immutable."""
        config = Configuration()
        with pytest.raises(AttributeError):
            config.kafka.bootstrap_servers = "new-value"  # type: ignore[attr-defined]
