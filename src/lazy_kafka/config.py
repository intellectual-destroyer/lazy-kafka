"""Handle configuration."""

from __future__ import annotations

import json
import logging
import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from _typeshed import SupportsRead

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaConfiguration:
    """Kafka Consumer/Producer configuration.

    Attributes:
        Configurations are mapped from [librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md),
        with the convention, that `.` (dot) are mapped to `_` underscores. Use `to_config` method to dump it
        into raw format.
    """

    bootstrap_servers: str = "localhost:9092"
    group_id: str = "my-work-group"
    auto_offset_reset: str = "earliest"
    security_protocol: str = "plaintext"
    enable_auto_commit: str = "false"
    sasl_username: str = ""
    sasl_password: str = ""
    sasl_mechanism: str = ""
    logger: logging.Handler | None = None
    # Note: log_queue has to go together with the logger param, so spotaneous logging from
    #   (non-python) threads is avoided.
    log_queue: bool = True

    def to_config(self, flavor: str = "librdkafka") -> dict[str, Any]:
        match flavor:
            case "librdkafka" | "confluent":
                config = self.__dict__
                # in confluent-kafka 2.5.0 it seems to be necessary to remove
                # sasl.mechanism if it is not used
                if config.get("sasl_mechanism") == "":
                    config.pop("sasl_mechanism", None)
                config = {k.replace("_", "."): v for k, v in config.items()}
            case _:
                raise ValueError("Unsupported configuration flavor.")
        return config


@dataclass(frozen=True)
class ConnectConfiguration:
    host: str = "http://localhost:8083"
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True)
class RegistryConfiguration:
    host: str = "http://localhost:8081"
    username: str | None = None
    password: str | None = None

    def to_config(self, flavor: str = "librdkafka") -> dict[str, str]:
        match flavor:
            case "librdkafka" | "confluent":
                username = self.username or ""
                password = self.password or ""
                return {
                    "url": self.host,
                    "basic.auth.user.info": f"{username}:{password}",
                }
            case _:
                raise ValueError("Unsupported configuration flavor.")


# TODO: @from_file(Path.home() / ".lazy-kafka.toml")
@dataclass(frozen=True)
class Configuration:
    """Lazy-kafka configuration."""

    kafka: KafkaConfiguration = field(default_factory=KafkaConfiguration)
    registry: RegistryConfiguration = field(default_factory=RegistryConfiguration)
    connect: ConnectConfiguration = field(default_factory=ConnectConfiguration)
    request_time_out: int = 1000
    log_level: Literal["DEBUG", "INFO"] = "INFO"
    # Run the app in development mode: logging, log-formatting etc.
    dev_mode: bool = False
    _file: Path = Path(__file__).parent / "default_config.toml"

    def __post_init__(self):
        if not isinstance(self.kafka, KafkaConfiguration):
            object.__setattr__(self, "kafka", KafkaConfiguration(**self.kafka))

        if not isinstance(self.connect, ConnectConfiguration):
            object.__setattr__(self, "connect", ConnectConfiguration(**self.connect))

        if not isinstance(self.registry, RegistryConfiguration):
            object.__setattr__(self, "registry", RegistryConfiguration(**self.registry))

    @classmethod
    def from_file(
        cls, file_path: Path, file_reader: Callable[[SupportsRead], Any], **kwargs: Any
    ):
        with open(file_path, **kwargs) as _f:
            config = file_reader(_f)
        return cls(**config, _file=file_path)

    @classmethod
    def from_toml(cls, file_path: Path):
        return cls.from_file(file_path, tomllib.load, mode="rb")

    @classmethod
    def from_json(cls, file_path: Path):
        return cls.from_file(file_path, json.load)

    @classmethod
    def default_config_file_path(cls):
        _p = Path.home() / ".config" / ".lazy-kafka.toml"
        _LOGGER.debug("Configfile path: %s", _p)
        return _p

    @classmethod
    def from_local_config(cls):
        # NOTE XDG_HOME is ignored
        try:
            return cls.from_toml(cls.default_config_file_path())
        except FileNotFoundError as exc:
            _LOGGER.error("No user configuration file.", exc_info=exc)
            return cls()


if __name__ == "__main__":
    from rich.pretty import pprint

    _p = Path(__file__).parent / "default_config.toml"
    c = Configuration.from_toml(_p)
    pprint(c)
    print(c.kafka)
    print(type(c.kafka))
    print(KafkaConfiguration())
    print(type(KafkaConfiguration()))
    print(c.connect)
    print(c.registry)
