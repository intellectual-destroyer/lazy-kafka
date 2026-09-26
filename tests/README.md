# Tests for lazy-kafka

This directory contains a comprehensive test suite for the lazy-kafka project. The tests follow best practices to ensure high quality while avoiding code duplication.

## Test Structure

```
tests/
├── __init__.py          # Test package initialization
├── conftest.py          # Common fixtures and test configuration
├── pytest.ini           # Pytest configuration
├── README.md            # This file
├── test_config.py       # Configuration management tests
├── test_connect.py      # Kafka Connect functionality tests
├── test_cli.py          # CLI functionality tests
├── test_plugin.py       # Plugin system tests
├── test_registry.py     # Schema registry tests
├── test_topic.py        # Kafka topic functionality tests
├── test_utils.py        # Utility function tests
└── test_widgets.py      # Textual widget tests
```

## Running Tests

### Basic Test Execution

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/lazy_kafka --cov-report=term-missing

# Run specific test file
pytest tests/test_config.py

# Run specific test
pytest tests/test_config.py::TestKafkaConfiguration::test_default_values
```

### Using Hatch

```bash
# Run tests using hatch (as configured in pyproject.toml)
hatch run test

# Run tests with coverage
hatch run test:test
```

## Test Categories

### Unit Tests
- **Configuration** (`test_config.py`): Tests for all configuration classes
- **Utilities** (`test_utils.py`): Tests for utility functions
- **Data Classes** (`test_registry.py`, `test_connect.py`, `test_topic.py`): Tests for data structures

### Integration Tests
- **CLI** (`test_cli.py`): Tests for command-line interface
- **Plugin System** (`test_plugin.py`): Tests for plugin registration and loading
- **Widgets** (`test_widgets.py`): Tests for Textual widgets

### Mocking Strategy

The tests use `unittest.mock` to prevent actual network calls and external dependencies:

- **HTTP Calls**: All `httpx` calls are mocked to prevent network requests
- **Kafka**: All Kafka consumer/producer calls are mocked
- **Async**: Async methods are tested using `AsyncMock`

## Fixtures

Common fixtures are defined in `conftest.py`:

- **Configuration Fixtures**: `kafka_config`, `registry_config`, `connect_config`, `full_config`
- **File Fixtures**: `temp_config_file`, `temp_json_config`
- **HTTP Fixtures**: `mock_http_client`, `mock_httpx_get`, `mock_httpx_async_get`
- **Kafka Fixtures**: `mock_consumer`, `mock_producer`
- **Network Prevention**: `prevent_network_calls` (autouse fixture)

## Best Practices

1. **Avoid Code Duplication**: Common setup is extracted into fixtures
2. **Test Isolation**: Each test is independent and uses fresh fixtures
3. **Fast Execution**: Tests avoid slow operations (no actual network calls)
4. **Clear Naming**: Test names follow `test_<description>` pattern
5. **Documentation**: Each test has a docstring explaining its purpose
6. **Assertions**: Use descriptive assertions with helpful messages

## Test Coverage

The tests cover:

- ✅ Configuration loading and validation
- ✅ Schema registry operations (sync and async)
- ✅ Kafka Connect operations (sync and async)
- ✅ Kafka topic operations and message handling
- ✅ Plugin system and registration
- ✅ CLI command structure
- ✅ Textual widget initialization
- ✅ Utility functions

## Adding New Tests

1. Create a new test file following the naming pattern: `test_<module>.py`
2. Use existing fixtures from `conftest.py` when possible
3. Follow the same structure: test classes with descriptive names
4. Add appropriate type hints
5. Keep tests focused and fast

## Example Test Structure

```python
"""Tests for <module>."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lazy_kafka.<module> import <Class>


class TestClass:
    """Tests for <Class>."""

    def test_method_1(self) -> None:
        """Test that method_1 does something."""
        # Setup
        obj = <Class>()
        
        # Action
        result = obj.method_1()
        
        # Assertion
        assert result == expected_value

    @patch("module.external_call")
    def test_method_2_with_mock(self, mock_external: MagicMock) -> None:
        """Test method_2 with mocked external call."""
        mock_external.return_value = "mocked"
        
        obj = <Class>()
        result = obj.method_2()
        
        assert mock_external.called
        assert result == "expected"
```

## Performance Considerations

- Tests should run quickly (no actual network/IO)
- Async tests are properly mocked
- Fixtures are reused across tests
- Slow tests are marked with `@pytest.mark.slow`

## Quality Standards

- All tests must pass before merging
- Test coverage is tracked but not enforced at 100% (focus on meaningful tests)
- Tests should be maintainable and readable
- Avoid testing implementation details, focus on behavior
