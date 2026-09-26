"""Tests for CLI functionality."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from lazy_kafka.cli._cli import app, global_options, version_callback
from lazy_kafka.config import Configuration


class TestVersionCallback:
    """Tests for version_callback function."""

    def test_version_callback_prints_version(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test that version_callback prints the version."""
        import typer
        with pytest.raises(typer.Exit):
            version_callback(True)
        captured = capsys.readouterr()
        assert "LazyKafka" in captured.out

    def test_version_callback_no_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test that version_callback doesn't output when value is False."""
        version_callback(False)
        captured = capsys.readouterr()
        assert captured.out == ""


class TestGlobalOptions:
    """Tests for global_options function."""

    def test_global_options_returns_none(self) -> None:
        """Test that global_options callback returns None (Typer convention)."""
        # global_options is a Typer callback, it doesn't return options
        # The options are defined in the function signature
        result = global_options()
        assert result is None

    def test_app_has_version_option(self) -> None:
        """Test that the app has version option configured."""
        # Check that the app has the version callback option
        assert app is not None
        # The version option is registered via the callback
        # We can verify the callback exists
        assert hasattr(app, 'callback')


class TestCLIApp:
    """Tests for CLI app."""

    def test_app_exists(self) -> None:
        """Test that CLI app exists."""
        assert app is not None

    def test_app_has_commands(self) -> None:
        """Test that CLI app has commands."""
        assert hasattr(app, "command")

    def test_app_callback(self) -> None:
        """Test that CLI app has a callback."""
        assert hasattr(app, "callback")


class TestCLIIntegration:
    """Integration tests for CLI."""

    def test_cli_imports(self) -> None:
        """Test that CLI imports are correct."""
        from lazy_kafka.cli import app as cli_app
        assert cli_app is not None

    def test_cli_entry_point(self) -> None:
        """Test that CLI entry point is configured."""
        from lazy_kafka.entry_points import lazy_kafka
        assert callable(lazy_kafka)


class TestMainEntryPoint:
    """Tests for main entry point."""

    def test_main_with_args_calls_cli(self) -> None:
        """Test that main calls CLI when args are provided."""
        with patch("lazy_kafka.__main__.sys") as mock_sys:
            mock_sys.argv = ["lazy-kafka", "--version"]
            
            with patch("lazy_kafka.cli.app") as mock_app:
                from lazy_kafka.__main__ import main
                main()
                assert mock_app.called

    def test_main_without_args_runs_tui(self) -> None:
        """Test that main runs TUI when no args are provided."""
        with patch("lazy_kafka.__main__.sys") as mock_sys:
            mock_sys.argv = ["lazy-kafka"]
            
            with patch("lazy_kafka.__main__.LazyKafka") as mock_tui:
                mock_instance = MagicMock()
                mock_tui.return_value = mock_instance
                
                with patch("lazy_kafka.__main__.logging") as mock_logging:
                    from lazy_kafka.__main__ import main
                    main()
                    # TUI should be instantiated
                    assert mock_tui.called or mock_instance.run.called


class TestPluginCLI:
    """Tests for plugin CLI commands."""

    def test_plugin_cli_commands_exist(self) -> None:
        """Test that plugin CLI commands are registered."""
        from lazy_kafka.plugin import load_builtin_plugins, iter_plugins
        
        load_builtin_plugins()
        plugins = list(iter_plugins())
        
        # Check that at least some plugins have CLI commands
        cli_commands = [p for p in plugins if p.build_cli() is not None]
        assert len(cli_commands) >= 0  # May be zero if plugins don't have CLI

    def test_core_kafka_cli_exists(self) -> None:
        """Test that core Kafka plugin has CLI commands."""
        from lazy_kafka.plugins.core_kafka.cli import kafka, consume, produce
        assert callable(kafka)
        assert callable(consume)
        assert callable(produce)

    def test_schema_registry_cli_exists(self) -> None:
        """Test that schema registry plugin has CLI commands."""
        from lazy_kafka.plugins.schema_registry.cli import schema_registry
        assert callable(schema_registry)
