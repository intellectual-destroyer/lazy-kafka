"""Logs widget for displaying application logs."""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING, Deque

from textual import messages
from textual.binding import Binding
from textual.containers import Container
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label

if TYPE_CHECKING:
    from textual.app import App

_LOGGER = logging.getLogger(__name__)

# Log level to color mapping for styling
LOG_LEVEL_COLORS = {
    "DEBUG": "$text-disabled",
    "INFO": "$text",
    "WARNING": "$warning",
    "ERROR": "$error",
    "CRITICAL": "bold $error",
}


class LogEntry:
    """Represents a single log entry."""

    def __init__(
        self,
        level: str,
        logger_name: str,
        message: str,
        timestamp: datetime | None = None,
        filename: str | None = None,
        lineno: int | None = None,
    ):
        self.level = level
        self.logger_name = logger_name
        self.message = message
        self.timestamp = timestamp or datetime.now()
        self.filename = filename
        self.lineno = lineno

    def to_tuple(self) -> tuple[str, str, str, str]:
        """Convert to tuple for display in DataTable."""
        time_str = self.timestamp.strftime("%H:%M:%S.%f")[:-3]
        return (time_str, self.level, self.logger_name, self.message)

    def get_color(self) -> str:
        """Get the CSS color for this log level."""
        return LOG_LEVEL_COLORS.get(self.level, "$text")


class LogHandler(logging.Handler):
    """Custom logging handler that stores log entries for display in the UI."""

    LOG_LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __init__(self, max_entries: int = 1000):
        super().__init__()
        self.max_entries: int = max_entries
        self.entries: Deque[LogEntry] = deque(maxlen=max_entries)
        self._min_level: str = "DEBUG"
        self._min_level_numeric: int = logging.DEBUG

    def emit(self, record: logging.LogRecord) -> None:
        """Handle a log record."""
        # Skip records below the minimum level
        if record.levelno < self._min_level_numeric:
            return

        entry = LogEntry(
            level=record.levelname,
            logger_name=record.name,
            message=record.getMessage(),
            timestamp=datetime.fromtimestamp(record.created),
            filename=record.filename,
            lineno=record.lineno,
        )
        self.entries.append(entry)

    def set_min_level(self, level: str) -> None:
        """Set the minimum log level to display."""
        level_upper = level.upper()
        if level_upper in self.LOG_LEVELS:
            self._min_level = level_upper
            self._min_level_numeric = self.LOG_LEVELS[level_upper]
        else:
            _LOGGER.warning("Unknown log level: %s, using INFO", level)
            self._min_level = "INFO"
            self._min_level_numeric = logging.INFO

    @property
    def min_level(self) -> str:
        """Get the current minimum log level."""
        return self._min_level

    def get_entries(self) -> list[LogEntry]:
        """Get all log entries as a list."""
        return list(self.entries)

    def clear(self) -> None:
        """Clear all log entries."""
        self.entries.clear()

    def set_max_entries(self, max_entries: int) -> None:
        """Set the maximum number of entries to keep."""
        self.max_entries = max_entries
        # Recreate deque with new maxlen
        old_entries = list(self.entries)
        self.entries = deque(old_entries[-max_entries:], maxlen=max_entries)


class LogsWidget(Container, can_focus=True):
    """Widget for displaying logs with filtering and level control."""

    BINDINGS = [
        Binding("c", "clear_logs", "Clear logs"),
        Binding("up", "increase_level", "Increase log level"),
        Binding("down", "decrease_level", "Decrease log level"),
        Binding("b", "scroll_bottom", "Scroll to bottom"),
        Binding("t", "scroll_top", "Scroll to top"),
    ]

    DEFAULT_CSS = """
    LogsWidget {
        layout: vertical;
        height: 100%;
        width: 100%;
        
        &:focus {
            border: none;
        }
        
        Container {
            layout: horizontal;
            height: auto;
            width: 100%;
            padding: 0 1;
        }
        
        #logs-title {
            text-style: bold;
            color: $primary;
        }
        
        #level-label {
            padding-left: 2;
        }
        
        DataTable {
            width: 100%;
            height: 1fr;
            background: $surface;
            
            & > .datatable--header {
                background: transparent;
                color: $primary;
                text-style: bold;
            }
            
            & > .datatable--odd-row {
                background: $surface-lighten-1;
            }
            
            & > .datatable--even-row {
                background: $background;
            }
            
            & > .datatable--cursor-row {
                background: $primary-lighten-3;
            }
            
            .level-debug {
                color: $text-disabled;
            }
            
            .level-info {
                color: $text;
            }
            
            .level-warning {
                color: $warning;
                text-style: bold;
            }
            
            .level-error {
                color: $error;
                text-style: bold;
            }
            
            .level-critical {
                color: $error;
                text-style: bold underline;
            }
        }
    }
    """

    log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    current_level_index: reactive[int] = reactive(1)  # Start at INFO (index 1)

    def __init__(self, log_handler: LogHandler | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_handler = log_handler or LogHandler()
        self._data_table: DataTable | None = None
        self._needs_refresh = False

    def compose(self) -> Container.ComposeResult:
        with Container():
            yield Label("Logs", id="logs-title")
            yield Label("Level: DEBUG", id="level-label")
        
        yield DataTable(id="logs-table")
        yield Footer()

    def on_mount(self) -> None:
        """Initialize the widget after mounting."""
        self._data_table = self.query_one("#logs-table", DataTable)
        self._setup_table()
        self._update_level_display()
        self._update_table()
        # Start with focus on the table
        self.set_focus(self._data_table)

    def _setup_table(self) -> None:
        """Set up the DataTable columns."""
        if self._data_table:
            self._data_table.clear()
            self._data_table.add_column("Time", width=12)
            self._data_table.add_column("Level", width=8)
            self._data_table.add_column("Logger", width=20)
            self._data_table.add_column("Message", width=100)

    def _update_level_display(self) -> None:
        """Update the log level display."""
        level_label = self.query_one("#level-label", Label)
        current_level = self.log_levels[self.current_level_index]
        level_label.update(f"Level: [bold yellow]{current_level}[/]")
        self.log_handler.set_min_level(current_level)

    def _get_level_class(self, level: str) -> str:
        """Get the CSS class for a log level."""
        return f"level-{level.lower()}"

    def _update_table(self) -> None:
        """Update the table with current log entries."""
        if not self._data_table:
            return

        self._data_table.clear()
        self._setup_table()

        current_level = self.log_levels[self.current_level_index]
        current_level_index = self.log_levels.index(current_level)
        
        for entry in self.log_handler.get_entries():
            try:
                entry_level_index = self.log_levels.index(entry.level)
                if entry_level_index >= current_level_index:
                    time_str, level, logger, message = entry.to_tuple()
                    # Add row with the level as a styled cell
                    self._data_table.add_row(
                        time_str,
                        f"[{self._get_level_class(level)}]{level}[/]",
                        logger,
                        message,
                    )
            except ValueError:
                # Skip entries with unknown log levels
                continue
        
        # Scroll to bottom to show newest logs
        if self._data_table:
            self._data_table.scroll_end()

    def action_clear_logs(self) -> None:
        """Clear all logs."""
        self.log_handler.clear()
        if self._data_table:
            self._data_table.clear()
            self._setup_table()

    def action_increase_level(self) -> None:
        """Increase log level (show less verbose logs)."""
        if self.current_level_index < len(self.log_levels) - 1:
            self.current_level_index += 1
            self._update_level_display()
            self._update_table()

    def action_decrease_level(self) -> None:
        """Decrease log level (show more verbose logs)."""
        if self.current_level_index > 0:
            self.current_level_index -= 1
            self._update_level_display()
            self._update_table()

    def action_scroll_bottom(self) -> None:
        """Scroll to the bottom of the logs."""
        if self._data_table:
            self._data_table.scroll_end()

    def action_scroll_top(self) -> None:
        """Scroll to the top of the logs."""
        if self._data_table:
            self._data_table.scroll_home()

    def watch_current_level_index(self, old: int, new: int) -> None:
        """React to log level changes."""
        self._update_level_display()
        self._update_table()

    def update_from_handler(self) -> None:
        """Update the table from the log handler's entries."""
        self._update_table()

    def add_log_entry(self, entry: LogEntry) -> None:
        """Add a log entry and update the display."""
        self.log_handler.entries.append(entry)
        # Only add if it passes the current level filter
        try:
            current_level = self.log_levels[self.current_level_index]
            current_level_index = self.log_levels.index(current_level)
            entry_level_index = self.log_levels.index(entry.level)
            if entry_level_index >= current_level_index:
                if self._data_table:
                    time_str, level, logger, message = entry.to_tuple()
                    self._data_table.add_row(
                        time_str,
                        f"[{self._get_level_class(level)}]{level}[/]",
                        logger,
                        message,
                    )
                    # Scroll to bottom
                    self._data_table.scroll_end()
        except ValueError:
            # Skip entries with unknown log levels
            pass




class LogsScreen(Screen):
    """Screen to display application logs."""

    BINDINGS = [
        Binding("c", "clear_logs", "Clear logs"),
        Binding("up", "increase_level", "Increase log level"),
        Binding("down", "decrease_level", "Decrease log level"),
        Binding("b", "scroll_bottom", "Scroll to bottom"),
        Binding("t", "scroll_top", "Scroll to top"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logs_widget: LogsWidget | None = None

    def compose(self) -> Screen.ComposeResult:
        # Get the log_handler from the app
        log_handler = getattr(self.app, 'log_handler', None)
        self._logs_widget = LogsWidget(log_handler=log_handler)
        yield self._logs_widget
        yield Footer()

    def action_clear_logs(self) -> None:
        """Clear all logs."""
        if self._logs_widget:
            self._logs_widget.action_clear_logs()

    def action_increase_level(self) -> None:
        """Increase log level."""
        if self._logs_widget:
            self._logs_widget.action_increase_level()

    def action_decrease_level(self) -> None:
        """Decrease log level."""
        if self._logs_widget:
            self._logs_widget.action_decrease_level()

    def action_scroll_bottom(self) -> None:
        """Scroll to bottom."""
        if self._logs_widget:
            self._logs_widget.action_scroll_bottom()

    def action_scroll_top(self) -> None:
        """Scroll to top."""
        if self._logs_widget:
            self._logs_widget.action_scroll_top()

    def on_mount(self) -> None:
        """Focus the logs widget on mount."""
        if self._logs_widget:
            self.set_focus(self._logs_widget)