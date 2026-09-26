"""Tests for utility functions."""

from __future__ import annotations

import re
from unittest.mock import patch

import pytest

from lazy_kafka.utils import get_current_time


class TestGetCurrentTime:
    """Tests for get_current_time function."""

    def test_returns_string(self) -> None:
        """Test that get_current_time returns a string."""
        result = get_current_time()
        assert isinstance(result, str)

    def test_format_is_hh_mm_ss(self) -> None:
        """Test that the time format is HH:MM:SS."""
        result = get_current_time()
        # Check format matches HH:MM:SS or HH:MM:SS.microseconds
        pattern = r"^\d{2}:\d{2}:\d{2}(\.\d{6})?$"
        assert re.match(pattern, result) is not None

    def test_uses_current_time(self) -> None:
        """Test that the function returns the current time."""
        import time
        
        before = time.time()
        result = get_current_time()
        after = time.time()
        
        # Parse the time string
        hours, minutes, seconds = map(float, result.split(":"))
        result_seconds = hours * 3600 + minutes * 60 + seconds
        
        # Check that the result is within a reasonable range
        # (accounting for time zone differences)
        current_seconds = time.localtime(before).tm_hour * 3600 + \
                         time.localtime(before).tm_min * 60 + \
                         time.localtime(before).tm_sec
        
        # Allow 2 seconds difference for execution time
        assert abs(result_seconds - current_seconds) < 2


class TestTimeMocking:
    """Tests for time mocking scenarios."""

    def test_mocked_time(self) -> None:
        """Test get_current_time with mocked time."""
        # Create a mock that returns a specific time tuple
        mock_time = (2024, 1, 15, 14, 30, 45, 0, 15, 0)  # Jan 15, 2024, 14:30:45
        
        with patch("time.localtime", return_value=mock_time):
            # Also mock time.time() to return an integer (no fractional seconds)
            with patch("time.time", return_value=1705342245.0):
                result = get_current_time()
                assert result == "14:30:45"

    def test_mocked_time_with_microseconds(self) -> None:
        """Test get_current_time with mocked time including microseconds."""
        mock_time = (2024, 1, 15, 14, 30, 45, 0, 15, 0)
        
        with patch("time.localtime", return_value=mock_time):
            with patch("time.time", return_value=1705342245.123456):
                result = get_current_time()
                # Should include microseconds
                assert "." in result
                assert result.startswith("14:30:45")
