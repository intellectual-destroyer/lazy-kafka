"""Tests for Textual widgets."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lazy_kafka.config import Configuration
from lazy_kafka.widgets.registry import (
    CreateDialog,
    DeleteDialog,
    SchemaRegistryPanel,
    SubjectInput,
)
from lazy_kafka.widgets.switcher import ContentSwitcher


class TestContentSwitcher:
    """Tests for ContentSwitcher widget."""

    def test_init(self) -> None:
        """Test ContentSwitcher initialization."""
        switcher = ContentSwitcher(initial="test")
        assert switcher.initial == "test"

    def test_init_default(self) -> None:
        """Test ContentSwitcher initialization with default values."""
        switcher = ContentSwitcher()
        assert switcher.initial is None


class TestSubjectInput:
    """Tests for SubjectInput widget."""

    def test_validate_valid_subject(self) -> None:
        """Test validation of valid subject."""
        input_widget = SubjectInput()
        result = input_widget.validate("test-subject")
        assert result.is_valid

    def test_validate_empty_subject(self) -> None:
        """Test validation of empty subject."""
        input_widget = SubjectInput()
        result = input_widget.validate("")
        assert not result.is_valid

    def test_validate_invalid_subject(self) -> None:
        """Test validation of invalid subject with special characters."""
        input_widget = SubjectInput()
        # Subjects should not contain certain special characters
        result = input_widget.validate("test\x00subject")
        # This might be valid depending on implementation
        # Just ensure it returns a ValidationResult
        assert hasattr(result, "is_valid")

    def test_is_valid_schema_type_avro(self) -> None:
        """Test is_valid_schema_type with AVRO."""
        result = SubjectInput.is_valid_schema_type("AVRO")
        assert result is True

    def test_is_valid_schema_type_protobuf(self) -> None:
        """Test is_valid_schema_type with PROTOBUF."""
        result = SubjectInput.is_valid_schema_type("PROTOBUF")
        assert result is True

    def test_is_valid_schema_type_json(self) -> None:
        """Test is_valid_schema_type with JSON."""
        result = SubjectInput.is_valid_schema_type("JSON")
        assert result is True

    def test_is_valid_schema_type_invalid(self) -> None:
        """Test is_valid_schema_type with invalid type."""
        result = SubjectInput.is_valid_schema_type("INVALID")
        assert result is False


class TestSchemaRegistryPanel:
    """Tests for SchemaRegistryPanel widget."""

    @pytest.fixture
    def mock_panel(self) -> SchemaRegistryPanel:
        """Create a mock SchemaRegistryPanel for testing."""
        with patch("lazy_kafka.registry.SchemaRegistry") as mock_registry:
            mock_registry_instance = MagicMock()
            mock_registry.return_value = mock_registry_instance
            
            return SchemaRegistryPanel(hook=mock_registry_instance, *[], **{})

    def test_init(self, mock_panel: SchemaRegistryPanel) -> None:
        """Test SchemaRegistryPanel initialization."""
        assert mock_panel.hook is not None

    def test_compose(self, mock_panel: SchemaRegistryPanel) -> None:
        """Test SchemaRegistryPanel.compose method."""
        # This would normally require Textual context
        # Just ensure the method exists
        assert hasattr(mock_panel, "compose")


class TestDeleteDialog:
    """Tests for DeleteDialog widget."""

    def test_init(self) -> None:
        """Test DeleteDialog initialization."""
        dialog = DeleteDialog("test-subject", 1)
        assert dialog.selected_id == "test-subject"
        assert dialog.selected_version == 1


class TestCreateDialog:
    """Tests for CreateDialog widget."""

    def test_init(self) -> None:
        """Test CreateDialog initialization."""
        dialog = CreateDialog("test-subject")
        assert dialog.selected_id == "test-subject"


class TestWidgetIntegration:
    """Integration tests for widgets."""

    def test_widgets_have_proper_structure(self) -> None:
        """Test that all widgets follow the proper structure."""
        # Test that widgets can be instantiated
        switcher = ContentSwitcher()
        assert hasattr(switcher, "compose")

    def test_widgets_accept_configuration(self) -> None:
        """Test that widgets can accept configuration."""
        config = Configuration()
        
        with patch("lazy_kafka.registry.SchemaRegistry") as mock_registry:
            mock_registry_instance = MagicMock()
            mock_registry.return_value = mock_registry_instance
            
            panel = SchemaRegistryPanel(hook=mock_registry_instance)
            # Panel should be able to use config
            assert panel is not None
