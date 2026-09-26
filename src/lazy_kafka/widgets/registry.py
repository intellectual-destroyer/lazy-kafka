from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, override

from textual import events, work
from textual.containers import Container, Grid
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import Reactive, reactive
from textual.validation import ValidationResult, Validator
from textual.widgets import (
    Button,
    DataTable,
    Input,
    Label,
    TextArea,
)

from lazy_kafka import registry
from lazy_kafka.widgets._status import Status
from lazy_kafka.widgets.common import (
    WidgetWithDataTable,
)

if TYPE_CHECKING:
    from textual.app import ComposeResult

_LOGGER = logging.getLogger(__name__)


class ValidSchemaType(Validator):
    """A custom validator."""

    def validate(self, value: str) -> ValidationResult:
        """Check a string is equal to its reverse."""
        if self.is_valid_schema_type(value):
            return self.success()
        else:
            return self.failure("That's not a palindrome :/")

    @staticmethod
    def is_valid_schema_type(value: str) -> bool:
        return value in registry.SchemaTypes


class SubjectInput(Input):
    """A custom Input widget for subject names with validation."""

    def validate(self, value: str) -> ValidationResult:
        """Validate the subject name."""
        if not value or not value.strip():
            return ValidationResult.failure("Subject name cannot be empty")
        # Basic validation - subject names should not contain control characters
        if any(ord(c) < 32 for c in value):
            return ValidationResult.failure("Subject name contains invalid characters")
        return ValidationResult.success()

    @staticmethod
    def is_valid_schema_type(value: str) -> bool:
        """Check if a schema type is valid."""
        return value in registry.SchemaTypes


class CreateDialog(Container, can_focus=True):
    """Modal to display on creating new schema."""

    BORDER_TITLE = "Create new schema"
    BORDER_SUBTITLE = "edit"

    _tt = """{
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "User",
      "description": "A Confluent Kafka Python User",
      "type": "object",
      "properties": {
        "name": {
          "description": "User's name",
          "type": "string"
        },
        "favorite_number": {
          "description": "User's favorite number",
          "type": "number",
          "exclusiveMinimum": 0
        },
        "favorite_color": {
          "description": "User's favorite color",
          "type": "string"
        }
      },
      "required": [ "name", "favorite_number", "favorite_color" ]
    }"""

    class Create(Message):
        """Color selected message."""

        def __init__(
            self, subject_name: str, schema_type: registry.SchemaTypes, schema: str
        ) -> None:
            self.subject_name = subject_name
            self.schema_type = schema_type
            self.schema = schema
            super().__init__()

    def __init__(self, selected_id: str | None = None, *args, **kwargs):
        self.selected_id = selected_id
        super().__init__(*args, **kwargs)

    def compose(self) -> ComposeResult:
        text_area = TextArea(show_line_numbers=True, id="editor").code_editor(
            CreateDialog._tt
        )
        text_area.cursor_blink = False
        text_area.indent_width = 2
        # Register the json and highlight query
        # text_area.register_language(java_language, java_highlight_query)
        # Switch to Java
        text_area.language = "json"
        yield Grid(
            Input(placeholder="Subject name", id="subject-name"),
            Input(
                placeholder="JSON | AVRO | PROTOBUF",
                id="schema-type",
                validators=[ValidSchemaType()],
            ),
            text_area,
            Button("Create", variant="success", id="create"),
            Button("Cancel", variant="primary", id="cancel"),
            id="dialog",
        )

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "create":
            subject_name = self.query_one("#subject-name").value
            schema_type = self.query_one("#schema-type").value
            schema = self.query_one(TextArea).text
            # TODO: add json validator in the future?
            # NOTE: first the schema is parsed to get rid of oddities
            try:
                schema = json.dumps(json.loads(schema))
            except json.JSONDecodeError as e:
                _LOGGER.error(e.msg, exc_info=e)
                # TODO: dispatch invalid schema action
                return

            _LOGGER.debug(f"{subject_name=} {schema_type=}")
            _LOGGER.debug(f"{schema=}")
            self.post_message(self.Create(subject_name, schema_type, schema))
            await self.remove()
        else:
            await self.remove()


class DeleteDialog(Container, can_focus=True):
    """Modal to display on creating new schema."""

    BINDINGS = [
        ("y", "yes", "yes"),
        ("n", "no", "no"),
    ]

    class Delete(Message):
        """Color selected message."""

        def __init__(self, selected_id: str, selected_version: int | None = None) -> None:
            self.selected_id = selected_id
            self.selected_version = selected_version
            super().__init__()

    def __init__(self, selected_id: registry.Subject, selected_version: int | None = None, *args, **kwargs):
        self.selected_id = selected_id
        self.selected_version = selected_version
        super(Container, self).__init__(*args, **kwargs)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "delete":
            self.post_message(self.Delete(self.selected_id, self.selected_version))
        await self.remove()

    async def on_key(self, event: events.Key) -> None:
        """Handle D as button press."""
        if event.key == "y":
            self.post_message(self.Delete(self.selected_id, self.selected_version))
            await self.remove()
        elif event.key == "n":
            await self.remove()

        event.stop()

    def compose(self) -> ComposeResult:
        yield Grid(
            Label(f":warning: Delete [italic]{self.selected_id}[/]?", id="question"),
            Button("Yes (y)", variant="error", id="delete"),
            Button("No (n)", variant="primary", id="cancel"),
            id="dialog",
        )


class DialogOpen(Message):
    """Color selected message."""

    def __init__(self, selected_id: str) -> None:
        self.selected_id = selected_id
        super().__init__()


class SchemaRegistryPanel(
    WidgetWithDataTable[registry.Subject, registry.SubjectDetails]
):
    """Schema Registry widget."""

    BORDER_TITLE = "Subjects"
    BORDER_SUBTITLE = "status"
    BINDINGS = [
        ("c", "create", "create"),
        ("d", "delete", "delete"),
    ]

    selected_id: Reactive[str] = reactive(str)
    details = reactive(registry.SubjectDetails)

    async def action_delete(self):
        """Action to display the delete dialog."""
        try:
            await self.query_one("#details").remove()
        except NoMatches:
            pass

        self.mount(DeleteDialog(self.selected_id, id="delete-dialog"))
        self.post_message(DialogOpen("#delete"))

    @work(exclusive=True, exit_on_error=False)
    async def on_delete_dialog_delete(self, message: DeleteDialog.Delete):
        r = await self.hook.asubjects_delete(
            message.selected_id, version=message.selected_version
        )
        _LOGGER.info("Topic deleted %s", r)

    async def action_create(self):
        """Action to display the quit dialog."""
        try:
            await self.query_one("#details").remove()
        except NoMatches:
            pass
        self.mount(CreateDialog(id="create-dialog"))
        self.post_message(DialogOpen("TextArea"))

    @work(exclusive=True)
    async def on_create_dialog_create(self, message: CreateDialog.Create):
        r = await self.hook.asubjects_create(
            message.subject_name,
            data={"schema": message.schema, "schemaType": message.schema_type},
        )
        if r.is_success:
            _LOGGER.info("Topic created %s", r)
        else:
            _LOGGER.error("%s", r.content.decode("utf8"))

    def __init__(self, hook: registry.SchemaRegistry, *args: Any, **kwargs: Any):
        self.subjects = dict()
        self.hook = hook
        self.data_auto_refresh = False
        super().__init__(*args, **kwargs)
        _LOGGER.debug(self.subjects)

    def compose(self) -> Any:
        yield Status()
        yield DataTable(cursor_type="row", zebra_stripes=True)

    def on_mount(self):
        data_table = self.query_one(DataTable)
        data_table.add_column("Subjects")
        super().on_mount()

    @override
    def subject_to_table(self, response: dict[str, registry.Subject], *args, **kwargs):
        """Transform raw `hook.asubjects` results into DataTable dict values."""
        return {i: registry.Subject(i) for i in response}

    def apply_filter(
        self, data: registry.Subject, token: str, *, textual_styling="dark_orange"
    ) -> registry.Subject:
        return registry.Subject(
            data.replace(
                token,
                f"[{textual_styling}]{token}[/{textual_styling}]",
            )
        )
