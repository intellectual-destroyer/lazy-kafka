"""Confluent kafka registry interface (API).

`confluent_kafka` uses requests library, which is not going to cut it for us.


textual docs: https://textual.textualize.io/guide/workers/#thread-workers


This is confulent cloud, so not that relevant
Schemas API: https://docs.confluent.io/cloud/current/api.html#tag/Modes-(v1)/operation/updateTopLevelMode

"""

from __future__ import annotations

import json
import logging
from enum import EnumMeta, StrEnum
from functools import partialmethod
from typing import Any, TypedDict, override

import httpx

from lazy_kafka.config import RegistryConfiguration

_LOGGER = logging.getLogger(__name__)

list_schemas = "schemas/types"


class Subject(str):
    """Custom type to represent the selected `subject`.

    The class has to support the `unpack` operator to play nice with
    textual `DataTable.add_rows`.


    """

    def to_table_values(self):
        return (self,)

    @override
    def __iter__(self):
        yield str(self)


class MetaEnum(EnumMeta):
    """Implement `in` and case-insensitive enum."""

    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True
    
    def __getitem__(cls, name):
        """Make enum case-insensitive for item access."""
        if isinstance(name, str):
            name_upper = name.upper()
            if hasattr(cls, name_upper):
                return getattr(cls, name_upper)
        return super().__getitem__(name)
    
    def __call__(cls, value, *args, **kwargs):
        """Make enum case-insensitive by converting value to uppercase."""
        if isinstance(value, str):
            value_upper = value.upper()
            # Try each member's value
            for member in cls:
                if member.value.upper() == value_upper:
                    return member
        # If not found, raise ValueError as normal
        return super().__call__(value, *args, **kwargs)


class SubjectDetails(TypedDict):
    id: int
    subject: Subject
    version: int
    schema: Any


class SubjectNew(TypedDict):
    """New subject.

    schema – The schema string
    schemaType – Defines the schema format: AVRO (default), PROTOBUF, JSON (Optional)
    references – Specifies the names of referenced schemas (Optional). To learn more, see Schema references.
    metadata – Specifies the metadata for the schema (Optional). To learn more, see “Metadata Properties” in Data Contracts for Schema Registry .
    ruleSet – Specifies the ruleSet for the schema (Optional). To learn more, see “Rules” in Data Contracts for Schema Registry .
    """

    schema: str
    schemaType: SchemaTypes
    references: Any | None
    metadata: Any | None
    ruleSet: Any | None


class SchemaTypes(StrEnum, metaclass=MetaEnum):
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"
    AVRO = "AVRO"


class SchemaRegistry:
    # Reference: https://docs.confluent.io/platform/current/schema-registry/develop/api.html#content-types
    _CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

    def __init__(self, config: RegistryConfiguration = RegistryConfiguration()) -> None:
        """Initialise client.

        Performs a sanity 'get' request, before creating a client.

        Args:
            host:
        """
        _auth = None
        if config.username and config.password:
            # Basic authentication
            _auth = httpx.BasicAuth(username=config.username, password=config.password)

        try:
            _debug_info = httpx.get(config.host, auth=_auth)
        except httpx.HTTPError as e:
            _LOGGER.error("Connection error: %s", e)
            raise ConnectionRefusedError(f"Cannot connect to {config.host}") from e
        _LOGGER.debug("Registry info: %s", _debug_info)
        assert _debug_info.status_code == 200
        self.host = config.host
        self._client = httpx.AsyncClient(
            base_url=self.host,
            headers={"Content-Type": SchemaRegistry._CONTENT_TYPE},
            auth=_auth,
        )

    def __repr__(self):
        return f"SchemaRegistry@{self.host}"

    def _generic_get_json(self, url: str):
        response = httpx.get(self.host + url)
        return response.json()

    async def _ageneric_get_json(self, url: str):
        _LOGGER.debug("%s request: %s", self, url)
        response = await self._client.get(self.host + url)
        return response.json()

    asubjects = partialmethod(_ageneric_get_json, "/subjects")
    subjects = partialmethod(_generic_get_json, "/subjects")
    subjects.__doc__ = """List subjects.

        The subjects resource provides a list of all registered subjects across
        all contexts in your Schema Registry. A subject refers to the name under
        which the schema is registered. If you are using Schema Registry for Kafka
        , then a subject refers to either a “<topic>-value” or “<topic>-key”
        depending on whether you are registering the value schema for that topic
        or the key schema. To learn more about subject name strategies, see
        [How the naming strategies work](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#sr-schemas-subject-name-strategies-work).
        """

    async def asubject_versions(self, subject: Subject):
        """Get all versions of `subject`."""
        return await self._ageneric_get_json(url=f"/subjects/{subject}/versions")

    async def asubject_latest(self, subject: tuple[Subject]) -> SubjectDetails:
        """Get latest version of subject."""
        return await self._ageneric_get_json(url=f"/subjects/{subject}/versions/-1")

    aget_details = asubject_latest

    async def asubjects_delete(self, subject: Subject, version: int):
        return await self._client.delete(url=f"/subjects/{subject}/versions/{version}")

    async def aschema_versions(self, subject: Subject):
        return await self._ageneric_get_json(url=f"/subjects/{subject}/versions")

    async def asubjects_create(self, subject: Subject, data: SubjectNew):
        return await self._client.post(url=f"/subjects/{subject}/versions", json=data)

    def _serialize_json_schema(self, schema: object) -> str:
        return json.dumps(schema, sort_keys=True)


async def test_async_api():
    import json

    sr = SchemaRegistry()
    schema = json.dumps(
        {
            "type": "object",
            "properties": {"amount": {"type": "number"}, "id": {"type": "number"}},
        }
    )
    SubjectNew(
        schema=schema,
        schemaType="JSONSchema",
    )
    res = await sr.asubjects_create(
        # TODO remove the dumps form here
        "other",
        data={"schema": schema, "schemaType": "JSONSchema"},
    )
    print(res)
    return res


if __name__ == "__main__":
    # sr = SchemaRegistry()
    # from rich.pretty import pprint
    # pprint(sr.subjects())
    import asyncio
    # Create a nice information summary:
    # number of subjects
    # Allowed formats /schemas/types/
    # Compatibility mode (/cont
    # global mode?

    r = asyncio.run(test_async_api())
# >>> t = {"type": "string"}
# >>> s2 = json.dumps({"schemaType":"JSON", "schema": json.dumps(t) })
# >>> r = httpx.post("http://localhost:8081/subjects/kakadu/versions",headers={"Content-Type": "application/vnd.schemaregistry.v1+json"} ,data=s2)
