"""Base types and abstract parser for the Universal Source Loader.

All parsers convert their input format into a common ParsedSchema
intermediate representation, which is then assembled into a blueprint dict.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class InputFormat(str, Enum):
    """Detected input format for auto-routing."""

    DDL = "ddl"
    ETL_MAPPING = "etl_mapping"
    SCHEMA_JSON = "schema_json"
    SCHEMA_YAML = "schema_yaml"
    ENV_FILE = "env_file"
    RAW_TEXT = "raw_text"
    UNKNOWN = "unknown"


@dataclass
class ParsedColumn:
    """A column extracted from any input format."""

    name: str
    data_type: str
    raw_type: str = ""
    nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    default_value: Optional[str] = None
    check_constraints: list[str] = field(default_factory=list)
    precision: Optional[int] = None
    scale: Optional[int] = None
    max_length: Optional[int] = None
    comment: Optional[str] = None


@dataclass
class ParsedForeignKey:
    """A foreign key relationship extracted from any input format."""

    fk_column: str
    referenced_table: str
    referenced_column: str


@dataclass
class ParsedTable:
    """A table extracted from any input format."""

    name: str
    schema: str = "dbo"
    columns: list[ParsedColumn] = field(default_factory=list)
    foreign_keys: list[ParsedForeignKey] = field(default_factory=list)
    row_count: int = 1000


@dataclass
class ParsedSchema:
    """Complete schema extracted from any input format."""

    source_name: str
    source_type: str = "parsed"
    tables: list[ParsedTable] = field(default_factory=list)
    input_format: InputFormat = InputFormat.UNKNOWN
    raw_input: str = ""
    parse_warnings: list[str] = field(default_factory=list)


class BaseParser(ABC):
    """Abstract base class for input format parsers."""

    @abstractmethod
    def parse(self, content: str, **kwargs) -> ParsedSchema:
        """Parse input content into a ParsedSchema."""
        ...

    @abstractmethod
    def can_parse(self, content: str) -> bool:
        """Return True if this parser can handle the given content."""
        ...
