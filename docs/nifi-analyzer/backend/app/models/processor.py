"""Core processor and connection models."""

from pydantic import BaseModel


class Processor(BaseModel):
    """A single processor/component extracted from an ETL flow definition."""

    name: str
    type: str
    platform: str = "unknown"
    properties: dict = {}
    group: str = "(root)"
    state: str = "RUNNING"
    scheduling: dict | None = None


class Connection(BaseModel):
    """A directed edge between two processors in the flow."""

    source_name: str
    destination_name: str
    relationship: str = "success"


class ProcessGroup(BaseModel):
    """A logical grouping of processors."""

    name: str
    processors: list[str] = []


class ControllerService(BaseModel):
    """A shared service used by one or more processors (e.g. DBCP connection pool)."""

    name: str
    type: str
    properties: dict = {}
