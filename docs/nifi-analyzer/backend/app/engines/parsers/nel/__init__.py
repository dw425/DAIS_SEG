"""NiFi Expression Language engine — translates ${attr} expressions to PySpark."""

from app.engines.parsers.nel.parser import parse_nel_expression

__all__ = ["parse_nel_expression"]
