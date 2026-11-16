"""Validation exception hierarchy for SQL AST security checks."""

from __future__ import annotations


class ValidationError(Exception):
    """Base class for deterministic SQL AST validation rejections."""

    rule: str = "VALIDATION_ERROR"

    def __init__(self, message: str, *, rule: str | None = None) -> None:
        if rule is not None:
            self.rule = rule
        super().__init__(message)
        self.message = message


class QueryLengthExceededError(ValidationError):
    """Raised when incoming SQL exceeds MAX_SQL_LENGTH."""

    rule = "QUERY_LENGTH_EXCEEDED"


class ASTSyntaxError(ValidationError):
    """Raised when SQL cannot be parsed or contains multiple statements."""

    rule = "SYNTAX_ERROR"


class DisallowedStatementError(ValidationError):
    """Raised when SQL contains non-SELECT statements or disallowed AST nodes."""

    rule = "DISALLOWED_STATEMENT"


class DisallowedFunctionError(ValidationError):
    """Raised when SQL invokes a banned or non-allowlisted function."""

    rule = "DISALLOWED_FUNCTION"


class DisallowedTableReferenceError(ValidationError):
    """Raised when SQL references an unallowlisted table, file literal, or schema."""

    rule = "DISALLOWED_TABLE_REFERENCE"


class DisallowedAliasError(ValidationError):
    """Raised when a column, table, or CTE alias fails identifier validation."""

    rule = "DISALLOWED_ALIAS"


class JoinLimitExceededError(ValidationError):
    """Raised when SQL exceeds MAX_JOINS."""

    rule = "JOIN_LIMIT_EXCEEDED"
