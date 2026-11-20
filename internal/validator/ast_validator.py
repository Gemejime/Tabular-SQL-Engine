"""Deterministic sqlglot AST security validator for read-only DuckDB queries."""

from __future__ import annotations

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.errors import ParseError, TokenError

from internal.validator.exceptions import (
    ASTSyntaxError,
    DisallowedAliasError,
    DisallowedFunctionError,
    DisallowedStatementError,
    DisallowedTableReferenceError,
    JoinLimitExceededError,
    QueryLengthExceededError,
)
from internal.validator.rules import (
    ALLOWED_ANONYMOUS_FUNCTIONS,
    ALLOWED_ROOT_EXPRESSIONS,
    ALLOWED_STRUCTURAL_NODES,
    APPROVED_DATE_UNITS,
    BANNED_AST_NODES,
    BANNED_FUNCTION_NAMES,
    BANNED_FUNCTION_PREFIXES,
    SAFE_IDENTIFIER_RE,
    ASTNode,
)


class ASTValidator:
    """Validates incoming SQL queries against a strict read-only DuckDB AST allowlist."""

    def __init__(self, *, max_sql_length: int = 8192, max_joins: int = 5) -> None:
        self._max_sql_length = max_sql_length
        self._max_joins = max_joins
        self._banned_type_cache: dict[type[ASTNode], bool] = {}
        self._allowed_type_cache: dict[type[ASTNode], bool] = {}
        self._dialect = Dialect.get_or_raise("duckdb")
        # Pre-warm sqlglot DuckDB dialect parser so steady-state validation stays <= 2 ms.
        sqlglot.parse_one("SELECT 1", read=self._dialect)

    def _is_banned_node(self, node: ASTNode) -> bool:
        node_type = type(node)
        cached = self._banned_type_cache.get(node_type)
        if cached is None:
            cached = isinstance(node, BANNED_AST_NODES)
            self._banned_type_cache[node_type] = cached
        return cached

    def _is_allowed_node(self, node: ASTNode) -> bool:
        node_type = type(node)
        cached = self._allowed_type_cache.get(node_type)
        if cached is None:
            cached = isinstance(node, ALLOWED_STRUCTURAL_NODES)
            self._allowed_type_cache[node_type] = cached
        return cached

    def validate(self, sql: str, allowed_table_alias: str) -> ASTNode:
        """Validate SQL against length, syntax, statement, function, table, alias, and join rules.

        Args:
            sql: Raw SQL string from the incoming Flight ticket.
            allowed_table_alias: The single table alias bound in the DuckDB sandbox.

        Returns:
            The validated root sqlglot AST expression.

        Raises:
            ValidationError: If any security or structural rule is violated.
        """
        if not sql or not sql.strip():
            raise ASTSyntaxError("SQL query must not be empty.")

        if len(sql) > self._max_sql_length:
            raise QueryLengthExceededError(
                f"SQL query length {len(sql)} exceeds maximum of {self._max_sql_length} characters."
            )

        if not SAFE_IDENTIFIER_RE.match(allowed_table_alias):
            raise DisallowedAliasError(f"Invalid allowed table alias: {allowed_table_alias!r}")

        try:
            statements = sqlglot.parse(sql, read=self._dialect)
        except (ParseError, TokenError) as exc:
            raise ASTSyntaxError(f"Failed to parse SQL query: {exc}") from exc

        non_empty = [stmt for stmt in statements if stmt is not None]
        if len(non_empty) != 1:
            raise ASTSyntaxError(f"Expected exactly 1 SQL statement, got {len(non_empty)}.")

        root = non_empty[0]
        if self._is_banned_node(root) or not isinstance(root, ALLOWED_ROOT_EXPRESSIONS):
            raise DisallowedStatementError(
                f"Statement type {type(root).__name__} is not permitted; "
                "only read-only SELECT is allowed."
            )

        self._validate_cte_aliases(root)
        join_count = 0

        for node in root.walk():
            if self._is_banned_node(node):
                raise DisallowedStatementError(
                    f"Disallowed AST construct {type(node).__name__} detected."
                )

            if not self._is_allowed_node(node) and not self._is_approved_date_unit_var(node):
                raise DisallowedStatementError(
                    f"Unallowlisted AST node {type(node).__name__} detected."
                )

            if isinstance(node, exp.Join):
                join_count += 1
                if join_count > self._max_joins:
                    raise JoinLimitExceededError(
                        f"Query contains {join_count} joins, exceeding MAX_JOINS={self._max_joins}."
                    )

            if isinstance(node, exp.Func):
                self._validate_function(node)

            if isinstance(node, exp.Table):
                self._validate_table(node, allowed_table_alias)

            if isinstance(node, exp.Alias | exp.TableAlias):
                self._validate_alias(node)

        return root

    @staticmethod
    def _is_approved_date_unit_var(node: ASTNode) -> bool:
        """Allow exp.Var only when used as an approved unit argument on date functions."""
        if not isinstance(node, exp.Var) or node.arg_key != "unit":
            return False
        if not isinstance(
            node.parent,
            exp.DateDiff | exp.DateTrunc | exp.DateAdd | exp.DateSub | exp.Extract,
        ):
            return False
        return node.name.lower() in APPROVED_DATE_UNITS

    @staticmethod
    def _validate_cte_aliases(root: ASTNode) -> None:
        """Validate all CTE alias identifiers defined in WITH clauses."""
        for cte in root.find_all(exp.CTE):
            alias_name = cte.alias_or_name
            if not alias_name or not SAFE_IDENTIFIER_RE.match(alias_name):
                raise DisallowedAliasError(f"Invalid CTE alias: {alias_name!r}")

    @staticmethod
    def _is_descendant_or_self(node: ASTNode, ancestor: ASTNode) -> bool:
        curr: ASTNode | None = node
        while curr is not None:
            if curr is ancestor:
                return True
            curr = curr.parent
        return False

    @classmethod
    def _visible_cte_names(cls, table: exp.Table) -> set[str]:
        """Collect CTE names visible from ``table``'s lexical SQL scope."""
        visible: set[str] = set()
        child: ASTNode = table
        curr = table.parent
        while curr is not None:
            with_clause = curr.args.get("with_") or curr.args.get("with")
            if isinstance(with_clause, exp.With):
                is_recursive = bool(with_clause.args.get("recursive"))
                for cte in with_clause.expressions:
                    if not isinstance(cte, exp.CTE):
                        continue
                    alias_name = cte.alias_or_name
                    if child is with_clause and cls._is_descendant_or_self(table, cte):
                        if is_recursive and alias_name:
                            visible.add(alias_name.lower())
                        break
                    if alias_name:
                        visible.add(alias_name.lower())
            child = curr
            curr = curr.parent
        return visible

    @staticmethod
    def _validate_function(node: exp.Func) -> None:
        """Ensure function calls are not banned and anonymous functions are allowlisted."""
        if isinstance(node, exp.Anonymous):
            func_name = node.name.lower()
            if func_name in BANNED_FUNCTION_NAMES or func_name.startswith(BANNED_FUNCTION_PREFIXES):
                raise DisallowedFunctionError(f"Function '{func_name}' is explicitly banned.")
            if func_name not in ALLOWED_ANONYMOUS_FUNCTIONS:
                raise DisallowedFunctionError(
                    f"Function '{func_name}' is not in the approved function allowlist."
                )
            return

        sql_name = (node.key or type(node).__name__).lower()
        if sql_name in BANNED_FUNCTION_NAMES or sql_name.startswith(BANNED_FUNCTION_PREFIXES):
            raise DisallowedFunctionError(f"Function '{sql_name}' is explicitly banned.")

    @classmethod
    def _validate_table(
        cls,
        table: exp.Table,
        allowed_table_alias: str,
    ) -> None:
        """Ensure table references point only to the bound alias or a visible local CTE."""
        if table.catalog or table.db:
            raise DisallowedTableReferenceError(
                "Schema-qualified or catalog-qualified table references are not permitted."
            )

        if not isinstance(table.this, exp.Identifier):
            raise DisallowedTableReferenceError(
                "Table reference must be a simple identifier "
                "(file literals and sub-expressions are banned)."
            )

        table_name = table.name
        if not SAFE_IDENTIFIER_RE.match(table_name):
            raise DisallowedTableReferenceError(f"Invalid table identifier: {table_name!r}")

        lowered = table_name.lower()
        if lowered != allowed_table_alias.lower() and lowered not in cls._visible_cte_names(table):
            raise DisallowedTableReferenceError(
                f"Table '{table_name}' is not permitted; "
                f"only '{allowed_table_alias}' or local CTEs are allowed."
            )

    @staticmethod
    def _validate_alias(node: exp.Alias | exp.TableAlias) -> None:
        """Validate column and table aliases against the safe identifier pattern."""
        alias_name = node.alias_or_name
        if alias_name and not SAFE_IDENTIFIER_RE.match(alias_name):
            raise DisallowedAliasError(f"Disallowed alias identifier: {alias_name!r}")
