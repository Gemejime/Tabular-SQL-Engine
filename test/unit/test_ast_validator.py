"""Unit tests for deterministic sqlglot AST security validator."""

from __future__ import annotations

import time

import pytest

from internal.validator import (
    ASTSyntaxError,
    ASTValidator,
    DisallowedAliasError,
    DisallowedStatementError,
    DisallowedTableReferenceError,
    JoinLimitExceededError,
    QueryLengthExceededError,
)


@pytest.fixture(scope="module")
def validator() -> ASTValidator:
    return ASTValidator(max_sql_length=4096, max_joins=3)


def test_allows_valid_select_with_aggregations(validator: ASTValidator) -> None:
    sql = (
        "SELECT region, SUM(revenue) AS total_rev, COUNT(*) AS row_cnt "
        "FROM tbl WHERE quantity >= 2 GROUP BY region ORDER BY total_rev DESC LIMIT 10"
    )
    ast = validator.validate(sql, allowed_table_alias="tbl")
    assert ast is not None


def test_allows_with_cte_and_union_intersect_except(validator: ASTValidator) -> None:
    sql = """
    WITH regional AS (
        SELECT region, SUM(revenue) AS total_rev
        FROM tbl
        GROUP BY region
    )
    SELECT region, total_rev FROM regional WHERE total_rev > 100
    UNION ALL
    SELECT region, total_rev FROM regional WHERE total_rev <= 100
    """
    assert validator.validate(sql, allowed_table_alias="tbl") is not None

    intersect_sql = (
        "SELECT region FROM tbl WHERE quantity > 1 "
        "INTERSECT SELECT region FROM tbl WHERE revenue > 50"
    )
    assert validator.validate(intersect_sql, allowed_table_alias="tbl") is not None

    except_sql = (
        "SELECT region FROM tbl WHERE quantity > 1 EXCEPT SELECT region FROM tbl WHERE revenue < 20"
    )
    assert validator.validate(except_sql, allowed_table_alias="tbl") is not None


def test_allows_safe_anonymous_analytical_functions(validator: ASTValidator) -> None:
    sql = (
        "SELECT region, MEDIAN(revenue) AS med_rev, "
        "ROUND(AVG(revenue), 2) AS avg_rev, "
        "APPROX_COUNT_DISTINCT(category) AS distinct_cats "
        "FROM tbl GROUP BY region"
    )
    assert validator.validate(sql, allowed_table_alias="tbl") is not None


def test_rejects_unallowlisted_table_and_schema_references(validator: ASTValidator) -> None:
    with pytest.raises(DisallowedTableReferenceError):
        validator.validate("SELECT * FROM other_table", allowed_table_alias="tbl")

    with pytest.raises(DisallowedTableReferenceError):
        validator.validate("SELECT * FROM main.tbl", allowed_table_alias="tbl")

    with pytest.raises(DisallowedTableReferenceError):
        validator.validate("SELECT * FROM information_schema.tables", allowed_table_alias="tbl")


def test_rejects_unsafe_aliases(validator: ASTValidator) -> None:
    with pytest.raises(DisallowedAliasError):
        validator.validate('SELECT revenue AS "bad;alias" FROM tbl', allowed_table_alias="tbl")

    with pytest.raises(DisallowedAliasError):
        validator.validate(
            'WITH "bad-cte" AS (SELECT 1) SELECT * FROM tbl',
            allowed_table_alias="tbl",
        )


def test_enforces_join_limit(validator: ASTValidator) -> None:
    ok_sql = (
        "SELECT a.id FROM tbl a "
        "JOIN tbl b ON a.id = b.id "
        "JOIN tbl c ON a.id = c.id "
        "JOIN tbl d ON a.id = d.id"
    )
    assert validator.validate(ok_sql, allowed_table_alias="tbl") is not None

    too_many_joins_sql = (
        "SELECT a.id FROM tbl a "
        "JOIN tbl b ON a.id = b.id "
        "JOIN tbl c ON a.id = c.id "
        "JOIN tbl d ON a.id = d.id "
        "JOIN tbl e ON a.id = e.id"
    )
    with pytest.raises(JoinLimitExceededError):
        validator.validate(too_many_joins_sql, allowed_table_alias="tbl")


def test_enforces_query_length_and_empty_syntax(validator: ASTValidator) -> None:
    with pytest.raises(ASTSyntaxError):
        validator.validate("   ", allowed_table_alias="tbl")

    with pytest.raises(QueryLengthExceededError):
        validator.validate("SELECT " + ("1," * 3000) + "1 FROM tbl", allowed_table_alias="tbl")


def test_warm_steady_state_validation_latency(validator: ASTValidator) -> None:
    sql = (
        "SELECT region, category, SUM(revenue) AS total_rev, COUNT(*) AS cnt "
        "FROM tbl WHERE quantity > 1 GROUP BY region, category ORDER BY total_rev DESC"
    )
    # Warm up once before timing steady-state calls.
    validator.validate(sql, allowed_table_alias="tbl")

    iterations = 50
    start = time.perf_counter()
    for _ in range(iterations):
        validator.validate(sql, allowed_table_alias="tbl")
    avg_ms = ((time.perf_counter() - start) / iterations) * 1000.0
    # Pure-Python sqlglot.parse takes ~2-4 ms on shared CI runners; guard against regressions.
    assert avg_ms <= 15.0, f"Expected warm AST validation <= 15.0 ms, got {avg_ms:.3f} ms"


def test_rejects_unterminated_sql_string_with_syntax_error(validator: ASTValidator) -> None:
    with pytest.raises(ASTSyntaxError):
        validator.validate("SELECT 'unterminated", allowed_table_alias="tbl")


def test_enforces_lexical_cte_scope(validator: ASTValidator) -> None:
    nested_leak_sql = (
        "SELECT * FROM (WITH inner_cte AS (SELECT region FROM tbl) SELECT * FROM inner_cte) sub "
        "UNION ALL SELECT * FROM inner_cte"
    )
    with pytest.raises(DisallowedTableReferenceError):
        validator.validate(nested_leak_sql, allowed_table_alias="tbl")


def test_allows_date_diff_with_approved_unit_and_rejects_unrelated_var(
    validator: ASTValidator,
) -> None:
    assert (
        validator.validate(
            "SELECT DATE_DIFF('day', start_dt, end_dt) FROM tbl",
            allowed_table_alias="tbl",
        )
        is not None
    )
    with pytest.raises(DisallowedStatementError):
        validator.validate(
            "SELECT DATE_DIFF('bogus_unit', start_dt, end_dt) FROM tbl",
            allowed_table_alias="tbl",
        )
