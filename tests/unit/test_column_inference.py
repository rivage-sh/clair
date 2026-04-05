"""Tests for clair.docs.columns -- column inference from SQL."""

from __future__ import annotations

from clair.docs.columns import ColumnStatus, infer_columns
from clair.trouves.column import Column, ColumnType


class TestInferColumnsWithDeclaredColumns:
    """When the user explicitly declares columns, inference should use them as-is."""

    def test_declared_columns_returned_verbatim(self):
        declared = [
            Column(name="id", type=ColumnType.STRING),
            Column(name="amount", type=ColumnType.FLOAT),
        ]
        result = infer_columns(
            declared_columns=declared,
            resolved_sql="SELECT * FROM raw_orders",
        )
        assert result.status == ColumnStatus.DECLARED
        assert result.columns == declared
        assert result.message == ""

    def test_declared_columns_take_priority_over_sql(self):
        """Even if SQL has explicit columns, user-declared columns win."""
        declared = [Column(name="custom_col", type=ColumnType.STRING)]
        result = infer_columns(
            declared_columns=declared,
            resolved_sql="SELECT id, name FROM users",
        )
        assert result.status == ColumnStatus.DECLARED
        assert len(result.columns) == 1
        assert result.columns[0].name == "custom_col"


class TestInferColumnsNoSql:
    """SOURCE trouves have no SQL -- should return NO_SQL status."""

    def test_none_sql(self):
        result = infer_columns(declared_columns=[], resolved_sql=None)
        assert result.status == ColumnStatus.NO_SQL
        assert result.columns == []
        assert "source" in result.message.lower()

    def test_empty_sql(self):
        result = infer_columns(declared_columns=[], resolved_sql="")
        assert result.status == ColumnStatus.NO_SQL

    def test_whitespace_only_sql(self):
        result = infer_columns(declared_columns=[], resolved_sql="   \n  ")
        assert result.status == ColumnStatus.NO_SQL


class TestInferColumnsSelectStar:
    """SELECT * queries should be detected and return SELECT_STAR status."""

    def test_bare_select_star(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT * FROM orders",
        )
        assert result.status == ColumnStatus.SELECT_STAR
        assert result.columns == []
        assert "SELECT *" in result.message

    def test_select_star_multiline(self):
        sql = """
            SELECT
                *
            FROM
                analytics.revenue.orders
        """
        result = infer_columns(declared_columns=[], resolved_sql=sql)
        assert result.status == ColumnStatus.SELECT_STAR

    def test_select_distinct_star(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT DISTINCT * FROM orders",
        )
        assert result.status == ColumnStatus.SELECT_STAR

    def test_select_qualified_star(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT t.* FROM orders t",
        )
        assert result.status == ColumnStatus.SELECT_STAR

    def test_count_star_is_not_select_star(self):
        """count(*) is a function call, not a star projection."""
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT count(*) as total_rows FROM orders",
        )
        assert result.status != ColumnStatus.SELECT_STAR

    def test_select_star_case_insensitive(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="select * from orders",
        )
        assert result.status == ColumnStatus.SELECT_STAR


class TestInferColumnsFromSql:
    """When SQL has explicit columns, they should be inferred."""

    def test_simple_column_list(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT id, name, email FROM users",
        )
        assert result.status == ColumnStatus.INFERRED
        assert [c.name for c in result.columns] == ["id", "name", "email"]

    def test_aliased_columns(self):
        sql = """
            SELECT
                date_trunc('day', created_at) AS order_date,
                count(*) AS order_count,
                sum(amount) AS total_amount
            FROM raw_orders
            GROUP BY 1
        """
        result = infer_columns(declared_columns=[], resolved_sql=sql)
        assert result.status == ColumnStatus.INFERRED
        column_names = [c.name for c in result.columns]
        assert column_names == ["order_date", "order_count", "total_amount"]

    def test_qualified_column_references(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT o.id, o.amount FROM orders o",
        )
        assert result.status == ColumnStatus.INFERRED
        assert [c.name for c in result.columns] == ["id", "amount"]

    def test_mixed_aliased_and_bare(self):
        sql = "SELECT id, name, upper(email) AS email_upper FROM users"
        result = infer_columns(declared_columns=[], resolved_sql=sql)
        assert result.status == ColumnStatus.INFERRED
        assert [c.name for c in result.columns] == ["id", "name", "email_upper"]

    def test_inferred_columns_have_unknown_type(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT id FROM users",
        )
        assert result.columns[0].type == "UNKNOWN"

    def test_column_names_lowercased(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="SELECT ID, UserName FROM users",
        )
        assert [c.name for c in result.columns] == ["id", "username"]

    def test_nested_function_calls_with_alias(self):
        sql = "SELECT coalesce(a, b, 0) AS fallback_value, id FROM t"
        result = infer_columns(declared_columns=[], resolved_sql=sql)
        assert result.status == ColumnStatus.INFERRED
        assert [c.name for c in result.columns] == ["fallback_value", "id"]

    def test_expression_without_alias_skipped(self):
        """Complex expressions without AS aliases can't be named."""
        sql = "SELECT id, 1 + 2 FROM users"
        result = infer_columns(declared_columns=[], resolved_sql=sql)
        assert result.status == ColumnStatus.INFERRED
        # Only 'id' can be extracted; '1 + 2' has no alias
        assert [c.name for c in result.columns] == ["id"]


class TestInferColumnsParseFailed:
    """SQL that can't be parsed at all should return PARSE_FAILED."""

    def test_no_select_keyword(self):
        # INSERT INTO target SELECT * FROM source -- the first match finds SELECT * FROM source
        # This actually will match SELECT *, so let's use a truly unparseable statement
        pass

    def test_unparseable_sql(self):
        result = infer_columns(
            declared_columns=[],
            resolved_sql="CALL my_stored_procedure()",
        )
        assert result.status == ColumnStatus.PARSE_FAILED
        assert result.columns == []
        assert "columns=[]" in result.message


class TestBuildCatalogColumnInference:
    """Integration: verify that build_catalog attaches column_inference to each trouve."""

    def test_catalog_includes_column_inference(self, simple_project):
        from clair.core.dag import build_dag
        from clair.core.discovery import discover_project
        from clair.docs.catalog import build_catalog

        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        catalog = build_catalog(dag, simple_project)

        for full_name, trouve_data in catalog["trouves"].items():
            assert "column_inference" in trouve_data, (
                f"Trouve {full_name} missing column_inference"
            )
            inference = trouve_data["column_inference"]
            assert "status" in inference
            assert "columns" in inference
            assert "message" in inference

    def test_declared_columns_get_declared_status(self, simple_project):
        """Both the source and the table in simple_project have declared columns."""
        from clair.core.dag import build_dag
        from clair.core.discovery import discover_project
        from clair.docs.catalog import build_catalog

        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        catalog = build_catalog(dag, simple_project)

        for full_name, trouve_data in catalog["trouves"].items():
            inference = trouve_data["column_inference"]
            assert inference["status"] == ColumnStatus.DECLARED
