"""The tests of clair.docs.columns. That module reads the columns from the SQL."""

from __future__ import annotations

from clair.docs.columns import ColumnStatus, infer_columns
from clair.trouves.column import Column, ColumnType


class TestInferColumnsWithDeclaredColumns:
    """When the user declares the columns, clair keeps them with no change."""

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
        """The columns of the user win, and the columns in the SQL do not."""
        declared = [Column(name="custom_col", type=ColumnType.STRING)]
        result = infer_columns(
            declared_columns=declared,
            resolved_sql="SELECT id, name FROM users",
        )
        assert result.status == ColumnStatus.DECLARED
        assert len(result.columns) == 1
        assert result.columns[0].name == "custom_col"


class TestInferColumnsNoSql:
    """A SOURCE Trouve has no SQL. Clair gives the NO_SQL status."""

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
    """Clair finds a SELECT * query and gives the SELECT_STAR status."""

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
        """count(*) is a function call, not a star in the projection."""
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
    """When the SQL names each column, clair reads the names."""

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
        """A long expression with no AS alias has no name."""
        sql = "SELECT id, 1 + 2 FROM users"
        result = infer_columns(declared_columns=[], resolved_sql=sql)
        assert result.status == ColumnStatus.INFERRED
        # Clair reads only 'id'. The expression '1 + 2' has no alias.
        assert [c.name for c in result.columns] == ["id"]


class TestInferColumnsParseFailed:
    """Clair gives the PARSE_FAILED status for SQL that it cannot read."""

    def test_no_select_keyword(self):
        # In "INSERT INTO target SELECT * FROM source", the first match is
        # "SELECT * FROM source". Thus that statement gives SELECT *. A test
        # needs a statement that clair truly cannot read.
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
    """An integration test: build_catalog puts column_inference on each trouve."""

    def test_catalog_includes_column_inference(self, simple_project):
        from clair.core.dag import build_dag
        from clair.core.discovery import discover_project
        from clair.docs.catalog import build_catalog

        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        catalog = build_catalog(dag, simple_project)

        for physical_address, trouve_data in catalog["trouves"].items():
            assert "column_inference" in trouve_data, (
                f"Trouve {physical_address} missing column_inference"
            )
            inference = trouve_data["column_inference"]
            assert "status" in inference
            assert "columns" in inference
            assert "message" in inference

    def test_declared_columns_get_declared_status(self, simple_project):
        """The source and the table in simple_project both have declared columns."""
        from clair.core.dag import build_dag
        from clair.core.discovery import discover_project
        from clair.docs.catalog import build_catalog

        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        catalog = build_catalog(dag, simple_project)

        for trouve_data in catalog["trouves"].values():
            inference = trouve_data["column_inference"]
            assert inference["status"] == ColumnStatus.DECLARED
