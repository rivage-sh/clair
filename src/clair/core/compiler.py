"""The compiler. It completes the SQL of each selected Trouve, and it writes no file."""

from __future__ import annotations

import ast
from pathlib import Path

from pydantic import BaseModel

from clair.core.comments import build_comment_statements
from clair.core.dag import ClairDag, addresses_of
from clair.core.runner import resolve_effective_mode, wanted_object_type
from clair.core.staging import (
    build_clone_statement,
    build_cluster_by_statements,
    build_drop_staging_statement,
    build_promote_statements,
    build_type_change_comment,
    make_staging_address,
)
from clair.exceptions import CompileError
from clair.trouves.address import NodeAddresses
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, SqlTrouve, table_options_of
from clair.trouves.trouve_type import TrouveType


class CompiledNodeInfo(BaseModel):
    """The data of one compiled node.

    Attributes:
        addresses: The logical address, the physical address, and the staging
            address of the node. The staging address is None when the plan
            writes to the physical address directly.
        type: TABLE, VIEW or SOURCE.
        execution_type: SNOWFLAKE or PANDAS.
        effective_run_mode: The run mode after clair applied the RunConfig of
            the Trouve to the run mode of the caller.
        dependencies: The physical address of each upstream Trouve.
        sql: The statements, in the order that clair executes them. A pandas
            Trouve has no statements.
        python_source: The Python text of a pandas Trouve: a header that names
            each input, the imports of its module, and its function. None for
            a SQL Trouve.
    """

    addresses: NodeAddresses
    type: TrouveType
    execution_type: ExecutionType
    effective_run_mode: RunMode = RunMode.FULL_REFRESH
    dependencies: list[str]
    sql: list[str]
    python_source: str | None = None


class CompileOutput(BaseModel):
    """The result of one compile operation.

    Attributes:
        trouve_count: The number of Trouves in the DAG.
        source_count: The number of sources in the DAG.
        compiled_nodes: One entry for each Trouve that clair compiled, in the
            order that clair executes them.
        run_id: The UUIDv7 hex string that identifies this compile run.
        project_root: The root directory of the project.
        env_name: The name of the active environment.
        run_mode: The run mode that the caller gave.
        sample_rows: The row limit of a sampled plan. None for a plan that
            reads each row.
    """

    trouve_count: int
    source_count: int
    compiled_nodes: list[CompiledNodeInfo]
    run_id: str = ""
    project_root: Path | None = None
    env_name: str = ""
    run_mode: RunMode = RunMode.FULL_REFRESH
    sample_rows: int | None = None

    def node(self, address: str) -> CompiledNodeInfo | None:
        """Find one compiled node by its logical address or its physical address."""
        for node_info in self.compiled_nodes:
            if node_info.addresses.matches(address):
                return node_info
        return None

    @staticmethod
    def render_header(trouve_count: int, source_count: int, compiled_nodes: list[CompiledNodeInfo]) -> str:
        """Make the text of the compile header and the execution order."""
        lines = [
            "=== Clair Compile ===",
            "",
            (
                f"DAG: {trouve_count} Trouve{'s' if trouve_count != 1 else ''}, "
                f"{source_count} source{'s' if source_count != 1 else ''}"
            ),
            "",
        ]

        if compiled_nodes:
            lines.append("Execution order:")
            for i, node in enumerate(compiled_nodes, 1):
                lines.append(f"  {i}. {node.addresses.physical} ({node.type.value.upper()})")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def render_node(node: CompiledNodeInfo) -> str:
        """Make the output text of one compiled node."""
        lines: list[str] = []
        lines.append(f"--- {node.addresses.physical} ---")
        deps_str = ", ".join(node.dependencies) if node.dependencies else "(none)"
        lines.append(f"Dependencies: {deps_str}")
        lines.append("SQL:")
        for stmt in node.sql:
            for sql_line in stmt.strip().splitlines():
                lines.append(f"  {sql_line}")
            lines.append("")
        return "\n".join(lines)

    def render(self) -> str:
        """Make the complete summary text for stdout."""
        parts = [self.render_header(self.trouve_count, self.source_count, self.compiled_nodes)]

        for node in self.compiled_nodes:
            parts.append(self.render_node(node))

        return "\n".join(parts)


def build_statements(
    trouve: SqlTrouve,
    run_mode: RunMode,
    run_id: str,
    use_staging: bool = True,
) -> list[str]:
    """Make the SQL statements of one Trouve, in the order that clair executes them.

    A staged plan shows each step: the clone for an incremental Trouve, the build
    at the staging address, and the promotion after the tests pass. The plan shows
    the path of a run that passes. A run that fails a test stops after the build,
    and the staging object stays.
    """
    effective_mode = resolve_effective_mode(trouve, run_mode)
    table_options = table_options_of(trouve)

    assert trouve.compiled is not None
    physical_address = trouve.compiled.physical_address

    if not use_staging:
        plan = trouve.build_sql(effective_mode, run_id=run_id, target_address=physical_address)
        return [
            build_type_change_comment(wanted_object_type(trouve), physical_address),
            *plan.statements,
            *build_cluster_by_statements(
                physical_address,
                table_options,
                incremental=effective_mode == RunMode.INCREMENTAL,
            ),
            *build_comment_statements(physical_address, trouve),
            *plan.cleanup_statements,
        ]

    staging_address = make_staging_address(physical_address, run_id)

    statements: list[str] = []
    if effective_mode == RunMode.INCREMENTAL:
        assert table_options is not None, "an incremental Trouve is a Table"
        statements.append(
            build_clone_statement(physical_address, staging_address, table_options=table_options)
        )
    plan = trouve.build_sql(effective_mode, run_id=run_id, target_address=staging_address)
    statements.extend(plan.statements)
    statements.extend(
        build_cluster_by_statements(
            staging_address,
            table_options,
            incremental=effective_mode == RunMode.INCREMENTAL,
        )
    )
    statements.extend(plan.cleanup_statements)

    statements.append("-- staging: the data quality tests run here")
    statements.append(build_type_change_comment(wanted_object_type(trouve), physical_address))
    statements.extend(
        build_promote_statements(
            trouve.type,
            staging_address=staging_address,
            physical_address=physical_address,
            resolved_sql=trouve.full_refresh_sql(),
            table_options=table_options,
        )
    )
    statements.extend(build_comment_statements(physical_address, trouve))
    statements.append(build_drop_staging_statement(trouve.type, staging_address))
    return statements


def build_compile_output(
    dag: ClairDag,
    selected: list[str],
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    use_staging: bool = True,
) -> CompileOutput:
    """Give the compile output of the selected Trouves. This function writes no file.

    A caller that wants the artifacts gives the output to
    ``clair.core.artifacts.write_compile_output``.

    Args:
        dag: The complete project DAG.
        selected: The names to compile, in topological order. The list holds no
            SOURCE Trouve.
        run_mode: The run mode for the new SQL statements.
        run_id: The UUIDv7 hex string that identifies this compile run.
        use_staging: If True, the plan shows the staged path: the build at the
            staging address, the test step, and the promotion. If False, the plan
            writes to the physical address directly.

    Returns:
        A CompileOutput. It holds the data and supplies a .render() method.
    """
    source_count = sum(
        1
        for node in dag.nodes
        if dag.get_trouve(node).type == TrouveType.SOURCE
    )
    trouve_count = len(dag.nodes) - source_count

    compiled_nodes: list[CompiledNodeInfo] = []
    for name in selected:
        trouve = dag.get_trouve(name)
        deps = list(dag.predecessors(name))

        assert trouve.compiled is not None, f"Clair did not compile {name}"
        physical_address = trouve.compiled.physical_address
        addresses = addresses_of(
            dag,
            name,
            make_staging_address(physical_address, run_id) if use_staging else None,
        )

        if isinstance(trouve, DataframeTrouve):
            body_source = trouve.source_text()

            imports_section = ""
            source_file = trouve.source_file()
            if source_file is not None:
                try:
                    source_text = Path(source_file).read_text()
                    tree = ast.parse(source_text)
                    import_lines = [
                        ast.get_source_segment(source_text, node)
                        for node in tree.body
                        if isinstance(node, (ast.Import, ast.ImportFrom))
                    ]
                    import_lines = [line for line in import_lines if line]
                    if import_lines:
                        imports_section = "\n".join(import_lines) + "\n\n"
                except (OSError, SyntaxError):
                    pass

            input_lines = [
                f"#   {parameter_name}  ->  {upstream.require_compiled().physical_address}"
                for parameter_name, upstream in zip(
                    trouve.parameter_names(), trouve.upstream_trouves()
                )
            ]

            header = f"# clair compiled: {trouve.require_compiled().physical_address}\n# execution_type: pandas\n"
            if input_lines:
                header += "# inputs:\n" + "\n".join(input_lines) + "\n"
            header += "\n"
            compiled_nodes.append(CompiledNodeInfo(
                addresses=addresses,
                type=trouve.type,
                execution_type=ExecutionType.PANDAS,
                effective_run_mode=RunMode.FULL_REFRESH,
                dependencies=deps,
                sql=[],
                python_source=header + imports_section + body_source,
            ))
        elif isinstance(trouve, SqlTrouve):
            statements = build_statements(
                trouve, run_mode, run_id, use_staging=use_staging
            )
            compiled_nodes.append(CompiledNodeInfo(
                addresses=addresses,
                type=trouve.type,
                execution_type=ExecutionType.SNOWFLAKE,
                effective_run_mode=resolve_effective_mode(trouve, run_mode),
                dependencies=deps,
                sql=statements,
            ))
        else:
            raise CompileError(f"Clair cannot compile the Trouve class {type(trouve).__name__} of {name}")

    return CompileOutput(
        trouve_count=trouve_count,
        source_count=source_count,
        compiled_nodes=compiled_nodes,
        run_id=run_id,
        run_mode=run_mode,
    )
