"""The compiler. It completes the SQL and writes the compile output."""

from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel

from clair.core.dag import ClairDag
from clair.core.discovery import ARTIFACTS_DIR_NAME
from clair.core.runner import resolve_effective_mode
from clair.core.staging import (
    build_clone_statement,
    build_drop_staging_statement,
    build_promote_statement,
    make_staging_address,
)
from clair.exceptions import CompileError
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, Trouve, TrouveType


class CompiledNodeInfo(BaseModel):
    """The data of one compiled node.

    Attributes:
        name: The physical address, as the DAG keys the node.
        logical_address: The name that the file path gives.
        physical_address: The name that clair writes to. A routing entry makes
            it from the logical address.
        staging_address: The run-scoped address that a staged plan builds at. It
            is None when the plan writes to the physical address directly.
        type: TABLE, VIEW or SOURCE.
        execution_type: SNOWFLAKE or PANDAS.
        effective_run_mode: The run mode after clair applied the RunConfig of
            the Trouve to the run mode of the caller.
        dependencies: The physical address of each upstream Trouve.
        sql: The statements, in the order that clair executes them. A pandas
            Trouve has no statements.
        artifact_path: The file that holds the compiled text.
    """

    name: str
    logical_address: str = ""
    physical_address: str = ""
    staging_address: str | None = None
    type: str
    execution_type: ExecutionType
    effective_run_mode: RunMode = RunMode.FULL_REFRESH
    dependencies: list[str]
    sql: list[str]
    artifact_path: Path | None = None


class CompileOutput(BaseModel):
    """The result of one compile operation.

    Attributes:
        trouve_count: The number of Trouves in the DAG.
        source_count: The number of sources in the DAG.
        compiled_nodes: One entry for each Trouve that clair compiled, in the
            order that clair executes them.
        artifacts_dir: The directory that holds the compiled text.
        run_id: The UUIDv7 hex string that identifies this compile run.
        project_root: The root directory of the project.
        env_name: The name of the active environment.
        run_mode: The run mode that the caller gave.
    """

    trouve_count: int
    source_count: int
    compiled_nodes: list[CompiledNodeInfo]
    artifacts_dir: Path
    run_id: str = ""
    project_root: Path | None = None
    env_name: str = ""
    run_mode: RunMode = RunMode.FULL_REFRESH

    def node(self, address: str) -> CompiledNodeInfo | None:
        """Find one compiled node by its logical address or its physical address."""
        for node_info in self.compiled_nodes:
            if address in (node_info.name, node_info.logical_address, node_info.physical_address):
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
                lines.append(f"  {i}. {node.name} ({node.type})")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def render_node(node: CompiledNodeInfo) -> str:
        """Make the output text of one compiled node."""
        lines: list[str] = []
        lines.append(f"--- {node.name} ---")
        deps_str = ", ".join(node.dependencies) if node.dependencies else "(none)"
        lines.append(f"Dependencies: {deps_str}")
        lines.append("SQL:")
        for stmt in node.sql:
            for sql_line in stmt.strip().splitlines():
                lines.append(f"  {sql_line}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def render_footer(artifacts_dir: Path) -> str:
        """Make the last line of the compile summary."""
        return f"Clair wrote the compiled SQL to {artifacts_dir}/"

    def render(self) -> str:
        """Make the complete summary text for stdout."""
        parts = [self.render_header(self.trouve_count, self.source_count, self.compiled_nodes)]

        for node in self.compiled_nodes:
            parts.append(self.render_node(node))

        parts.append(self.render_footer(self.artifacts_dir))

        return "\n".join(parts)


def build_statements(
    trouve: Trouve,
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

    if not use_staging:
        return trouve.build_sql(effective_mode, run_id=run_id)

    assert trouve.compiled is not None
    physical_address = trouve.compiled.physical_address
    staging_address = make_staging_address(physical_address, run_id)

    statements: list[str] = []
    if effective_mode == RunMode.INCREMENTAL:
        statements.append(build_clone_statement(physical_address, staging_address))
    statements.extend(
        trouve.build_sql(effective_mode, run_id=run_id, staging_address=staging_address)
    )

    statements.append("-- staging: the data quality tests run here")
    statements.append(
        build_promote_statement(
            trouve.type,
            staging_address=staging_address,
            physical_address=physical_address,
            resolved_sql=trouve.compiled.resolved_sql,
        )
    )
    statements.append(build_drop_staging_statement(trouve.type, staging_address))
    return statements


def write_compile_output(
    dag: ClairDag,
    selected: list[str],
    project_root: Path,
    on_node_compiled: Callable[[CompiledNodeInfo], None] = lambda _: None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    use_staging: bool = True,
) -> CompileOutput:
    """Write the compiled SQL to _clairtifacts/<run_id>/ and give the output.

    Args:
        dag: The complete project DAG.
        selected: The names to compile, in topological order. The list holds no
            SOURCE Trouve.
        project_root: The root directory of the project.
        on_node_compiled: A callback. Clair calls it after it compiles a node
            and writes the node to the disk. Thus the caller can show the
            output immediately.
        run_mode: The run mode for the new SQL statements.
        run_id: The UUIDv7 hex string that identifies this compile run.
        use_staging: If True, the plan shows the staged path: the build at the
            staging address, the test step, and the promotion. If False, the plan
            writes to the physical address directly.

    Returns:
        A CompileOutput. It holds the data and supplies a .render() method.
    """
    artifacts_dir = project_root / ARTIFACTS_DIR_NAME / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)

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
        # The artifact path follows the physical address: one directory for the
        # database name, one for the schema name, and the table name as the file.
        physical_address = trouve.compiled.physical_address
        artifact_directory = (
            artifacts_dir
            / physical_address.database_name
            / physical_address.schema_name
        )
        staging_address = (
            str(make_staging_address(physical_address, run_id)) if use_staging else None
        )

        node_info = None
        if trouve.compiled.execution_type == ExecutionType.PANDAS:
            assert isinstance(trouve, DataframeTrouve)
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
                f"#   {parameter_name}  ->  {upstream.physical_address}"
                for parameter_name, upstream in zip(
                    trouve.parameter_names(), trouve.upstream_trouves()
                )
            ]

            header = f"# clair compiled: {trouve.physical_address}\n# execution_type: pandas\n"
            if input_lines:
                header += "# inputs:\n" + "\n".join(input_lines) + "\n"
            header += "\n"
            artifact_content = header + imports_section + body_source

            artifact_path = artifact_directory / f"{physical_address.table_name}.py"

            node_info = CompiledNodeInfo(
                name=name,
                logical_address=str(trouve.compiled.logical_address),
                physical_address=str(physical_address),
                staging_address=staging_address,
                type=trouve.type.value.upper(),
                execution_type=ExecutionType.PANDAS,
                effective_run_mode=RunMode.FULL_REFRESH,
                dependencies=deps,
                sql=[],
                artifact_path=artifact_path,
            )
            compiled_nodes.append(node_info)

            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(artifact_content)
        elif trouve.compiled.execution_type == ExecutionType.SNOWFLAKE:
            assert isinstance(trouve, Trouve)
            statements = build_statements(
                trouve, run_mode, run_id, use_staging=use_staging
            )

            sql_file = artifact_directory / f"{physical_address.table_name}.sql"

            node_info = CompiledNodeInfo(
                name=name,
                logical_address=str(trouve.compiled.logical_address),
                physical_address=str(physical_address),
                staging_address=staging_address,
                type=trouve.type.value.upper(),
                execution_type=ExecutionType.SNOWFLAKE,
                effective_run_mode=resolve_effective_mode(trouve, run_mode),
                dependencies=deps,
                sql=statements,
                artifact_path=sql_file,
            )
            compiled_nodes.append(node_info)

            sql_file.parent.mkdir(parents=True, exist_ok=True)
            sql_content = "\n\n---\n\n".join(s.strip() for s in statements)
            sql_file.write_text(sql_content + "\n")
        else:
            raise CompileError(f"Clair does not know the execution_type '{trouve.compiled.execution_type}' for {name}")

        on_node_compiled(node_info)

    return CompileOutput(
        trouve_count=trouve_count,
        source_count=source_count,
        compiled_nodes=compiled_nodes,
        artifacts_dir=artifacts_dir,
        run_id=run_id,
        project_root=project_root,
        run_mode=run_mode,
    )
