"""Compiler -- resolve SQL and produce compile output."""

from __future__ import annotations

import ast
import inspect
import os
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel

from clair.core.dag import ClairDag
from clair.core.discovery import ARTIFACTS_DIR_NAME
from clair.core.runner import resolve_effective_mode
from clair.core.strict import (
    build_clone_statement,
    build_promote_statements,
    strict_staging_name,
)
from clair.exceptions import CompileError
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, Trouve, TrouveType


class CompiledNodeInfo(BaseModel):
    """Structured info about a single compiled node."""

    name: str
    type: str
    execution_type: ExecutionType
    dependencies: list[str]
    sql: list[str]


class CompileOutput(BaseModel):
    """Structured result of a compile operation."""

    trouve_count: int
    source_count: int
    compiled_nodes: list[CompiledNodeInfo]
    artifacts_dir: Path

    @staticmethod
    def render_header(trouve_count: int, source_count: int, compiled_nodes: list[CompiledNodeInfo]) -> str:
        """Render the compile header and execution order."""
        lines = [
            "=== Clair Compile ===",
            "",
            f"DAG: {trouve_count} Trouve{'s' if trouve_count != 1 else ''}, "
            f"{source_count} source{'s' if source_count != 1 else ''}",
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
        """Render the output for a single compiled node."""
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
        """Render the final compile summary line."""
        return f"Compiled SQL written to {artifacts_dir}/"

    def render(self) -> str:
        """Produce the formatted summary string for stdout."""
        parts = [self.render_header(self.trouve_count, self.source_count, self.compiled_nodes)]

        for node in self.compiled_nodes:
            parts.append(self.render_node(node))

        parts.append(self.render_footer(self.artifacts_dir))

        return "\n".join(parts)


def build_statements(
    trouve: Trouve,
    run_mode: RunMode,
    run_id: str,
    strict: bool = False,
) -> list[str]:
    """Build the SQL statements for one Trouve, as they would be executed.

    Under strict mode the plan is shown end to end: the optional clone, the build
    into the staging object, and the promotion that follows a passing test run.
    The runner decides between SWAP and RENAME by checking whether the target
    exists; compile has no connection, so it shows the SWAP form.
    """
    effective_mode = resolve_effective_mode(trouve, run_mode)

    if not strict:
        return trouve.build_sql(effective_mode, run_id=run_id)

    target_name = trouve.full_name
    staging_name = strict_staging_name(target_name, run_id)

    statements: list[str] = []
    if effective_mode == RunMode.INCREMENTAL:
        statements.append(build_clone_statement(target_name, staging_name))
    statements.extend(trouve.build_sql(effective_mode, run_id=run_id, target_name=staging_name))

    assert trouve.compiled is not None
    statements.append("-- strict: tests run against the staging object here")
    statements.extend(
        build_promote_statements(
            trouve.type,
            staging_name=staging_name,
            target_name=target_name,
            target_exists=True,
            resolved_sql=trouve.compiled.resolved_sql,
        )
    )
    return statements


def write_compile_output(
    dag: ClairDag,
    selected: list[str],
    project_root: Path,
    on_node_compiled: Callable[[CompiledNodeInfo], None] = lambda _: None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    strict: bool = False,
) -> CompileOutput:
    """Write compiled SQL to _clairtifacts/<run_id>/ and return a structured output.

    Args:
        dag: The full project DAG.
        selected: Ordered list of full_names to compile (non-SOURCE, topological order).
        project_root: The project root directory.
        on_node_compiled: Callback invoked after each node is compiled and written
            to disk, allowing callers to stream output.
        run_mode: The run mode to use when generating SQL statements.
        run_id: UUIDv7 hex string identifying this compile run.
        strict: When True, emit the full strict-mode plan (staging build, test
            checkpoint, promotion) rather than a direct write to the target.

    Returns:
        A CompileOutput with structured data and a .render() method.
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

        assert trouve.compiled is not None, f"{name} has not been compiled"
        node_info = None
        if trouve.compiled.execution_type == ExecutionType.PANDAS:
            try:
                fn_source = inspect.getsource(trouve.df_fn)
            except (OSError, TypeError):
                # Source unavailable for lambdas, built-ins, or compiled extensions
                fn_source = repr(trouve.df_fn)

            imports_section = ""
            try:
                source_file = inspect.getfile(trouve.df_fn)
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

            input_lines = []
            for param in inspect.signature(trouve.df_fn).parameters.values():
                if isinstance(param.default, Trouve):
                    input_lines.append(f"#   {param.name}  ->  {param.default.full_name}")

            header = "# clair compiled: {}\n# execution_type: pandas\n".format(trouve.full_name)
            if input_lines:
                header += "# inputs:\n" + "\n".join(input_lines) + "\n"
            header += "\n"
            artifact_content = header + imports_section + fn_source

            node_info = CompiledNodeInfo(
                name=name,
                type=trouve.type.value.upper(),
                execution_type=ExecutionType.PANDAS,
                dependencies=deps,
                sql=[],
            )
            compiled_nodes.append(node_info)

            parts = name.split(".")
            artifact_path = artifacts_dir / os.sep.join(parts[:-1]) / f"{parts[-1]}.py"
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(artifact_content)
        elif trouve.compiled.execution_type == ExecutionType.SNOWFLAKE:
            statements = build_statements(trouve, run_mode, run_id, strict=strict)

            node_info = CompiledNodeInfo(
                name=name,
                type=trouve.type.value.upper(),
                execution_type=ExecutionType.SNOWFLAKE,
                dependencies=deps,
                sql=statements,
            )
            compiled_nodes.append(node_info)

            parts = name.split(".")
            sql_file = artifacts_dir / os.sep.join(parts[:-1]) / f"{parts[-1]}.sql"
            sql_file.parent.mkdir(parents=True, exist_ok=True)
            sql_content = "\n\n---\n\n".join(s.strip() for s in statements)
            sql_file.write_text(sql_content + "\n")
        else:
            raise CompileError(f"Unknown execution_type '{trouve.compiled.execution_type}' for {name}")

        on_node_compiled(node_info)

    return CompileOutput(
        trouve_count=trouve_count,
        source_count=source_count,
        compiled_nodes=compiled_nodes,
        artifacts_dir=artifacts_dir,
    )
