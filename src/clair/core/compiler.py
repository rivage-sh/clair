"""The compiler. It completes the SQL of each selected Trouve, and it writes no file."""

from __future__ import annotations

import ast
from pathlib import Path

from pydantic import BaseModel

from clair.adapters.objects import WarehouseObject
from clair.core.dag import ClairDag, addresses_of
from clair.core.staging import make_staging_address
from clair.exceptions import CompileError
from clair.trouves.address import NodeAddresses
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.plan import NodePlan, PlanContext
from clair.trouves.run_config import RunMode
from clair.trouves.statements import type_change_comment
from clair.trouves.trouve import ExecutionType, MaterializedTrouve, SqlTrouve
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


def offline_plan_context(
    trouve: MaterializedTrouve, run_mode: RunMode, run_id: str, use_staging: bool = True
) -> PlanContext:
    """Give the context of a plan that clair makes with no connection.

    ``clair compile`` and ``clair validate`` read nothing from the warehouse.
    They plan the run of a project whose objects exist with the type of each
    Trouve, thus an incremental Trouve shows its incremental plan.
    """
    physical_address = trouve.require_compiled().physical_address
    assumed_object = WarehouseObject(object_type=trouve.warehouse_object_type)
    return PlanContext(
        run_id=run_id,
        effective_mode=trouve.effective_mode(run_mode, assumed_object),
        staged=use_staging,
        offline=True,
        physical_address=physical_address,
        staging_address=make_staging_address(physical_address, run_id) if use_staging else None,
    )


# The text after the name of a phase in its header, when the condition is not "always".
_PHASE_CONDITIONS = {
    "test": "a blocking test that fails stops the promotion",
    "promote": "when should_promote() gives True",
    "retire": "after the promotion",
    "cleanup": "always, also after a failure",
}


def _phase_header(phase_name: str) -> str:
    """Give the comment line that names one phase in the compiled SQL."""
    condition = _PHASE_CONDITIONS.get(phase_name)
    label = phase_name if condition is None else f"{phase_name} · {condition}"
    return f"-- ── {label} " + "─" * max(4, 56 - len(label))


def render_plan(trouve: MaterializedTrouve, context: PlanContext, plan: NodePlan) -> list[str]:
    """Give the statements of a plan in the order that clair executes them, with a header for each phase.

    The header goes on the first statement of its phase, thus each item stays
    one statement. A phase with no statement gets no header.
    """
    type_comment = type_change_comment(trouve.warehouse_object_type, context.physical_address)
    test_statements = [
        f"-- {query.label}"
        + (f" ({query.column_name})" if query.column_name else "")
        + f", {query.severity.value.upper()}\n{query.sql}"
        for query in plan.test
    ]
    phases: list[tuple[str, list[str]]] = [
        ("prepare", plan.prepare),
        ("build", plan.build),
        ("configure", plan.configure),
    ]
    if context.staged:
        phases += [("test", test_statements), ("promote", plan.promote)]
    phases += [("document", plan.document), ("retire", plan.retire), ("cleanup", plan.cleanup)]

    statements: list[str] = []
    # The type test comes before the first statement that writes production:
    # the promotion of a staged run, and the first statement of a run without one.
    type_comment_phase = "promote" if context.staged else next(
        (phase_name for phase_name, phase_statements in phases if phase_statements), ""
    )
    for phase_name, phase_statements in phases:
        for statement_index, sql in enumerate(phase_statements):
            if statement_index == 0:
                header = _phase_header(phase_name)
                if phase_name == type_comment_phase:
                    header = f"{header}\n{type_comment}"
                sql = f"{header}\n{sql}"
            statements.append(sql)
    return statements


def build_statements(
    trouve: SqlTrouve,
    run_mode: RunMode,
    run_id: str,
    use_staging: bool = True,
) -> list[str]:
    """Make the SQL statements of one Trouve, in the order that clair executes them.

    The plan shows the path of a run that passes. A run that fails a test stops
    after the tests, and the staging object stays.
    """
    context = offline_plan_context(trouve, run_mode, run_id, use_staging)
    return render_plan(trouve, context, trouve.plan(context))


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
                effective_run_mode=offline_plan_context(trouve, run_mode, run_id, use_staging).effective_mode,
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
