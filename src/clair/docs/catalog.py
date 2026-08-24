"""The catalog builder of clair docs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from clair import __version__
from clair.core.dag import ClairDag
from clair.docs.columns import infer_columns
from clair.trouves.trouve import TrouveType


def build_catalog(dag: ClairDag, project_root: Path) -> dict:
    """Make a catalog dict from a compiled DAG.

    Each trouve item holds a ``column_inference`` object with these fields:
    - ``status``: the method that gave the columns, such as declared, inferred,
      or select_star
    - ``columns``: the column list. The user declared it, or clair read it from
      the SQL.
    - ``message``: advice for the UI when a column is absent

    Args:
        dag: A correct ClairDag from build_dag().
        project_root: The absolute path of the project root.

    Returns:
        A dict that clair can write as JSON. The server makes the bytes.
    """
    trouves_catalog: dict[str, dict] = {}

    for physical_name in dag.nodes:
        trouve = dag.get_trouve(physical_name)
        trouve_data = trouve.model_dump(mode="json")

        resolved_sql = (
            trouve.compiled.resolved_sql
            if trouve.compiled and trouve.type != TrouveType.SOURCE
            else None
        )

        inference = infer_columns(
            declared_columns=trouve.columns,
            resolved_sql=resolved_sql,
        )

        trouve_data["column_inference"] = inference.model_dump(mode="json")

        trouves_catalog[physical_name] = trouve_data

    return {
        "project_name": project_root.name,
        "generated_at": datetime.now(UTC).isoformat(),
        "clair_version": __version__,
        "trouves": trouves_catalog,
        "edges": [{"source": source, "target": target} for source, target in dag.edges],
    }
