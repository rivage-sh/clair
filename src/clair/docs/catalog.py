"""Catalog builder for clair docs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from clair import __version__
from clair.core.dag import ClairDag
from clair.docs.columns import infer_columns
from clair.trouves.trouve import TrouveType


def build_catalog(dag: ClairDag, project_root: Path) -> dict:
    """Build a catalog dict from a compiled DAG.

    Each trouve entry includes a ``column_inference`` object with:
    - ``status``: how the columns were determined (declared, inferred, select_star, etc.)
    - ``columns``: the resolved column list (user-declared or best-effort inferred)
    - ``message``: guidance text for the UI when columns are missing or inferred

    Args:
        dag: A validated ClairDag (from build_dag).
        project_root: Absolute path to the project root.

    Returns:
        A JSON-serializable dict. The server serializes this to bytes.
    """
    trouves_catalog: dict[str, dict] = {}

    for full_name in dag.nodes:
        trouve = dag.get_trouve(full_name)
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

        trouves_catalog[full_name] = trouve_data

    return {
        "project_name": project_root.name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "clair_version": __version__,
        "trouves": trouves_catalog,
        "edges": [{"source": source, "target": target} for source, target in dag.edges],
    }
