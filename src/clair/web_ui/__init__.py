"""clair docs -- the local documentation server for a clair project."""

from clair.web_ui.catalog import build_catalog
from clair.web_ui.columns import ColumnInference, ColumnStatus, infer_columns
from clair.web_ui.server import serve

__all__ = [
    "ColumnInference",
    "ColumnStatus",
    "build_catalog",
    "infer_columns",
    "serve",
]
