"""clair docs -- the local documentation server for a clair project."""

from clair.webui.catalog import build_catalog
from clair.webui.columns import ColumnInference, ColumnStatus, infer_columns
from clair.webui.server import serve

__all__ = [
    "ColumnInference",
    "ColumnStatus",
    "build_catalog",
    "infer_columns",
    "serve",
]
