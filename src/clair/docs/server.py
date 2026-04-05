"""Local HTTP server for clair docs."""

from __future__ import annotations

import json
import mimetypes
import threading
import webbrowser
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import structlog

logger = structlog.get_logger()

STATIC_DIR = Path(__file__).parent / "static"


class CatalogHandler(SimpleHTTPRequestHandler):
    """Serves static files from STATIC_DIR and the catalog at /api/catalog.json.

    The catalog bytes are attached to the server instance as ``server.catalog_json``
    (a pre-serialized bytes object) to avoid re-serializing on every request.
    """

    server: CatalogServer  # this handler is only used with CatalogServer

    def do_GET(self) -> None:
        if self.path == "/api/catalog.json":
            self._serve_catalog()
        elif self.path == "/" or not self._static_file_exists():
            # SPA fallback: serve index.html for any path that doesn't
            # match a static file. This supports potential future deep linking.
            self._serve_file("index.html")
        else:
            self._serve_file(self.path.lstrip("/"))

    def _serve_catalog(self) -> None:
        body = self.server.catalog_json
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, relative_path: str) -> None:
        file_path = STATIC_DIR / relative_path
        if not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = file_path.read_bytes()
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _static_file_exists(self) -> bool:
        """Check if the request path maps to an actual file in STATIC_DIR."""
        candidate = STATIC_DIR / self.path.lstrip("/")
        # Prevent directory traversal
        try:
            candidate.resolve().relative_to(STATIC_DIR.resolve())
        except ValueError:
            return False
        return candidate.is_file()

    def log_message(self, format: str, *args) -> None:
        """Suppress default stderr logging -- we use structlog."""
        pass


class CatalogServer(HTTPServer):
    """HTTPServer subclass that carries the pre-serialized catalog."""

    catalog_json: bytes


def serve(
    catalog: dict,
    *,
    host: str = "127.0.0.1",
    port: int = 8741,
    open_browser: bool = True,
) -> None:
    """Start the docs server. Blocks until Ctrl+C.

    Args:
        catalog: The catalog dict from build_catalog().
        host: Bind address.
        port: Bind port.
        open_browser: Whether to open the user's default browser.
    """
    catalog_bytes = json.dumps(
        catalog, separators=(",", ":")
    ).encode("utf-8")

    server = CatalogServer((host, port), CatalogHandler)
    server.catalog_json = catalog_bytes

    url = f"http://{host}:{port}"
    logger.info("docs.serving", url=url)

    if open_browser:
        # Open in a thread so it doesn't delay the server start
        threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        logger.info("docs.stopped")
