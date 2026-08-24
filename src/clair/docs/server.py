"""The local HTTP server of clair docs."""

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
    """This handler sends the files in STATIC_DIR and the catalog.

    The catalog is at /api/catalog.json. The server object holds the catalog
    bytes in ``server.catalog_json``. Clair makes those bytes one time only,
    and thus each request is fast.
    """

    server: CatalogServer  # This handler operates only with a CatalogServer.

    def do_GET(self) -> None:
        if self.path == "/api/catalog.json":
            self._serve_catalog()
        elif self.path == "/" or not self._static_file_exists():
            # If the path is not a file in STATIC_DIR, send index.html. Thus a
            # link to a page in the application can operate in the future.
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
        """Tell you if the request path points to a true file in STATIC_DIR."""
        candidate = STATIC_DIR / self.path.lstrip("/")
        # Stop a path that goes out of STATIC_DIR.
        try:
            candidate.resolve().relative_to(STATIC_DIR.resolve())
        except ValueError:
            return False
        return candidate.is_file()

    def log_message(self, format: str, *args) -> None:
        """Stop the default log output on stderr. Clair uses structlog."""


class CatalogServer(HTTPServer):
    """An HTTPServer subclass that holds the catalog bytes."""

    catalog_json: bytes


def serve(
    catalog: dict,
    *,
    host: str = "127.0.0.1",
    port: int = 8741,
    open_browser: bool = True,
) -> None:
    """Start the docs server. The function stops at Ctrl+C.

    Args:
        catalog: The catalog dict from build_catalog().
        host: The address of the server.
        port: The port of the server.
        open_browser: True if clair must open the default browser of the user.
    """
    catalog_bytes = json.dumps(
        catalog, separators=(",", ":")
    ).encode("utf-8")

    server = CatalogServer((host, port), CatalogHandler)
    server.catalog_json = catalog_bytes

    url = f"http://{host}:{port}"
    logger.info("docs.serving", url=url)

    if open_browser:
        # Use a thread. Thus the browser does not delay the start of the server.
        threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        logger.info("docs.stopped")
