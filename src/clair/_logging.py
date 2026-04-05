"""Structlog configuration for the Clair CLI."""

from __future__ import annotations

import os
import sys
from collections.abc import MutableMapping
from datetime import datetime
from typing import Any

import click
import structlog

_LEVEL_FG = {"debug": "blue", "info": "green", "warning": "yellow", "error": "red", "critical": "red"}


def _multiline_renderer(_logger: Any, _method: str, event_dict: MutableMapping[str, Any]) -> str:
    level = event_dict.pop("level", "info")
    event = event_dict.pop("event", "")
    event_dict.pop("timestamp", None)  # replaced by our own below

    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]  # millisecond precision
    kv = {"timestamp": ts, **event_dict}

    if sys.stderr.isatty():
        level_str = click.style(f"{level:<8}", fg=_LEVEL_FG.get(level, "white"), bold=True)
        header = f"[{level_str}] {click.style(event, bold=True)}"
        kv_lines = "\n".join(f"  {click.style(k, fg='cyan')}={click.style(str(v), fg='magenta')}" for k, v in kv.items())
    else:
        header = f"[{level:<8}] {event}"
        kv_lines = "\n".join(f"  {k}={v}" for k, v in kv.items())

    return f"{header}\n{kv_lines}" if kv_lines else header


def configure_logging() -> None:
    log_format = os.environ.get("CLAIR_LOG_FORMAT", "").lower()
    use_json = log_format == "json"

    if use_json:
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        )
    else:
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
                _multiline_renderer,
            ],
            wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )
