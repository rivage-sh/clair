---
name: Run commands with uv
description: Always use uv run for running clair commands, pytest, and python. Never use .venv/bin/python directly.
type: feedback
---

Use `uv run` to execute all project commands:
- `uv run pytest tests/` to run tests
- `uv run clair <subcommand>` to run CLI
- `uv run python -c "..."` to run Python snippets

Do NOT use `.venv/bin/python` or `.venv/bin/pytest` directly -- the user corrected this explicitly.
If the editable install is broken, run `uv pip install --reinstall -e .` first.
