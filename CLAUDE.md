# clair — Claude context

## Layout

| Package | Source | Tests |
|---------|--------|-------|
| `clair` | `src/clair/` | `tests/` |

## Tooling: uv

This project uses **uv**. Always use `uv run` — never invoke `.venv/bin/python` or `.venv/bin/pytest` directly.

```bash
uv run clair                              # run the clair CLI
uv sync                                   # install/update all deps
uv run pytest tests/                     # run clair tests
```

If the editable install seems broken: `uv sync --reinstall`

## Worktrees

Features are developed in git worktrees under `.claude/worktrees/<branch-name>/`. Each worktree is an isolated checkout — run all commands from within the worktree directory, not the repo root.

```bash
# Inside a worktree:
uv venv
uv sync
uv run pytest tests/
```

The worktree shares git history with the main repo but has its own `.venv/`. Always `uv sync` after entering a new worktree.

## CLI entrypoints

- `clair = "clair.cli.main:cli"` — installed to `.venv/bin/clair` after `uv sync`

## Coding guidelines

- Use descriptive variable names for all code in `src/clair/`
  - e.g. `trouve` instead of `t`
- Use `database_name` instead of `database`, `schema_name` instead of `schema`, `table_name` instead of `table`
- Address git merge conflicts by pulling main, resolving conflicts, and pushing. Favour simplicity over clean commit history — PRs are squash-merged anyway.
