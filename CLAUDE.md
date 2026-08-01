# clair — Claude context

## What clair is

A Python-native data transformation framework for Snowflake. Users write Python files
(Trouves). clair compiles them to SQL, builds a DAG from the references between them, and
runs the DAG in topological order. No Jinja, no YAML configuration.

**Start a feature request here, in this order:**

1. `grep` `site_docs/docs/` for the behaviour. It is the source of truth, it is Markdown,
   and it is small. Do not read `src/` to learn what a feature does.
2. Read `.claude/MEMORY.md` for the design rules.
3. Open only the two or three source files that the map below names.

## Layout

| Path | Holds |
|------|-------|
| `src/clair/` | The package. Tests are in `tests/`. |
| `site_docs/docs/` | User documentation, published by CI. Source of truth for behaviour. |
| `example_projects/`, `example_notebooks/` | Runnable examples. |

Packages inside `src/clair/`:

| Package | Holds |
|---------|-------|
| `cli/` | `main.py` — the click entrypoint. Read first for any CLI change. |
| `trouves/` | The domain models: `trouve.py`, `column.py`, `test.py`, `config.py`, `run_config.py`, `_refs.py`. |
| `core/` | The pipeline: `discovery.py`, `dag.py`, `compiler.py`, `runner.py`, `test_runner.py`, `dag_render.py`, `selector.py`, `scaffold.py`. |
| `adapters/` | `base.py` holds the `WarehouseAdapter` ABC. `snowflake.py` is the only implementation. |
| `environments/` | `environments.py` reads `~/.clair/environments.yml`. `routing.py` remaps targets. |
| `docs/` | `clair docs` server: `catalog.py`, `columns.py`, `server.py`, bundled SPA in `static/`. |
| (top level) | `__init__.py` is the public API surface. Also `lineage.py`, `exceptions.py`, `_logging.py`. |

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

## Comments: Simplified Technical English

Write all code comments and docstrings in Simplified Technical English (ASD-STE100). STE is a controlled writing standard that makes technical text clear and unambiguous for readers who do not speak English as a first language.

Grammar rules:

- Use active verbs, not passive ones.
- Keep sentences short: 20 words maximum for instructions, 25 words maximum for descriptions.
- Use three nouns in a row maximum.
- Do not use `-ing` verb forms, unless they are part of a technical name.
- Do not use the present perfect tense.

Vocabulary rules:

- Give each word one meaning only.
- Use each word as one part of speech only (a noun or a verb, not both).
- Do not use words with many meanings or unclear actions, such as `check`, `verify`, or `ensure`. Use a specific word instead.
- Technical terms for this project are permitted, such as `trouve`, `database_name`, or `worktree`.

Examples:

```python
# Bad: "This function is used for checking if the schema has been loaded."
# Good: "This function tells you if the loader read the schema."
```
