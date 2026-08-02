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

## CI failures

If CI fails, replicate the failure locally and iterate until every job passes. Do not push
commits to see if the remote turns green. Each CI job is one simple command: read
`.github/workflows/ci.yml` for the current commands, and run them locally.

Push one commit after the commands pass locally.

## Coding guidelines

- Use descriptive variable names for all code in `src/clair/`
  - e.g. `trouve` instead of `t`
- Use `database_name` instead of `database`, `schema_name` instead of `schema`, `table_name` instead of `table`
- Address git merge conflicts by pulling main, resolving conflicts, and pushing. Favour simplicity over clean commit history — PRs are squash-merged anyway.

## Backwards compatibility

The major version is 0. While the major version stays 0, clair does not keep backwards
compatibility. clair has no users at this time, so the best design wins against a stable
interface.

- Change a public name, a file format, or a function signature when the change makes the
  system better.
- Do not add a deprecation shim, an alias for an old name, or a migration path.
- Delete the old code path. Do not keep it beside the new one.
- Name each behaviour change in the pull request description.

## Keep site_docs/ up to date

`site_docs/docs/` is the source of truth for behaviour. Before you complete a change, compare
it against `site_docs/docs/` and update the pages that the change makes wrong.

Do this for every change to:

- The CLI: its commands, options, and output. See `site_docs/docs/cli/`.
- The public API in `src/clair/__init__.py`. See `site_docs/docs/reference/`.
- A behaviour that a guide or a concept page describes.

The example code in `site_docs/docs/` must stay identical to the equivalent code in
`example_projects/`. If you change one, change the other.

## Documentation: point to the source of truth

When you write documentation, point to the source of truth. Do not copy it. A copy becomes
wrong when the source changes, and many copies are difficult to maintain.

## Simplified Technical English

Write all text that you generate in Simplified Technical English (ASD-STE100). STE is a controlled writing standard that makes technical text clear and unambiguous for readers who do not speak English as a first language.

This rule applies to every text that you write:

- Code comments and docstrings.
- User-facing strings: CLI output, log messages, and error messages.
- Documentation in `site_docs/docs/`, `README.md`, and this file.
- Commit messages and pull request descriptions.

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
