# clair — Claude context

A Python-native data transformation framework for Snowflake. Users write Python files
(Trouves). clair compiles them to SQL, builds a DAG from the references between them, and
runs the DAG in topological order. No Jinja, no YAML configuration.

**Start a feature request here, in this order:**

1. `grep` `site_docs/docs/` for the behaviour. It is the source of truth, it is Markdown,
   and it is small. Do not read `src/` to learn what a feature does.
2. Read `.claude/MEMORY.md`. It indexes `.claude/memory/conventions.md`, which holds the
   design invariants, the quality bar for code, and how to write a test.
3. Open only the two or three source files that the map below names.

## Layout

| Path | Holds |
|------|-------|
| `src/clair/` | The package. Tests are in `tests/`. |
| `site_docs/docs/` | User documentation, published by CI. Source of truth for behaviour. |
| `examples/projects/`, `examples/notebooks/` | Runnable examples. |

Packages inside `src/clair/`:

| Package | Holds |
|---------|-------|
| `cli/` | `main.py` — the click entrypoint. Read first for any CLI change. |
| `trouves/` | The domain models: `trouve.py`, `column.py`, `test.py`, `config.py`, `run_config.py`, `_refs.py`. |
| `core/` | The pipeline: `discovery.py`, `dag.py`, `compiler.py`, `runner.py`, `test_runner.py`, `dag_render.py`, `selector.py`, `scaffold.py`. |
| `adapters/` | `base.py` holds the `WarehouseAdapter` ABC. `snowflake.py` is the only implementation. |
| `environments/` | `environments.py` reads `~/.clair/environments.yml`. `routing.py` remaps targets. |
| `web_ui/` | `clair docs` server: `catalog.py`, `columns.py`, `server.py`, bundled SPA in `static/`. |
| (top level) | `api.py` holds the operations: `run`, `compile`, `test`, `docs`, `catalog`. The CLI calls them. `__init__.py` is the public API surface. Also `lineage.py`, `exceptions.py`, `_logging.py`. |

## Tooling: uv and worktrees

Always use `uv run`. Never invoke `.venv/bin/python` or `.venv/bin/pytest` directly.

```bash
uv sync                       # install or update each dependency
uv run clair                  # run the CLI (entrypoint: clair.cli.main:cli)
uv run pytest tests/          # run the tests
uv sync --reinstall           # repair a broken editable install
```

Features go in a git worktree under `.claude/worktrees/<branch-name>/`. A worktree shares
the git history but holds its own `.venv/`, so run `uv venv && uv sync` after you enter a
new one, and run each command from inside the worktree — not from the repo root.

A worktree shares each branch ref with the main checkout. Only the working tree and the
index belong to one worktree. Therefore:

- Branch with a new name: `git checkout -b <feature> origin/main`.
- Never pass `-B`, and never name the branch `main`. `git checkout -B main origin/main`
  moves the shared ref, and the main checkout keeps its old files under a new HEAD. Its
  `git status` then shows a complete reverse diff, which reads like data loss.

## Code

`.claude/memory/conventions.md` holds the quality bar. Two rules that CI cannot catch:

- Descriptive names everywhere in `src/clair/`: `trouve` not `t`, `column_name` not `c`.
- `database_name`, `schema_name`, `table_name` — never `database`, `schema`, `table`.

## CI failures

Replicate each failure locally and iterate until every job passes. Do not push commits to
see if the remote turns green. Each CI job is one command: read `.github/workflows/ci.yml`,
and run the commands locally. Push one commit after they pass.

For a merge conflict, pull main, resolve, and push. Favour simplicity over a clean commit
history — CI squash-merges each PR.

## Backwards compatibility

The major version is 0, clair has no users, and the best design wins against a stable
interface. Change a public name, a file format, or a signature when the change makes the
system better. Add no deprecation shim, no alias, and no migration path. Delete the old
code path — do not keep it beside the new one. Name each behaviour change in the PR
description.

## Documentation

Point to the source of truth. Do not copy it: a copy becomes wrong when the source
changes.

`site_docs/docs/` is the source of truth for behaviour, so update it in the same PR that
changes the CLI (`cli/`), the public API in `src/clair/__init__.py` (`reference/`), or a
behaviour that a guide or a concept page describes. The example code in `site_docs/docs/`
must stay identical to the equivalent code in `examples/projects/`. Change one, change the
other.

## Simplified Technical English

Write every text in Simplified Technical English (ASD-STE100): code comments and
docstrings, CLI output, log messages, error messages, `site_docs/docs/`, `README.md`, this
file, commit messages, and PR descriptions. STE keeps technical text clear for a reader
who does not speak English as a first language.

- Use active verbs. Do not use the passive voice or the present perfect tense.
- Keep sentences short: 20 words for an instruction, 25 for a description.
- Use three nouns in a row maximum.
- Do not use an `-ing` verb form, unless it is part of a technical name.
- Give each word one meaning, and one part of speech.
- Do not use a word with many meanings, such as `check`, `verify`, or `ensure`. Use a
  specific word. Technical terms of this project are permitted: `trouve`, `worktree`,
  `database_name`.

```python
# Bad: "This function is used for checking if the schema has been loaded."
# Good: "This function tells you if the loader read the schema."
```
