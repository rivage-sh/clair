# Shared Team Memory

Cross-cutting context all agents should know about clair.

## What clair is

Python-native data transformation framework for Snowflake. Users write Python files (Trouves); clair compiles them to SQL, builds a DAG from Python import references, and runs in topological order. No Jinja, no YAML config, full IDE support.

**Strategic angle:** dbt (Fivetran) and SQLMesh (Fivetran) are both owned by Fivetran. Clair is the independent alternative. Target user: Python-first analytics engineers, 3–15 person teams, 100–500 dbt models on Snowflake, hitting dbt's macro ceiling.

## In Progress
- **PandasTrouve** — arch spec at `.claude/specs/pandas_trouve_arch.md`. Separate class (not a Trouve subclass), inputs via dict, pandas optional dep, full-refresh only for v1.

## Design invariants — never violate these

1. No Jinja, ever. SQL is plain Python f-strings.
2. No YAML for config. Auth only (`~/.clair/environments.yml`). All other config is Python.
3. File path is the source of truth for `database.schema.table`. Three levels deep from project root.
4. `clair compile` never connects to Snowflake — it is a pure local operation.
5. Eager validation: invalid Trouve construction raises immediately, not at run time.
6. Shared SQL logic = regular Python functions, imported normally.
7. Adapter is abstracted via ABC (`WarehouseAdapter`) — designed for future multi-warehouse without rewriting runner.

## Key files — where to look

Read the code; don't rely on memory for specifics. These are the entry points:

- `src/clair/__init__.py` — public API surface. Check here for what's exported.
- `src/clair/cli/main.py` — CLI (init, compile, run, test, dag, clean, docs). Start here for any CLI change.
- `src/clair/trouves/trouve.py` — Trouve model + `build_sql()`. Core domain object.
- `src/clair/trouves/run_config.py` — RunMode, IncrementalMode, UpsertConfig, RunConfig.
- `src/clair/core/discovery.py` — walks project, loads Trouves, resolves SQL placeholders.
- `src/clair/core/dag.py` — ClairDag (DiGraph subclass), build_dag(), topological sort.
- `src/clair/core/runner.py` — run_project(), RunResult. Sequential, partial failure.
- `src/clair/core/compiler.py` — write_compile_output() → stdout + `_clairtifacts/<run_id>/`.
- `src/clair/core/test_runner.py` — run_tests(), TestResult. Zero rows = pass.
- `src/clair/core/dag_render.py` — render_dag(), pure function, plain text, upstream-only.
- `src/clair/core/selector.py` — fnmatch-based `--select` filtering on dotted full_names.
- `src/clair/docs/` — catalog.py (build_catalog), server.py (stdlib HTTP, port 8741).
- `src/clair/auth/environments.py` — load_environment() from `~/.clair/environments.yml`.
- `src/clair/adapters/base.py` — WarehouseAdapter ABC, QueryResult.
- `src/clair/_logging.py` — structlog config (multiline renderer, click colors, ms timestamps, stderr).
- `src/clair/exceptions.py` — ClairError hierarchy.
- `src/clair/lineage.py` — get_dag() public API.
- `tests/` — unit tests never connect to Snowflake (adapter mocked). Integration tests opt-in via `@pytest.mark.integration` + `CLAIR_TEST_PROFILE` env var.

## Placeholder system (how cross-Trouve SQL works)

`Trouve.__format__` is overridden: `f"SELECT * FROM {other_trouve}"` emits a token like `__CLAIR_TROUVE_<id>__`. After all modules load, discovery resolves these tokens to real full_names. This avoids context variables, frame inspection, and import-order dependencies.

## Environments (not profiles)

Auth and routing live in `~/.clair/environments.yml`. The CLI flag is `--env` (was `--profile`). Two routing policies:
- `database_override` — swap the database, keep schema/table
- `schema_isolation` — collapse all Trouves into one `{db}_{schema}_{table}` name in a personal schema

SOURCEs are never routed. Default env name is `"dev"`. Full spec: `specs/environments.md`.

## Routing collisions

Two Trouves routing to the same target = **warning, not hard error**. Collisions only occur when routing is active (dev-time concept), so blocking the run is disproportionate. All collisions reported upfront. `--strict` to escalate is deferred to v2.

## Working preferences

- **One PR at a time.** After merging, check `git status` on main — if stray files remain, create a housekeeping PR immediately.
- **All memory goes in the repo.** Keep memory in `.claude/agent-memory/` (committed), not in `~/.claude/` (local only).
- **Tests assert on Pydantic fields, not formatted strings.** Functions return Pydantic objects; tests check `.field` values, never string output.
- **Always `uv run`.** Never `.venv/bin/python` or `.venv/bin/pytest` directly.
