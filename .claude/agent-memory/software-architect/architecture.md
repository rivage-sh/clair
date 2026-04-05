---
name: Clair architecture
description: Key design decisions and file pointers for clair — read the code for specifics, use this for orientation and design rationale
type: project
---

# Clair Architecture

## Module map (entry points)

```
src/clair/
  __init__.py              # Public exports — authoritative list of what users import
  trouves/
    trouve.py              # Trouve(BaseModel), TrouveType, CompiledAttributes, build_sql()
    column.py              # Column(BaseModel), ColumnType (str subclass)
    test.py                # Test subclasses: TestUnique, TestNotNull, TestRowCount, TestUniqueColumns
    config.py              # DatabaseDefaults, SchemaDefaults, ResolvedConfig
    run_config.py          # RunMode, IncrementalMode, UpsertConfig, RunConfig
    _refs.py               # Placeholder registry (Trouve.__format__ token system)
  core/
    discovery.py           # Walk project, load modules, resolve placeholders, detect imports
    dag.py                 # ClairDag(DiGraph), build_dag(), cycle detection, topo sort
    compiler.py            # write_compile_output() → stdout + _clairtifacts/<run_id>/
    runner.py              # run_project() (sequential, partial failure), RunResult
    test_runner.py         # run_tests(), TestResult. Zero rows = pass.
    dag_render.py          # render_dag() — pure function, plain text, upstream-only --select
    selector.py            # fnmatch --select filtering on dotted full_names
    scaffold.py            # scaffold_project(), write_environments_yml()
  adapters/
    base.py                # WarehouseAdapter ABC, QueryResult
    snowflake.py           # SnowflakeAdapter
  auth/
    environments.py        # load_environment() from ~/.clair/environments.yml
  docs/
    catalog.py             # build_catalog(dag, root) → dict
    server.py              # CatalogHandler (stdlib http.server), serve() — port 8741
    static/                # Bundled SPA
  cli/
    main.py                # click entrypoint — read this first for any CLI change
  _logging.py              # structlog: multiline key=value, click colors, ms timestamps, stderr
  exceptions.py            # ClairError hierarchy
  lineage.py               # get_dag() public API → ClairDag
```

## Key design decisions

### Placeholder token system (how cross-Trouve SQL works)
`Trouve.__format__` is overridden. `f"SELECT * FROM {other_trouve}"` registers a token `__CLAIR_TROUVE_<id>__` via `_refs.py`. The SQL stored on the Trouve contains these tokens. After all modules load, `discover_project()` builds an `id → full_name` map and resolves tokens via regex. Import detection also uses these tokens — no module global inspection. **Consequence:** SQL placeholder resolution is always a post-load step, not available at Trouve construction time.

### Config cascade
Resolution order (later overrides earlier): profile defaults → `__database_config__.py` → `__schema_config__.py`. Resolved during discovery, stored in `CompiledAttributes`.

### Partial failure in `clair run`
On failure: mark failed node, compute all downstream via `nx.descendants()`, mark them skipped, continue unrelated branches. No `--fail-fast` flag.

### Adapter abstraction
`WarehouseAdapter` ABC in `adapters/base.py`. Only `SnowflakeAdapter` exists. Runner only uses the ABC — future warehouse support doesn't require rewriting runner.

### `clair docs`
No new Python runtime deps. Static SPA bundled into the package. Catalog is in-memory (not written to disk). No Snowflake connection needed. Port 8741.

### Environments / routing
Two routing policies: `database_override` and `schema_isolation`. Implementation in `src/clair/auth/environments.py`. SOURCEs are never routed. Routing collisions are warnings (not errors) — see shared memory for rationale.

### `render_dag()` design
Pure function in `core/dag_render.py`. Returns a string. No IO, no side effects. Upstream ancestors only for `--select`. Plain text (no ANSI). Each node appears once at its deepest level (diamond → max depth wins).
