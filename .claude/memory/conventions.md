# clair conventions

How to write clair, and how to judge whether a change is good. Read this file before you
propose a feature, a refactor, or a test.

Behaviour is not here. `site_docs/docs/` holds behaviour, and `src/` is the final
authority. This file holds only what those two cannot hold: the design position, the
quality bar, and the corrections that the user gave.

**How to read this file.** Part 1 is absolute: a change that breaks an invariant is wrong.
Parts 3 to 5 are the quality bar. They are principles, not a checklist. Apply judgement,
and prefer the simpler code when a principle and the code disagree. The specific rules in
those parts are examples of a principle, not the principle itself.

---

## Part 1 — Design invariants

These seven rules control all clair design work. A change that breaks one of them is
wrong, even if the code operates correctly.

1. No Jinja. SQL is a plain Python f-string.
2. No YAML for configuration. YAML holds credentials only, in `~/.clair/environments.yml`.
   All other configuration is Python.
3. The file path gives `database.schema.table`. Trouve files are three levels below the
   project root.
4. `clair compile` makes no connection to Snowflake. It is a local operation only.
5. Validation is eager. An invalid Trouve raises at construction time, not at run time.
6. Shared SQL logic is a normal Python function, which you import normally.
7. All warehouse access goes through the `WarehouseAdapter` ABC in
   `src/clair/adapters/base.py`. The runner must not import `SnowflakeAdapter` directly.

**Why:** These rules are the product position against dbt and SQLMesh. Python-native, no
template language, no configuration language, full IDE support.

**How to apply:** Before you write a new module or change an interface, compare your
design to this list.

---

## Part 2 — The documentation gives orientation. The code gives the truth.

`site_docs/docs/` is the source of truth for clair behaviour. It is Markdown, it is small
(approximately 9000 words), and `grep` finds content in it quickly. Read it first. Do not
read `src/` to learn what a feature does.

**But the documentation also rots.** The pandas guide, the landing page and the README
once documented a `PandasTrouve` class that nobody built, while the API reference
correctly documented the `df_fn` field that was built. The pages came from a design spec,
and nobody changed them when the implementation took a different shape. mkdocs does not
execute the examples, so CI did not find the error. (The 2026-08-01 backend split built
the real `PandasTrouve` and deleted `df_fn`, thus the two agree again — but the lesson
holds.)

**Therefore the code is the final authority.** Read the documentation for orientation,
then confirm any API detail against `src/` or `example_projects/` before you depend on
it. When the two disagree, the code wins and the page is a bug.

**How to apply:**

- To learn what a feature does, `grep` `site_docs/docs/` first. Then confirm the exact API
  against `src/` or a project in `example_projects/`.
- Search for the field name, not only the class name. The pandas feature was once
  invisible to a search for `PandasTrouve`, because the name in the code was `df_fn`.
- Map for orientation: `concepts/` (Trouve, DAG, project layout, environments),
  `guides/` (routing, incrementality, tests, selectors, pandas, per-database config),
  `cli/` (one page for each subcommand), `reference/` (API for Trouve, Column, RunConfig,
  Tests).
- When you change behaviour, change the matching page in `site_docs/docs/` in the same PR.
- Do not add a note to this file that repeats a documentation page.

---

## Part 3 — The quality bar

The target: a new contributor opens the repository, reads one module, and can predict what
the next module looks like. Each idea has one home. Nothing repeats. Names say what they
mean.

**One definition for one idea.** When two pieces of code do almost the same operation,
make one piece and give it a parameter. Two copies that drift apart are worse than one
function with an argument. This applies to code, to test fixtures, and to documentation
text. Before you add a helper, `grep` for one that exists.

**Dense references, not copies.** Point at the definition. A function calls the shared
function. A document links to the page. A test uses the shared fixture in
`tests/conftest.py` or `tests/helpers.py`. A copy becomes wrong when the original changes,
and nobody knows that it did.

**Simple and expressive.** The straight path first: a named variable, a short function, an
early return. Do not compress logic into an expression that a reader must decode.

```python
# Bad — one line, three decisions, unreadable (this pattern is in trouve.py today).
update_cols = upsert_config.update_columns if upsert_config and upsert_config.update_columns is not None else [c for c in all_col_names if c not in unique_keys]

# Good — the two cases are visible.
if upsert_config and upsert_config.update_columns is not None:
    update_column_names = upsert_config.update_columns
else:
    update_column_names = [
        column_name for column_name in all_column_names if column_name not in unique_keys
    ]
```

**Descriptive names, always.** `trouve`, not `t`. `column_name`, not `c`. This holds in a
comprehension and in a loop, where the short name is most tempting. Use `database_name`,
`schema_name`, and `table_name` — never `database`, `schema`, or `table`.

**Make the illegal state impossible.** Eager validation (invariant 5) is one example of a
larger rule: let a type or a constructor reject bad input, so the code that comes after it
holds no defensive branch.

**Fail loudly at the gap.** When code branches over a closed set — an enum such as
`ExecutionType`, `TrouveType`, or `RunMode` — use `if/elif/else` and raise the correct
`ClairError` subclass (`CompileError`, `RunError`) in the `else`. A ternary such as
`".py" if x == PANDAS else ".sql"` defaults silently when somebody adds a value. Declare
the result variable as `None` before the block, so no variable is first-assigned inside an
indented block. The `None` needs no annotation: the type checker infers the type after the
block, because the `else` always raises.

**Data first, format last.** A function returns a Pydantic object that holds the data. A
separate `format_*` function or a `.render()` method makes the string for the CLI. This
keeps the semantics testable and the format free to change. It applies to
`write_compile_output` (`compiler.py`), `render_dag` (`dag_render.py`), `format_run_output`
(`runner.py`), `format_test_output` (`test_runner.py`), and each future output function.

---

## Part 4 — Tests

A test states an invariant. It answers "what must stay true", not "what did the code print
on the day I wrote it".

**Test the invariant, not the transcript.** Assert on the fields of the returned object:
`result.succeeded_count == 1`, never `"1 succeeded" in output`. An equality check against
a display string tests the format, breaks on a cosmetic edit, and hides the property that
matters. A formatter needs no string-content test of its own.

**Cover the common case and the edges.** For each behaviour, think through the boundaries
before you write: empty input, one item, many items, a duplicate, a cycle, a missing
optional field, the wrong type, the error path. A suite that tests only the happy path
gives false confidence.

**Parametrize the shape.** When several tests differ only in their input and their
expected output, they are one test with `@pytest.mark.parametrize`. Ten near-identical
functions hide the one case that nobody covered; one parametrized table makes the gap
visible. The suite today holds one `parametrize` in 21 files, and files such as
`tests/unit/test_dag_render.py` (1019 lines) repeat the same build-render-assert triple.
That is the pattern to correct, not the pattern to copy.

**Test the public seam.** Drive the behaviour through the API that a user reaches. A test
that imports a private helper (`from clair.cli.main import _parse_before_spec`) locks the
refactor out. Reach for a private function only when the behaviour has no public path, and
treat that as a signal that the seam is in the wrong place.

**Share the setup.** Project fixtures live in `tests/conftest.py`, and the shared routing
entries live in `tests/helpers.py`. Extend them. Do not paste a fourth copy of a Trouve
builder into a fifth test file.

---

## Part 5 — The existing code is not the standard

Parts 3 and 4 describe where clair goes, not where each file is today. The survey of
2026-08-23 found short names in `dag.py`, `discovery.py`, and `trouve.py`, one
`parametrize` in the whole suite, approximately 100 string-membership assertions, and
tests that import private functions.

Therefore: do not copy the style of the file that you edit when that style breaks this
document. Write the new code to this bar. Correct the code that you touch, and leave the
rest — a mass rewrite is a separate PR with its own review.

Version 0 helps you here. clair keeps no backwards compatibility, adds no deprecation
shim, and keeps no alias for an old name. Delete the old path. The best design wins.
