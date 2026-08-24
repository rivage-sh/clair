# clair conventions

How to write clair, and how to judge a change. Read this before you propose a feature, a
refactor, or a test.

Behaviour is not here — `site_docs/docs/` holds behaviour, and `src/` is the final
authority. This file holds the design position, the quality bar, and the corrections that
the user gave.

The design invariants are absolute. The rest is a set of principles, not a checklist.
Apply judgement, and prefer the simpler code when a principle and the code disagree.

---

## Design invariants

A change that breaks one of these is wrong, even if the code operates correctly. They are
the product position against dbt and SQLMesh: Python-native, no template language, no
configuration language, full IDE support.

1. No Jinja. SQL is a plain Python f-string.
2. No YAML for configuration. YAML holds credentials only, in `~/.clair/environments.yml`.
3. The file path gives `database.schema.table`. Trouve files sit three levels below the
   project root.
4. `clair compile` makes no connection to Snowflake. It is local only.
5. Validation is eager. An invalid Trouve raises at construction time, not at run time.
6. Shared SQL logic is a normal Python function, which you import normally.
7. All warehouse access goes through the `WarehouseAdapter` ABC in `adapters/base.py`.
   The runner must not import `SnowflakeAdapter` directly.

---

## The documentation gives orientation. The code gives the truth.

`grep` `site_docs/docs/` to learn what a feature does — it is Markdown, and it is small
(approximately 9000 words). Then confirm the exact API against `src/` or
`example_projects/` before you depend on it. When the two disagree, the code wins and the
page is a bug.

The documentation rots. The pandas guide, the landing page and the README once documented
a `PandasTrouve` class that nobody built, while the API reference correctly documented the
`df_fn` field that was built. mkdocs does not execute the examples, so CI found nothing.

- Search for the field name, not only the class name. The pandas feature was invisible to
  a search for `PandasTrouve`, because the code called it `df_fn`.
- Map: `concepts/` (Trouve, DAG, layout, environments), `guides/` (routing,
  incrementality, tests, selectors, pandas, per-database config), `cli/` (one page per
  subcommand), `reference/` (Trouve, Column, RunConfig, Tests).
- When you change behaviour, change the matching page in the same PR. Do not add a note
  here that repeats a page.

---

## The quality bar

The target: a new contributor reads one module and can predict the next one. Each idea has
one home, nothing repeats, and names say what they mean.

**One definition for one idea.** When two pieces of code do almost the same operation,
make one piece and give it a parameter. Two copies that drift apart are worse than one
function with an argument. `grep` for a helper before you add one.

**Reference, do not copy.** A function calls the shared function, a document links to the
page, a test uses `tests/conftest.py` or `tests/helpers.py`. A copy becomes wrong when the
original changes, and nobody knows that it did.

**Simple and expressive.** A named variable, a short function, an early return. Do not
compress logic into an expression that a reader must decode.

```python
# Bad — one line, three decisions. This is trouve.py:229 today.
update_cols = upsert_config.update_columns if upsert_config and upsert_config.update_columns is not None else [c for c in all_col_names if c not in unique_keys]

# Good — the two cases are visible, and the names say what they hold.
if upsert_config and upsert_config.update_columns is not None:
    update_column_names = upsert_config.update_columns
else:
    update_column_names = [
        column_name for column_name in all_column_names if column_name not in unique_keys
    ]
```

**Descriptive names, always** — `trouve` not `t`, `column_name` not `c`, also inside a
comprehension, where the short name is most tempting. Use `database_name`, `schema_name`,
`table_name`.

**Make the illegal state impossible.** Eager validation (invariant 5) is one case of a
larger rule: let a type or a constructor reject bad input, so the code after it holds no
defensive branch.

**Fail loudly at the gap.** Branch over a closed set with `if/elif/else`, and raise the
correct `ClairError` subclass in the `else`.

```python
# Bad — a new ExecutionType value defaults silently to SQL.
suffix = ".py" if execution_type == ExecutionType.PANDAS else ".sql"

# Good — declare the result first, and raise on the gap.
suffix = None
if execution_type == ExecutionType.PANDAS:
    suffix = ".py"
elif execution_type == ExecutionType.SNOWFLAKE:
    suffix = ".sql"
else:
    raise CompileError(f"No file suffix for the execution type {execution_type}.")
```

The `None` needs no annotation: the type checker infers `str` after the block, because the
`else` always raises.

**Data first, format last.** A function returns a Pydantic object that holds the data; a
separate `format_*` function or `.render()` method makes the string for the CLI. This
keeps the semantics testable and the format free to change. It applies to
`write_compile_output`, `render_dag`, `format_run_output`, `format_test_output`, and each
future output function.

---

## Tests

A test states an invariant. It answers "what must stay true", not "what did the code print
the day I wrote it".

**Test the invariant, not the transcript.** An equality check against a display string
tests the format, breaks on a cosmetic edit, and hides the property that matters. A
formatter needs no string-content test of its own.

```python
# Bad
assert "1 succeeded" in output
# Good
assert summary.succeeded_count == 1
```

**Cover the common case and the edges.** Think through the boundaries before you write:
empty, one, many, a duplicate, a cycle, a missing optional field, the wrong type, the
error path. A suite that tests only the happy path gives false confidence.

**Parametrize the shape.** Tests that differ only in input and expected output are one
test. Ten near-identical functions hide the case that nobody covered; one table makes the
gap visible.

```python
@pytest.mark.parametrize(
    ("execution_type", "expected_suffix"),
    [(ExecutionType.SNOWFLAKE, ".sql"), (ExecutionType.PANDAS, ".py")],
)
def test_each_execution_type_gives_a_suffix(execution_type, expected_suffix):
    assert compiled_file_suffix(execution_type) == expected_suffix
```

**Test the public seam.** Drive the behaviour through the API that a user reaches. A test
that imports a private helper (`from clair.cli.main import _parse_before_spec`) locks the
refactor out, and signals that the seam sits in the wrong place.

**Share the setup.** Extend `tests/conftest.py` and `tests/helpers.py`. Do not paste a
fourth Trouve builder into a fifth test file.

---

## The existing code is not the standard

This file describes where clair goes, not where each file is today. The survey of
2026-08-23 found one `parametrize` in 21 test files against 909 assertions, approximately
100 string-membership assertions, `test_dag_render.py` at 1019 lines of one repeated
build-render-assert triple, tests that import private functions, and short names in
`dag.py`, `discovery.py`, and `trouve.py`.

So do not copy the style of the file that you edit when that style breaks this document.
Write new code to this bar, correct the code that you touch, and leave the rest — a mass
rewrite is a separate PR. Version 0 gives you room: see the backwards compatibility rule
in `CLAUDE.md`.
