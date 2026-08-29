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
`examples/projects/` before you depend on it. When the two disagree, the code wins and the
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

### `examples/` rots in the same way, and CI does not execute it

`examples/projects/` and `examples/notebooks/` are documentation that happens to be code.
Nothing runs the notebooks, and only the integration tests run some of the projects. Thus
they hold a stale API until a person reads them. `examples/notebooks/` once held one
notebook that read the DAG only, and a `requirements.txt` that named a package that does
not exist, months after the Python API arrived.

Apply the documentation rule to this directory:

- When you add a public name, a field, or an operation, look for the example that must
  show it, and change it in the same PR. A feature with no example is a feature that
  nobody finds.
- When you change a public name or a signature, `grep examples/` for the old name before
  you finish. The compiler does not read a notebook, and `ty` skips
  `examples/notebooks/`.
- The example code in `site_docs/docs/` must stay identical to the equivalent code in
  `examples/projects/`. Change one, change the other.
- Execute a notebook that you change, and commit the outputs:
  `uv run jupyter nbconvert --to notebook --execute --inplace <notebook>`. An output that
  a person wrote by hand is a lie that CI cannot find.
- A notebook must run on a machine with no Snowflake account: no connection, no
  `~/.clair/environments.yml`, and no write to the home directory of the reader. Give
  `clair.run()` an adapter that holds its tables in memory, and an `Environment` that the
  notebook makes. `clair.compile()` and `clair.validate()` take the environment name.
- Read `examples/` one time each release, in the same way that you read
  `site_docs/docs/`. Drift here is silent.

---

## The quality bar

The target: a new contributor reads one module and can predict the next one. Each idea has
one home, nothing repeats, and names say what they mean.

**One definition for one idea.** When two pieces of code do almost the same operation,
make one piece and give it a parameter. Two copies that drift apart are worse than one
function with an argument. `grep` for a helper before you add one.

**A closed set of values is a `StrEnum`.** When a field, an argument, or a return value
holds one of a known set of strings, declare a `StrEnum` and use its members. Do not write
the raw string at the definition site or at the call site. The enum gives one home for the
set, the type checker finds a name that is not a member, and a reader sees each permitted
value. See `TrouveType`, `RunMode`, and `TextReferenceLocation`.

```python
# Bad — the set of values lives in nobody's head but the author's.
location: str
sources.append(("test sql", test.sql))

# Good — the set has one home.
location: TextReferenceLocation
sources.append((TextReferenceLocation.TEST_SQL, test.sql))
```

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

**Comment only what the code cannot say.** A comment earns its place when it gives the
reason for a decision, a constraint from outside the file, or a warning. A comment that
repeats the signature, the type, or the next line is noise: it makes the reader read the
same idea twice, and it goes stale.

```python
# Bad — the two lines below say this already.
# Clair calls this method for every Trouve, and a SOURCE Trouve is not an
# exception. Examine trouve_type to give a SOURCE a different rule.
if trouve_type == TrouveType.SOURCE:
    return trouve_address

# Good — no comment. The condition is the documentation.
if trouve_type == TrouveType.SOURCE:
    return trouve_address
```

The same bar applies to a docstring, a README, and a page in `site_docs/docs/`. Say the
rule once, in the place that owns it, and link to that place from everywhere else.

**Do not write history in a comment.** A comment describes the code as it is now. Words
such as "before this change", "no longer", "used to", "we now", or a pull request number
mean nothing to a reader who never saw the old code. `git log` and `git blame` hold the
history, and they stay correct.

```python
# Bad
# We no longer skip a SOURCE here, because routing applies to every Trouve now.

# Good
# A TABLE that routes onto a SOURCE replaces the data it reads.
```

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

**Build a generated file with an f-string, not a placeholder.** When a function writes a
file of code — a fixture project, a scaffold, a CI configuration — give the function a
parameter for each value that changes, and return an f-string. Do not write a constant
with a token such as `__DATABASE_NAME__` and then call `.replace()` on it. The f-string
shows each value at the place where it goes, the type checker sees the parameter, and
nobody can forget one `.replace()` call.

```python
# Bad — the token and the value sit in two places, and nothing joins them.
_CHECKED_FILE = """
from __DATABASE_NAME__.source.rows import trouve as source_rows
tests=[TestRowCount(min_rows=__MINIMUM_ROWS__)]
"""
text = _CHECKED_FILE.replace("__DATABASE_NAME__", database_name).replace(
    "__MINIMUM_ROWS__", str(minimum_rows)
)

# Good — one function, one signature, each value at its place.
def checked_file(database_name: str, minimum_rows: int) -> str:
    return f"""
from {database_name}.source.rows import trouve as source_rows
tests=[TestRowCount(min_rows={minimum_rows})]
"""
```

A generated file that holds an f-string of its own needs two braces, for example
`{{source_rows}}`. Say so in the docstring of the module.

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

This file describes where clair goes, not where each file is today.

The survey of 2026-08-23 found one `parametrize` in 21 test files against 909 assertions,
approximately 100 string-membership assertions, `test_dag_render.py` at 1019 lines of one
repeated build-render-assert triple, tests that import private functions, and short names
in `dag.py`, `discovery.py`, and `trouve.py`.

The test refactor of 2026-08-28 corrected a part of that. The suite now holds no
`unittest.mock`: `tests/helpers.py` gives `RecordingAdapter`, a complete
`WarehouseAdapter` that holds its tables in memory. A test that needs the warehouse goes
in `tests/integration/`, and it calls `clair.run()` against Snowflake. `render_dag`,
`clair.validate()`, `clair.clean()` and `Trouve.upsert_plan()` give the data, and a
separate function makes the text. Short names stay in `dag.py` and `discovery.py`, and
`test_trouves.py` and `test_discovery.py` still hold string-membership assertions.

So do not copy the style of the file that you edit when that style breaks this document.
Write new code to this bar, correct the code that you touch, and leave the rest — a mass
rewrite is a separate PR. Version 0 gives you room: see the backwards compatibility rule
in `CLAUDE.md`.
