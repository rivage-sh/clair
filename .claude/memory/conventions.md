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
3. The file path gives `database.schema.table`, and the directory tree gives the
   configuration. Trouve files sit three levels below the project root.
   `__database_config__.py` and `__schema_config__.py` apply to each Trouve below them.
4. `clair compile` makes no connection to Snowflake. It is local only.
5. Validation is eager. An invalid Trouve raises at construction time, not at run time.
6. Shared SQL logic is a normal Python function, which you import normally.
7. All warehouse access goes through the `WarehouseAdapter` ABC in `adapters/base.py`.
   The runner must not import `SnowflakeAdapter` directly.

### Invariant 3 is a correctness rule, and not a convention

The other invariants remove a language: no Jinja, no YAML. Invariant 3 looks smaller, thus
a later change can trade it away by accident. Do not. It is the reason that clair has no
`dbt_project.yml`, and it does three things that no Python API gives you for free:

- **A duplicate address is impossible to write.** Two Trouves cannot hold one address,
  because two files cannot hold one path. `compute_logical_address()` in
  `core/discovery.py` reads the last three parts of the path, thus the file system is the
  name table, and clair maintains none.
- **A reviewer reads the DAG from `tree`.** The shape of the project is the shape of the
  directory, and a person sees it before they open one file.
- **The configuration inherits with no configuration language.** `_resolve_config()` moves
  up the tree and merges the profile defaults, then `__database_config__.py`, then
  `__schema_config__.py`. A directory is the scope. Nobody writes a scope.

A Python API that constructs a Trouve with no file gives up all three. It must then take
the address as a field, where a typo makes a collision, and it must re-invent the
directory as nested defaults objects. That is worse than the thing it replaces. Clair
rejected this design on 2026-08-28. The Python API takes a project directory, and the file
system stays the authoring mode.

This position is for a contributor, and not for a user.
`site_docs/docs/topics/project-layout.md` gives the rule and the layout, and it makes no
argument for them. A user who reads that page wants to know where the file goes.

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
- When you change behaviour, change the matching page in the same PR. Do not add a note
  here that repeats a page.

### One page has one job

`site_docs/docs/` follows the Diataxis division. Four kinds of page serve four different
readers, and a page that mixes two serves neither. Put each new page in the correct
directory, and keep the other kinds out of it.

| Directory | Kind | Serves | The test |
|-----------|------|--------|----------|
| `index.md`, `installation.md`, `quickstart.md` | Tutorial | A person who never used clair | One path, and it always operates. |
| `topics/` | Explanation | A person who wants to understand clair | The reader learns *why*, and does no work. |
| `cli/`, `reference/` | Reference | A person in the middle of a task | Complete, dry, and shaped by the code. |
| (none today) | How-to | A person with a goal | One goal, one recipe, from start to end. |

- **Reference takes its shape from the code.** One page per class or per command, each
  field, each flag, each default. A person reads a fragment of it, not the page. When a
  signature changes, the page changes. A person could generate it from `src/`.
- **A topic takes its shape from an idea.** It gives the reason, the position, and the
  comparison against another tool. Nobody can generate it. A rule of thumb: if the reader
  cannot do the job without the page, the page is reference; if the reader can do the job
  but does not understand it, the page is a topic.
- **Move the prose, do not copy it.** A design position inside a reference table is in the
  wrong place. Put it in `topics/`, and leave a link. `topics/library-and-cli.md` holds
  the position that clair is a Python library and the CLI is a thin caller of it, because
  that argument sat as two lines inside `reference/python-api.md`.
- **The how-to column is empty today.** A task-shaped page — clair in CI, clair in a
  pytest suite, clair in Airflow — goes in a new `guides/` directory, not in `topics/`.

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
keeps the semantics testable and the format free to change. It applies to `render_dag`,
`format_run_output`, `format_test_output`, and each future output function.

**An operation gives data. The caller decides where the bytes go.** Persistence is the
same concern as format, at a longer distance. An operation in `api.py` must not find a
path, open a file, or write a directory in the middle of its work. It gives a result
object that holds each fact, and a separate writer puts that result on disk. The CLI calls
the operation, then the writer — in the same way that the CLI reads
`~/.clair/environments.yml` into an `Environment` and gives the object to the API.

`write_compile_output` breaks this rule today, and the quality bar is not the current
code. `api.run()` calls it in the middle of the run, and it makes the
`project_root / _clairtifacts` path by itself. The data is already pure: `RunSummary`
holds each statement, each query ID, each address, and each status. Only the write is in
the wrong place. A writer that must fill a directory as the run continues takes an event
for each node; it does not become a call inside the runner.

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
