# clair conventions

The design rules and the code conventions for clair. Read this file before you propose a
feature, a refactor, or a test.

Behaviour is not here. `site_docs/docs/` holds behaviour, and `src/` is the final
authority. This file holds only what those two cannot hold: a design rule, and a
correction that the user gave.

---

## 1. Design invariants

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

## 2. The documentation gives orientation. The code gives the truth.

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

## 3. Branch on an enum with if/elif/else, and raise in the else

Prefer `if/elif/else` over a ternary expression when you branch on an enum. The `else`
must raise `CompileError`, or the correct `ClairError` subclass, with a descriptive
message.

**Why:** A ternary such as `".py" if x == PANDAS else ".sql"` defaults silently when
someone adds a new enum value. An explicit raise shows the gap immediately.

**How to apply:**

- Use this pattern each time code branches on `ExecutionType`, `TrouveType`, `RunMode`, or
  a similar enum.
- Use the most specific `ClairError` subclass for the context (`CompileError`, `RunError`).
- Declare the result variable as `None` before the block. Do not first-assign a variable
  inside an indented block. The `None` init needs no type annotation, because the type
  checker infers `str` after the block: the `else` always raises.

---

## 4. Functions return objects. Tests assert on fields, not on strings.

Do not test an output string for its properties. A function returns a Pydantic object with
the data, and a formatter makes the string.

**Why:** String assertions are brittle. They break when the format changes, and they hide
the data that the test examines. A structured object lets you assert on the semantics —
counts, names, flags — independently of the format.

**How to apply:**

- For a function that makes display output (a compile summary, a run report, a test
  report, a DAG render), put the format code in a separate `format_*` function or a
  `.render()` method on the model.
- Assert `result.succeeded_count == 1`, not `"1 succeeded" in output`.
- The formatter itself needs no string-content test.
- This applies to `write_compile_output` (`compiler.py`), `render_dag` (`dag_render.py`),
  `format_run_output` (`runner.py`), `format_test_output` (`test_runner.py`), and each
  future output function.
