---
name: no-string-output-tests
description: Functions should return structured Pydantic objects, not strings. Tests must assert on object fields, not formatted output strings.
type: feedback
---

Do NOT test output strings for properties. Functions should return Pydantic objects with data that is then formattable to strings.

**Why:** String assertions are brittle — they break on cosmetic formatting changes and obscure the actual data being tested. Structured objects let you assert on the semantics (counts, names, flags) independently of formatting.

**How to apply:**
- When writing or reviewing functions that produce "display output" (compile summaries, run reports, test reports, DAG renders), push the formatting into a separate `format_*` method or `.render()` call on the model.
- Tests should assert `result.succeeded_count == 1`, not `"1 succeeded" in output`.
- The formatted string (for CLI display) is derived from the object via a method or standalone formatter function — that formatter does NOT need its own string-content tests.
- This applies to: `write_compile_output` (compiler.py), `render_dag` (dag_render.py), `format_run_output` (runner.py), `format_test_output` (test_runner.py), and any future output-producing functions.
