# The Library and the CLI

clair is a Python library. `clair run` is a thin program that reads your arguments and
calls a function of that library. The library is the product, and the CLI is one of its
callers.

Most data transformation tools take the opposite position. The command line is the
product, and the program writes its result to stdout. A different program that wants the
result must start a subprocess, and then parse text that the tool designed for a person.
The text has no schema, so a cosmetic change to the output breaks the caller, and CI finds
nothing.

clair holds the boundary at a different place.

## What the CLI does, and what it does not do

`clair.cli.main` does three operations, and no more:

1. It reads the arguments and the flags.
2. It calls the matching function in `clair/api.py`.
3. It formats the result object for a terminal, and it selects the exit code.

Step 3 is the only step that knows about a terminal. Every decision about your project —
which files hold a Trouve, what the DAG looks like, which SQL Snowflake receives, which
test fails — happens below step 2, in code that no terminal reaches.

## What this gives you

**You call clair from Python with no subprocess.** `clair.run()` does the same work as
`clair run`. A notebook, an Airflow task, a pytest function, or your own program imports
the package and calls the function.

**You get an object, not a transcript.** Each operation gives a Pydantic model that holds
the complete data of the operation. You read `summary.failed_count` — you do not search
the terminal output for the word `failed`. The format of the CLI is then free to change,
because no caller depends on it.

**A fault raises an exception.** No API function writes to stdout, and no API function
stops the process. A fault raises a subclass of `ClairError`, which your own code catches
and handles.

**Your editor understands your project.** A Trouve is a Python object in a Python file,
and a dependency is a Python import. So "go to definition" moves to the Trouve upstream,
"find references" gives each Trouve downstream, and the type checker reads your pipeline.
No template language stands between your editor and your code.

## The rule for a new operation

A new operation starts as a function in `clair/api.py` that gives a result object. The CLI
comes after, and it adds only argument parsing and one format function. An operation that
a person can do but a program cannot is a bug in clair.

---

For the signature of each function, read the
[Python API reference](../reference/python-api.md). For the flags of each command, read the
[CLI](../cli/overview.md).
