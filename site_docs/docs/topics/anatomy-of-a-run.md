# The anatomy of a run

`clair run` does seven steps. This page names each step, and it tells you why the step
exists. Read it when you must know which table a Trouve reads in a partial run.

## The seven steps

Each step below links to its source code.

| # | Step | Result |
|---|---|---|
| 1 | [Load the environment](#1-load-the-environment) | The connection settings, and the environment name |
| 2 | [Find the routing entry](#2-find-the-routing-entry) | One `RoutingEntry`, or `None` |
| 3 | [Discover the project](#3-discover-the-project) | One compiled Trouve for each file |
| 4 | [Build the DAG](#4-build-the-dag) | The dependency graph |
| 5 | [Apply the selection](#5-apply-the-selection) | The Trouves that this run builds |
| 6 | [Resolve the addresses](#6-resolve-the-addresses) | The table that each Trouve reads |
| 7 | [Execute](#7-execute) | One Snowflake object for each Trouve |

## 0. Find the project root

*Source: [`core/discovery.py`][discovery-py] — `find_project_root()`*

Each step below takes a project root. `__routing__.py` marks it: clair walks up from the
working directory to the first one, in the same way that git finds `.git`. Thus you run a
command from any directory of the project. A directory that holds no `__routing__.py`, and
no parent with one, stops the command. See
[Project Layout](project-layout.md#the-project-root).

The search reads the file system, thus it belongs to the one file system reader of
`core/`. It runs before the pipeline: it produces the `project_dir` that step 2 and step 3
then take.

## 1. Load the environment

*Source: [`environments/environments.py`][environments-py] — `load_environment()`*

`clair run --env dev` reads the `dev` profile from `~/.clair/environments.yml`. The profile
gives the account, the warehouse, the role, and the credentials. It also gives the
environment name, which step 2 needs. See [Environments](environments.md).

The CLI does this step. `clair.run()` takes the parsed `Environment`, and it reads no
file. See [Python API](../reference/python-api.md).

## 2. Find the routing entry

*Source: [`environments/project_routing.py`][project-routing-py] — `load_project_routing()`, and [`environments/routing.py`][routing-py] — `route()`*

clair loads `__routing__.py` and finds the entry with the environment name of step 1. An
environment with no entry gets `None`, which means passthrough: each physical address is
the logical address. See [Routing](routing.md).

## 3. Discover the project

*Source: [`core/discovery.py`][discovery-py] — `discover_project()`, [`trouves/_refs.py`][refs-py], and [`core/text_references.py`][text-references-py]*

clair walks the project directory and imports each Python file that holds a `trouve`
object. Each file gets three addresses.

| Address | Source | Purpose |
|---|---|---|
| logical | The file path. `mydb/derived/daily.py` gives `mydb.derived.daily`. | The DAG edges and the `--select` patterns use it. It is the production address. |
| physical | The routing entry, which takes the logical address. | clair writes here. |
| reference | Step 6 decides it. | A different Trouve reads here. |

Then clair compiles each Trouve. An f-string reference such as `f"SELECT * FROM {orders}"`
becomes a placeholder token at import time. clair renders the SQL from those tokens and
writes the result to `resolved_sql`. At this point it writes the **logical** address of
each upstream Trouve, because it does not know the selection yet.

`Trouve.sql` and `TestSql.sql` keep the tokens. clair never edits the rendered SQL, thus
step 6 renders it a second time from the same source. Only a token becomes an address: an
address that you type as text, in a comment or in a string literal, stays as you wrote it.

Thus an address that you type as a table name makes no DAG edge, and routing does not move
it. [`clair validate`](../cli/validate.md) reports that fault.

A pandas Trouve holds the same information in a list. Its `input_addresses` gives the
address of each input, in the parameter order of the transform.

### One file gives one Trouve object

clair imports each Trouve file, and the Trouve files import each other. Python keys its
module table by the module name, thus one file under two names would run two times and
give two `Trouve` objects. clair would then know one object, while the SQL of the author
points to the other, and the DAG would lose that edge in silence.

*Source: [`core/module_identity.py`][module-identity-py]*

clair removes the fault at its origin. A file below the project root gives one module
object, whatever name an import uses: the import resolves in the normal way, and the
existing module object then answers under the new name too. The standard library does the
same by hand — `os.path` and `posixpath` are two names for one module.

A reference token that survives compilation stops the run anyway, and the error names the
Trouve. clair never sends unresolved SQL to the warehouse: Snowflake answers with a parse
error that names the token and nothing else.

### A file that clair cannot read stops the run

A Trouve file that raises at import time — a name that does not exist, a package that
nobody installed, two files that import each other — stops discovery. clair raises
`ProjectDiscoveryError`, and the error names each file and each cause.

clair does not skip the file and continue. A skip removes the Trouve from the DAG, and the
run then builds fewer tables than the project declares and reports success. The error
holds each fault together, thus you repair the project one time and not one file at a
time.

## 4. Build the DAG

*Source: [`core/dag.py`][dag-py] — `build_dag()`*

Each DAG node holds the physical address of a Trouve, because that address is unique in
the warehouse. The compiled `imports` hold logical addresses, thus clair maps one to the
other to make each edge. See [DAG](dag.md).

## 5. Apply the selection

*Source: [`core/selector.py`][selector-py] — `expand_selectors()`*

`--select` expands its globs and its `+` operators against the **logical** addresses.
clair then removes each SOURCE, because clair never builds a SOURCE, and it subtracts
`--exclude`. The result is the set of Trouves that this run builds.

## 6. Resolve the addresses

*Source: [`core/discovery.py`][discovery-py] — `recompile_for_selection()`*

This step decides which table each Trouve reads. clair renders the SQL a second time, from
the tokens of step 3, and the selection now decides each address.

| The upstream Trouve | It reads at | Why |
|---|---|---|
| This run builds it | The physical address | This run puts new data there. The Trouve must read that new data, and not the production data. |
| This run does not build it | The logical address | Nothing writes a new copy in this run, thus the production table holds the newest data. |
| A SOURCE | The physical address | clair never builds a SOURCE, thus the routing entry is the only statement about where the data is. |

An example. Your project holds 50 Trouves, and you build 10 of them in the `dev`
environment. `DeveloperRouting` adds your name to each database name:

```
clair run --env dev --select 'mydb.derived.*'
```

| The Trouve `mydb.derived.summary` | Address |
|---|---|
| writes to | `mydb_ALICE.derived.summary` |
| reads `mydb.derived.daily`, which the selection holds | `mydb_ALICE.derived.daily` |
| reads `mydb.refined.orders`, which the selection omits | `mydb.refined.orders` |
| reads `mydb.source.events`, a SOURCE | `mydb.source.events` |

Thus your 10 Trouves make one connected chain in your own database, and that chain reads
production for everything else. You do not build 40 tables to test 10.

!!! note "One assumption"
    clair reads an unselected Trouve at its logical address. This assumes that your
    production runs write there — which a passthrough routing entry does. If your `prod`
    entry moves the address, a partial `dev` run reads a table that nothing writes. Give
    `prod` an entry that gives the address back, as [Routing](routing.md) shows.

This step is the same for both backends. A SQL Trouve holds its addresses in its SQL, and
a pandas Trouve holds them in `input_addresses`. The rule above decides both. A `TestSql`
gets its addresses from the same step, thus a test reads the tables that its Trouve reads.

## 7. Execute

*Source: [`core/runner.py`][runner-py] — `run_project()`, and [`core/staging.py`][staging-py]*

clair starts a Trouve when each Trouve of the run that is upstream of it completed. The
thread count decides how many Trouves run at one time. Each thread holds a private
Snowflake connection, because a connection holds the role and the warehouse of the
session. See [Environments](environments.md).

Each Trouve writes to a staging address, the tests run against the staging data, and clair
promotes the data to the physical address after the tests pass. A failed test stops the
promotion, thus the physical address keeps its previous data. See [Staging](staging.md).

The five parts below tell you how the engine does this.

### The schedule

`get_executable_nodes()` sorts the DAG topologically and removes each SOURCE. clair keeps
the Trouves of step 5 from that list, and then it counts the upstream Trouves of each one.
A Trouve with a count of zero is ready.

*Source: [`core/runner.py`][runner-py] — `run_project()`, and [`core/dag.py`][dag-py] — `get_execution_order()`*

clair submits each ready Trouve to a `ThreadPoolExecutor`, up to the thread count. Then it
waits for the first Trouve that completes. That Trouve decrements the count of each Trouve
downstream of it, a count that reaches zero makes a new ready Trouve, and clair fills the
free thread immediately. The DAG thus stays as full as its own shape and the thread count
permit. No step waits for a complete "level" of the graph.

The selector does not break this. With `A -> B -> C`, and `B` outside the selection, clair
still makes `C` wait for `A`. A failure of `A` still skips `C`.

`--threads` gives the count for one run. The `threads` key of the environment gives the
default, and `clair init` asks you for it. The default is 4.

```bash
clair run --env prod --threads 8
```

### One connection for each thread

A Snowflake session holds the role and the warehouse, and a `USE ROLE` changes them for
each later query on that session. Two threads on one connection would thus change the
context of each other. `AdapterPool` prevents this: each thread takes one connection and
keeps it until the run ends.

*Source: [`adapters/pool.py`][pool-py] — `AdapterPool`*

The pool opens each connection when the run starts, and not at the first query of a
thread. SSO authentication opens a browser window, thus a lazy connection would ask you to
authenticate in the middle of a run.

clair makes each database and each schema of the run one time, before the threads start.
Many Trouves write to one schema, and concurrent `CREATE SCHEMA IF NOT EXISTS` statements
against one schema can collide in the warehouse.

Two consequences for the reader of the output:

- The results come in **completion order**, and not in topological order. A quick Trouve
  can report before a slow Trouve that started first. Sort against
  `get_execution_order(dag)` if you need the DAG order.
- A high thread count needs a warehouse that can hold the queries. Snowflake queues the
  queries that do not fit, thus a count above the capacity of the warehouse gives you no
  more speed.

### The steps for one node

Each node does the same steps. The run mode decides only if the clone is necessary.

| # | The step | Full refresh | Incremental |
|---|---|---|---|
| 1 | Clone the physical table to the staging address | No. The build makes the object. | Yes. `CREATE OR REPLACE TABLE <staging> CLONE <physical>` |
| 2 | Build at the staging address | `CREATE OR REPLACE TABLE <staging> AS (...)`, or the same statement for a VIEW | `INSERT INTO <staging>`, or the three statements of the `MERGE` |
| 3 | Test the staging object | Yes | Yes |
| 4 | Promote after each test passes | `CREATE OR REPLACE TABLE <physical> CLONE <staging> COPY GRANTS` | The same statement |
| 5 | Drop the staging object | Yes, after the promotion only | Yes, after the promotion only |

*Source: [`core/staging.py`][staging-py], and [`trouves/trouve.py`][trouve-py] — `build_sql()`*

clair promotes a node before it releases the Trouves downstream of it. A dependent thus
starts only after its upstream reaches its physical address, and it reads the address that
its own SQL names. You never write a staging name in a Trouve file.

Step 1 and step 4 are zero-copy clones. Snowflake makes a clone in the metadata, thus the
time is constant for a table of any size. An incremental Trouve therefore gets a full copy
of its target to change, at no copy cost, and a rejected candidate costs you nothing but
the divergence.

The promotion uses `COPY GRANTS`, and not `SWAP`. Snowflake attaches a privilege to the
object and not to the name, thus a `SWAP` moves the production grants to the staging name.
[Staging](staging.md#grants) gives the complete reason.

### The mode of each node

`resolve_effective_mode()` decides the mode of one Trouve. It gives `full_refresh` unless
each of three conditions is true: the Trouve is a TABLE, its `RunConfig` asks for the
incremental mode, and the command line asks for the incremental mode.

*Source: [`core/runner.py`][runner-py] — `resolve_effective_mode()`*

The runner adds a fourth condition, because it has a connection: it asks Snowflake if the
physical table exists. A table that does not exist yet cannot get an `INSERT` or a `MERGE`,
thus clair changes that node to the full refresh mode and writes
`run.node.incremental_fallback` to the log. The first run of a new incremental Trouve needs
no flag from you.

An `upsert` node makes three statements: it builds a merge source table, it merges that
table into the staging table, and it drops the merge source table. If the `MERGE` fails,
clair drops the merge source table anyway. A failure leaves you the staging candidate to
examine, and no clutter that you did not ask for. See [Incrementality](incrementality.md).

### A failure stops one branch, and not the run

A node fails if a statement fails, if a test fails, or if the promotion fails. clair marks
each node **downstream** of it as SKIPPED, and then it continues with the other branches.
The Trouves that already run on the other threads complete. clair starts no new Trouve that
waits for the node that failed.

*Source: [`core/runner.py`][runner-py] — `run_project()`, and [`core/test_runner.py`][test-runner-py] — `run_tests()`*

| The condition | The physical object | The result of the node | The other branches |
|---|---|---|---|
| A statement fails | Keeps its data | FAILED. The message names the staging object. | They run |
| A test fails | Keeps its data | FAILED. The staging object is the rejected candidate. | They run |
| The promotion fails | Keeps its data | FAILED. The tested data stays at the staging address. | They run |
| An upstream node failed | Keeps its data | SKIPPED, with the name of the node that failed | They run |

`clair run` stops with the status code 1 if one node failed. Each `SKIPPED` result names
the node that caused it, thus one look at the summary gives you the first fault.

`--no-test` removes the staging step, because the tests decide the promotion. A run with
`--no-test` writes to each physical address directly.

### The run id

clair makes one `run_id` for each run: the hex form of a UUIDv7. A UUIDv7 starts with a
millisecond timestamp, thus the identifiers sort by time. The `run_id` names the staging
objects (`<table>__clair_<run_id>`) and the artifact directory
(`_clairtifacts/<run_id>/`). Two runs at the same time therefore never write to the same
staging object, and you can find the artifacts of a run from the name of a table that it
left.

`clair run` logs both addresses for each node:

```
[info    ] run.node.start
  logical=mydb.derived.summary
  physical=mydb_ALICE.derived.summary
  effective_mode=full_refresh
```

## The related commands

*Source: [`cli/main.py`][cli-main-py], and [`core/compiler.py`][compiler-py] — `write_compile_output()`*

`clair compile` does steps 1 to 6, and it writes the SQL to `target/`. It does not connect
to Snowflake. Run it to read the SQL that a run executes.

`clair validate` does steps 2 and 3, and then it applies the routing entry to every Trouve.
It reads the environment name, but it opens no profile and it builds no DAG, thus it needs
no Snowflake connection and no credentials. It reports every problem at once: a bad
address, a collision, and an address that you type as text.

[environments-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/environments/environments.py
[project-routing-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/environments/project_routing.py
[routing-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/environments/routing.py
[discovery-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/discovery.py
[module-identity-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/module_identity.py
[refs-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/trouves/_refs.py
[text-references-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/text_references.py
[dag-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/dag.py
[selector-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/selector.py
[runner-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/runner.py
[staging-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/staging.py
[cli-main-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/cli/main.py
[compiler-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/compiler.py
[trouve-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/trouves/trouve.py
[test-runner-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/core/test_runner.py
[pool-py]: https://github.com/rivage-sh/clair/blob/main/src/clair/adapters/pool.py
