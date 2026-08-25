# The anatomy of a run

`clair run` does seven steps. This page names each step, and it tells you why the step
exists. Read it when you must know which table a Trouve reads in a partial run.

## The seven steps

| # | Step | Result |
|---|---|---|
| 1 | Load the environment | The connection settings, and the environment name |
| 2 | Find the routing entry | One `RoutingEntry`, or `None` |
| 3 | Discover the project | One compiled Trouve for each file |
| 4 | Build the DAG | The dependency graph |
| 5 | Apply the selection | The Trouves that this run builds |
| 6 | Resolve the addresses | The table that each Trouve reads |
| 7 | Execute | One Snowflake object for each Trouve |

## 1. Load the environment

`clair run --env dev` reads the `dev` profile from `~/.clair/environments.yml`. The profile
gives the account, the warehouse, the role, and the credentials. It also gives the
environment name, which step 2 needs. See [Environments](environments.md).

## 2. Find the routing entry

clair loads `__routing__.py` and finds the entry with the environment name of step 1. An
environment with no entry gets `None`, which means passthrough: each physical address is
the logical address. See [Routing](../guides/routing.md).

## 3. Discover the project

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

A pandas Trouve holds the same information in a list. Its `input_addresses` gives the
address of each input, in the parameter order of the transform.

## 4. Build the DAG

Each DAG node holds the physical address of a Trouve, because that address is unique in
the warehouse. The compiled `imports` hold logical addresses, thus clair maps one to the
other to make each edge. See [DAG](dag.md).

## 5. Apply the selection

`--select` expands its globs and its `+` operators against the **logical** addresses.
clair then removes each SOURCE, because clair never builds a SOURCE, and it subtracts
`--exclude`. The result is the set of Trouves that this run builds.

## 6. Resolve the addresses

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
    `prod` an entry that gives the address back, as [Routing](../guides/routing.md) shows.

This step is the same for both backends. A SQL Trouve holds its addresses in its SQL, and
a pandas Trouve holds them in `input_addresses`. The rule above decides both. A `TestSql`
gets its addresses from the same step, thus a test reads the tables that its Trouve reads.

## 7. Execute

clair runs the Trouves in topological order. Each Trouve writes to a staging address, the
tests run against the staging data, and clair promotes the data to the physical address
after the tests pass. A failed test stops the promotion, thus the physical address keeps
its previous data. See [Staging](../guides/staging.md).

`clair run` logs both addresses for each node:

```
[info    ] run.node.start
  logical=mydb.derived.summary
  physical=mydb_ALICE.derived.summary
  effective_mode=full_refresh
```

## The related commands

`clair compile` does steps 1 to 6, and it writes the SQL to `target/`. It does not connect
to Snowflake. Run it to read the SQL that a run executes.

`clair validate` does steps 1 to 4, and it applies the routing entry to every Trouve. It
reports every routing problem at once, and it needs no Snowflake connection.
