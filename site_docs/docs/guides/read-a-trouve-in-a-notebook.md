# Read a Trouve into pandas

This guide reads the data of a Trouve into a pandas DataFrame, in a notebook. You import
the Trouve from its file. clair gives its address, and the adapter reads the rows. The
examples use `examples/projects/example_4`, and
[`05_read_trouves_into_pandas.ipynb`](https://github.com/rivage-sh/clair/blob/main/examples/notebooks/05_read_trouves_into_pandas.ipynb)
does each step.

The notebook only reads. To change the data of a Trouve, edit its file and run
`clair run --env dev --select <address>`. The run builds at a staging address, runs the
tests, and promotes the object after they pass. See [Staging](../topics/staging.md).

## 1. Make the project importable

The notebook imports a Trouve file with the name that the project gives it. The Trouve
files of your project import each other in one of two ways, and that way tells you where
to start Jupyter.

| The Trouve files import each other | Example | Start Jupyter |
|---|---|---|
| Through a Python package. The project root holds `__init__.py`. | `from clair_projects.analytics.source.orders.raw import trouve` | At any directory from which Python imports the package, for example the root of the repository. You change no `sys.path`. |
| From the project root | `from example_4_database.refined.events import trouve` | In the project root, or add the project root to `sys.path` |

`example_4` uses the second way, thus a notebook outside its root adds the root:

```python
import sys

sys.path.insert(0, "/path/to/examples/projects/example_4")
```

See [Two names for one file](../topics/project-layout.md#two-names-for-one-file) for a
project inside a package.

## 2. Import the Trouve, and read its address

```python
from example_4_database.derived.daily_event_counts import trouve as daily_event_counts

daily_event_counts.logical_address               # example_4_database.derived.daily_event_counts
daily_event_counts.get_physical_address("dev")   # alice.derived.daily_event_counts
daily_event_counts.get_physical_address("prod")  # example_4_database.derived.daily_event_counts
```

| Address | It gives | It needs |
|---------|----------|----------|
| `logical_address` | The production address. The file path decides it. | The file only |
| `get_physical_address(env_name)` | The address that `clair run --env <env_name>` writes to | The file and `__routing__.py` |

Neither of the two connects to Snowflake, and neither runs a discovery of the project.
`get_physical_address()` finds `__routing__.py` above the file, and it calls the same
routing function as a run. Thus the two cannot disagree. With `CLAIR_USER=alice`, the
`dev` entry of `example_4` puts each Trouve in the database `alice`.

## 3. Read the data

```python
from clair import SnowflakeAdapter, load_environment

warehouse = SnowflakeAdapter.connected(load_environment("dev"))

production_rows = warehouse.fetch_dataframe(daily_event_counts)
dev_rows = warehouse.fetch_dataframe(daily_event_counts.get_physical_address("dev"))

warehouse.close()
```

`load_environment("dev")` reads the `dev` profile of `~/.clair/environments.yml`. The
profile gives the credentials only. It does not change the address that clair reads: a
Trouve always reads its logical address, the production address. To read the object of a
different environment, give its physical address, as `dev_rows` does.

clair applies no row limit, in the same way as `fetch_pandas_all()` of the Snowflake
connector. For a large table, aggregate in SQL first.

## Use your own connection

Put the address in your SQL. Write `.logical_address`, and not the Trouve itself:

```python
import pandas as pd

rows = pd.read_sql(
    f"select event_date, sum(event_count) from {daily_event_counts.logical_address} group by 1",
    my_connection,
)
```

A Trouve in an f-string gives a placeholder token, for example `__CLAIR_TROUVE_1402…__`,
because discovery replaces that token in a Trouve file. Outside a Trouve file, nothing
replaces it.

## When clair cannot give the address

| Condition | Result |
|-----------|--------|
| You made the Trouve in the notebook, and no file holds it | `TrouveFileNotFoundError` |
| No directory above the file holds `__routing__.py` | `get_physical_address()` raises `ProjectRootNotFoundError`. `logical_address` still works. |
| `__routing__.py` does not name the environment | Passthrough: the logical address, with the warning of a run |
| The Trouve file reads `clair.env` when it loads | The import raises: `clair.env` is `None`, because no discovery runs |

`clair.compile()` and `clair.run()` run each Trouve file again, and each module then holds a
new object. Import a Trouve again after one of them in the same notebook.
