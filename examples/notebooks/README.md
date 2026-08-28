# Notebooks

Four notebooks that show the Python API of clair. Each one runs against the projects in
`examples/projects/`, and **none of them opens a warehouse connection**. Thus you can run
each cell on a new machine, with no Snowflake account and no credit.

| Notebook | Shows |
|----------|-------|
| [01_python_api_tour.ipynb](01_python_api_tour.ipynb) | `clair.compile()`, `clair.validate()`, `clair.catalog()` and `clair.clean()`. The selectors, the run modes, the routing, a column report and a test coverage report. |
| [02_lineage_and_impact.ipynb](02_lineage_and_impact.ipynb) | The DAG as a networkx graph: the layers of a project, the blast radius of a source, the build order of a report, the longest chain, the parallel groups, and two drawings. |
| [03_run_without_snowflake.ipynb](03_run_without_snowflake.ipynb) | A complete `WarehouseAdapter` that holds its tables in memory. `clair.run()` builds the project against it, thus you read each statement, the staging and promotion mechanism, the test results, and the behaviour after a Trouve fails. |
| [04_author_trouves.ipynb](04_author_trouves.ipynb) | Write a Trouve in the notebook: call a `PandasTrouve` transform on your own DataFrame, read a `SeedTrouve`, and compile a project that the notebook writes to a temporary directory. |

Read them in that order. 01 gives the operations, and each other notebook uses one of them.

## Run the notebooks

From the repository root:

```sh
uv sync --extra examples
uv run jupyter notebook examples/notebooks/
```

The `examples` extra adds jupyter, matplotlib and pyvis. clair itself needs none of them.

## What the notebooks do to your machine

- They write no file to your home directory. Each notebook that needs an environment makes
  an `environments.yml` in a temporary directory, and it removes that directory at the end.
- They write the compiled SQL to the artifacts directory of the example project, and the
  last cell removes it with `clair.clean()`.
- Notebook 02 writes `dag.html` in this directory. git does not track that file.

## The documentation

`site_docs/docs/` is the source of truth for the behaviour of clair. CI publishes it at
[rivage-sh.github.io/clair](https://rivage-sh.github.io/clair/), and each link below goes
to that site:

- [The Python API reference](https://rivage-sh.github.io/clair/reference/python-api/)
- [The Trouve](https://rivage-sh.github.io/clair/topics/trouve/)
- [The topics](https://rivage-sh.github.io/clair/topics/): the DAG, the routing, the staging,
  the seeds, the incrementality, and the pandas Trouves
