"""A Trouve reference becomes an address. This module holds that step.

``trouves/_refs.py`` holds the other half. An f-string that interpolates a
Trouve, as ``f"select * from {orders}"``, calls ``Trouve.__format__``. That
method registers the object and gives a token, ``__CLAIR_TROUVE_<id>``, thus
the SQL of the author carries the identity of the object as text.

This module reads those tokens back. It takes a map from the object id to an
address, and it renders the SQL. Clair renders each Trouve two times from one
source string:

* ``discover_project()`` renders the logical addresses. It does not know the
  selection of the run yet.
* ``recompile_for_selection()`` renders again, and the selection now decides
  each address.

Both calls read ``SqlTrouve.sql``, which keeps its tokens. Thus the second call
needs no text substitution on the result of the first, and an address that the
author types as text stays as the author wrote it.

The object id is the join key, thus one file must give one Trouve object. See
``core/project_imports.py``.

A sampled run (``clair run --sample``) renders a reference to a Trouve that
the run does not build as the subquery of ``Trouve.sample()``. A Trouve that the
run builds is a sample already, thus a reference to it keeps the address.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

from clair.core.project_imports import names_of_files_that_ran_two_times
from clair.environments.routing import TrouveAddress
from clair.exceptions import CompileError
from clair.trouves._refs import THIS_PLACEHOLDER, TROUVE_PLACEHOLDER_PREFIX
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.test import TestSql
from clair.trouves.trouve import (
    DEFAULT_SAMPLE_ROWS,
    DataframeInput,
    SqlTrouve,
    TrouveAbc,
    TrouveType,
)

_PLACEHOLDER_RE = re.compile(re.escape(TROUVE_PLACEHOLDER_PREFIX) + r"(\d+)")


def resolve_sql(
    sql: str,
    id_to_address: dict[int, TrouveAddress],
    this_address: TrouveAddress,
    id_to_sampled_reference: dict[int, str] | None = None,
) -> str:
    """Render the SQL of the author into SQL with true addresses.

    The function replaces a token that points to a different Trouve
    (``__CLAIR_TROUVE_<id>``) with the address in ``id_to_address``. It also
    replaces the THIS marker (``__CLAIR_THIS__``) with ``this_address``.

    Clair calls this function two times, and the map decides the difference.
    discover_project() gives the logical addresses, because it does not know the
    selection. recompile_for_selection() gives the address that the selection
    decides. Both calls read the same source string, thus the second call needs
    no text substitution on the result of the first.

    Args:
        id_to_sampled_reference: The subquery of each Trouve that a sampled
            run reads in place of the address. None samples nothing.

    Raises:
        CompileError: If the SQL qualifies a column with a sampled reference,
            as ``{orders}.order_id``. A subquery cannot qualify a column.
        CompileError: If a token points to an object without an address. Thus
            no SQL from this function holds a token, and no token gets to the
            warehouse. ``describe_unresolved_tokens()`` finds this fault first,
            and its message names the file that ran two times.
    """
    sampled_references = id_to_sampled_reference or {}

    def replace(match: re.Match[str]) -> str:
        object_id = int(match.group(1))
        sampled_reference = sampled_references.get(object_id)
        address = id_to_address.get(object_id)
        if address is None:
            raise CompileError(
                f"The SQL of '{this_address}' interpolates a Trouve object "
                "that clair did not collect, thus clair cannot give it an address."
            )
        if sampled_reference is None:
            return str(address)
        if sql.startswith(".", match.end()):
            raise CompileError(
                f"The SQL of '{this_address}' qualifies a column with the "
                f"reference to '{address}', and a sampled run makes that "
                "reference a subquery. Give the reference an alias, for "
                "example `FROM {orders} AS orders`, and write `orders.<column>`."
            )
        return sampled_reference

    result = _PLACEHOLDER_RE.sub(replace, sql)
    return result.replace(THIS_PLACEHOLDER, str(this_address))


def detect_imports(
    sql: str,
    id_to_logical_address: dict[int, TrouveAddress],
    own_logical_address: TrouveAddress,
) -> list[str]:
    """Give the logical address of each Trouve that the SQL points to with a token."""
    imports: list[str] = []
    for match in _PLACEHOLDER_RE.finditer(sql):
        dependency = id_to_logical_address.get(int(match.group(1)))
        if dependency is None or dependency == own_logical_address:
            continue
        if str(dependency) not in imports:
            imports.append(str(dependency))
    return imports


def texts_with_tokens(trouve: TrouveAbc) -> list[str]:
    """Give each SQL text of the author that can hold a reference token.

    Each text is the source, before clair renders it: the sample, the SQL, and
    the SQL of each TestSql. Add a new SQL field of a Trouve here, thus the
    token guard reads it too.
    """
    texts = [trouve.sample(DEFAULT_SAMPLE_ROWS)]
    if isinstance(trouve, SqlTrouve):
        texts.append(trouve.sql)
    texts.extend(test.sql for test in trouve.tests if isinstance(test, TestSql))
    return texts


def describe_unresolved_tokens(
    collected: Sequence[tuple[TrouveAbc, TrouveAddress, Path, str]],
    id_to_address: dict[int, TrouveAddress],
) -> list[str]:
    """Give one fault for each Trouve with a token that has no address.

    A token has no address when the author interpolated an object that clair
    did not collect. The module identity finder removes the common cause — one
    file that runs two times under two names — but an import machinery that
    clair does not see, for example the finder of an editable install, can
    still make a second module object.

    Discovery calls this function before it compiles, thus the fault stops each
    run, also a run without --sample. Each DAG holds the edge of a
    ``sample_sql``, and a token without an address removes that edge in
    silence. ``resolve_sql()`` also refuses such a token, but its message
    cannot name the file that ran two times.
    """
    faulty: list[tuple[TrouveAddress, Path]] = []
    for trouve_obj, logical_address, file_path, _ in collected:
        if any(
            int(match.group(1)) not in id_to_address
            for text in texts_with_tokens(trouve_obj)
            for match in _PLACEHOLDER_RE.finditer(text)
        ):
            faulty.append((logical_address, file_path))
    if not faulty:
        return []

    # A file that ran two times is the probable cause, and that file is the
    # referenced file, not the file that keeps the token. Name each such file.
    duplicates = names_of_files_that_ran_two_times(
        file_path for _, _, file_path, _ in collected
    )
    duplicate_text = ""
    if duplicates:
        listed = "; ".join(
            f"{file_path.name} as " + " and ".join(names)
            for file_path, names in sorted(duplicates.items())
        )
        duplicate_text = f" Python ran one file two times: {listed}."

    faults: list[str] = []
    for logical_address, file_path in faulty:
        faults.append(
            f"{file_path}: the Trouve '{logical_address}' interpolates a "
            "Trouve object that clair did not collect, thus a reference token "
            "stays in the SQL. Clair stops, because the warehouse cannot read "
            "that SQL." + duplicate_text
        )
    return faults


def _reference_addresses_for_selection(
    trouves: Sequence[TrouveAbc], selected_addresses: set[str]
) -> dict[int, TrouveAddress]:
    """Give the address that each Trouve reads at, keyed by the object id.

    Three rules decide the address:

    * This run builds the Trouve, thus a reader takes the physical address. The
      new data goes there.
    * This run does not build the Trouve, thus a reader takes the logical
      address. Nothing writes a new copy, thus the production table holds the
      newest data.
    * The Trouve is a SOURCE, thus a reader takes the physical address. Clair
      never builds a SOURCE, thus the routing entry is the only statement about
      where the data is.
    """
    reference_addresses: dict[int, TrouveAddress] = {}
    for trouve in trouves:
        if not trouve.compiled:
            continue
        this_run_builds_it = (
            str(trouve.compiled.physical_address) in selected_addresses
        )
        if trouve.type == TrouveType.SOURCE or this_run_builds_it:
            reference_addresses[id(trouve)] = trouve.compiled.physical_address
        else:
            reference_addresses[id(trouve)] = trouve.logical_address
    return reference_addresses


def render_sample(
    trouve: TrouveAbc,
    id_to_address: dict[int, TrouveAddress],
    address: TrouveAddress,
    sample_rows: int,
) -> str:
    """Give the subquery that a sampled read of *trouve* at *address* executes.

    ``{THIS}`` becomes *address*. A reference to another Trouve in the
    ``sample_sql`` becomes its address in *id_to_address*, and never a sample
    of its own: thus one sample reads no second sample.
    """
    return resolve_sql(trouve.sample(sample_rows), id_to_address, this_address=address)


def _sampled_references(
    trouves: Sequence[TrouveAbc],
    selected_addresses: set[str],
    reference_addresses: dict[int, TrouveAddress],
    sample_rows: int | None,
) -> dict[int, str]:
    """Give the sample of each Trouve that the run does not build, keyed by the object id.

    A Trouve that the run builds is a sample already. A second sample of it
    would make each layer of the DAG smaller than the layer before it.
    """
    if sample_rows is None:
        return {}
    sampled_references: dict[int, str] = {}
    for trouve in trouves:
        if trouve.compiled is None:
            continue
        if str(trouve.compiled.physical_address) in selected_addresses:
            continue
        sampled_references[id(trouve)] = render_sample(
            trouve, reference_addresses, reference_addresses[id(trouve)], sample_rows
        )
    return sampled_references


def recompile_for_selection(
    trouves: Sequence[TrouveAbc],
    selected_addresses: set[str],
    sample_rows: int | None = None,
) -> None:
    """Resolve each address again, now that clair knows the selection.

    discover_project() resolves each reference to a logical production address,
    because it does not know the selection yet. This function resolves each
    reference a second time, and the selection now decides each address. See
    ``_reference_addresses_for_selection`` for the rule.

    The function reads the placeholder tokens of the author, and not the
    addresses that discovery wrote. ``SqlTrouve.sql`` and ``TestSql.sql`` keep
    those tokens, thus clair renders the SQL again from the source. Only a token
    becomes an address. An address that the author types as text stays as it is,
    and it makes no DAG edge either.

    This function changes each Trouve in place. It writes three places: the
    resolved_sql of a SQL Trouve, the resolved_sql of each TestSql, and the
    inputs of a DataFrame Trouve. It changes nothing for a Trouve outside
    the selection, because this run does not execute that Trouve.

    A sampled run reads ``Trouve.sample()`` in place of each Trouve that the
    run does not build, in the SQL and in the inputs of a DataFrame Trouve. It
    does not sample the SQL of a test: a test examines the object that the run
    built, and a test that reads a second Trouve needs each of its rows.

    Args:
        trouves: Each Trouve from discover_project().
        selected_addresses: The physical addresses of the Trouves for this run.
            The DAG selector gives them.
        sample_rows: The row limit of a sampled run, for each Trouve without a
            ``sample_sql``. None reads each row.

    Raises:
        CompileError: If a sampled reference qualifies a column. See
            ``resolve_sql``.
    """
    reference_addresses = _reference_addresses_for_selection(
        trouves, selected_addresses
    )
    sampled_references = _sampled_references(
        trouves, selected_addresses, reference_addresses, sample_rows
    )

    for trouve in trouves:
        if not trouve.compiled:
            continue
        if str(trouve.compiled.physical_address) not in selected_addresses:
            continue

        # The Trouve writes to its own physical address, thus its own SQL points
        # to the physical address too. An incremental Trouve reads the target
        # with the THIS marker.
        this_address = trouve.compiled.physical_address

        if isinstance(trouve, SqlTrouve):
            trouve.compiled = trouve.compiled.model_copy(
                update={
                    "resolved_sql": resolve_sql(
                        trouve.sql,
                        reference_addresses,
                        this_address=this_address,
                        id_to_sampled_reference=sampled_references,
                    )
                }
            )
        elif isinstance(trouve, DataframeTrouve):
            # A DataFrame Trouve names each input in a list, and not in SQL.
            trouve.compiled = trouve.compiled.model_copy(
                update={
                    "inputs": [
                        DataframeInput(
                            address=str(reference_addresses[id(upstream)]),
                            sample_query=sampled_references.get(id(upstream)),
                        )
                        for upstream in trouve.upstream_trouves()
                    ]
                }
            )

        for test in trouve.tests:
            if isinstance(test, TestSql):
                test.resolved_sql = resolve_sql(
                    test.sql, reference_addresses, this_address=this_address
                )
