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
gives an ``event_time`` as a subquery: ``(SELECT * FROM <address> WHERE
<event_time> in the window)``. A token from ``trouve.unsampled()`` always
renders as the address.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

from clair.core.project_imports import names_of_files_that_ran_two_times
from clair.environments.routing import TrouveAddress
from clair.exceptions import CompileError
from clair.trouves._refs import THIS_PLACEHOLDER, TROUVE_PLACEHOLDER_PREFIX, UNSAMPLED_MARKER
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.sample_window import SampleWindow
from clair.trouves.test import TestSql
from clair.trouves.trouve import DataframeInput, SqlTrouve, TrouveAbc, TrouveType

# Group 1 holds the unsampled marker, or None. Group 2 holds the object id.
_PLACEHOLDER_RE = re.compile(
    re.escape(TROUVE_PLACEHOLDER_PREFIX) + f"({re.escape(UNSAMPLED_MARKER)})?" + r"(\d+)"
)


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
            run filters. A token of such a Trouve becomes its subquery, and an
            unsampled token keeps the address. None samples nothing.

    Raises:
        CompileError: If the SQL qualifies a column with a sampled reference,
            as ``{orders}.order_id``. A subquery cannot qualify a column.
    """
    sampled_references = id_to_sampled_reference or {}

    def replace(match: re.Match[str]) -> str:
        object_id = int(match.group(2))
        is_unsampled = match.group(1) is not None
        sampled_reference = None if is_unsampled else sampled_references.get(object_id)
        address = id_to_address.get(object_id)
        if sampled_reference is None:
            return str(address) if address else match.group(0)
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
        dependency = id_to_logical_address.get(int(match.group(2)))
        if dependency is None or dependency == own_logical_address:
            continue
        if str(dependency) not in imports:
            imports.append(str(dependency))
    return imports



def describe_unresolved_tokens(
    collected: Sequence[tuple[TrouveAbc, TrouveAddress, Path, str]],
) -> list[str]:
    """Give one fault for each Trouve that keeps a placeholder token.

    A token stays in the SQL when clair holds no address for the object that
    the author interpolated. The module identity finder removes the common
    cause — one file that runs two times under two names — but an import
    machinery that clair does not see, for example the finder of an editable
    install, can still make a second module object. Clair must never send such
    SQL to the warehouse: the warehouse answers with a parse error that names
    the token and nothing else, and the DAG has already lost the edge in
    silence.
    """
    faulty: list[tuple[TrouveAddress, Path]] = []
    for trouve_obj, logical_address, file_path, _ in collected:
        compiled = trouve_obj.compiled
        if compiled is None:
            continue
        texts = [compiled.resolved_sql]
        texts.extend(
            test.resolved_sql
            for test in trouve_obj.tests
            if isinstance(test, TestSql) and test.resolved_sql
        )
        if any(_PLACEHOLDER_RE.search(text) for text in texts):
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
            reference_addresses[id(trouve)] = trouve.compiled.logical_address
    return reference_addresses


def _sampled_references(
    trouves: Sequence[TrouveAbc],
    reference_addresses: dict[int, TrouveAddress],
    sample: SampleWindow | None,
) -> dict[int, str]:
    """Give the subquery of each Trouve that the sample window filters, keyed by the object id."""
    if sample is None:
        return {}
    return {
        id(trouve): sample.reference(str(reference_addresses[id(trouve)]), trouve.event_time)
        for trouve in trouves
        if trouve.event_time is not None and id(trouve) in reference_addresses
    }


def _dataframe_input(
    upstream: TrouveAbc,
    reference_addresses: dict[int, TrouveAddress],
    sample: SampleWindow | None,
) -> DataframeInput:
    """Give the read of one input of a DataFrame Trouve."""
    row_filter = None
    if sample is not None and upstream.event_time is not None:
        row_filter = sample.row_filter(upstream.event_time)
    return DataframeInput(address=str(reference_addresses[id(upstream)]), row_filter=row_filter)


def recompile_for_selection(
    trouves: Sequence[TrouveAbc],
    selected_addresses: set[str],
    sample: SampleWindow | None = None,
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

    A sample window filters each reference to a Trouve that gives an
    ``event_time``, in the SQL and in the inputs of a DataFrame Trouve. It does
    not filter the SQL of a test: a test examines the object that the run
    built, and a test that reads a second Trouve needs each of its rows.

    Args:
        trouves: Each Trouve from discover_project().
        selected_addresses: The physical addresses of the Trouves for this run.
            The DAG selector gives them.
        sample: The window of a sampled run, or None to read each row.

    Raises:
        CompileError: If a sampled reference qualifies a column. See
            ``resolve_sql``.
    """
    reference_addresses = _reference_addresses_for_selection(
        trouves, selected_addresses
    )
    sampled_references = _sampled_references(trouves, reference_addresses, sample)

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
                        _dataframe_input(upstream, reference_addresses, sample)
                        for upstream in trouve.upstream_trouves()
                    ]
                }
            )

        for test in trouve.tests:
            if isinstance(test, TestSql):
                test.resolved_sql = resolve_sql(
                    test.sql, reference_addresses, this_address=this_address
                )
