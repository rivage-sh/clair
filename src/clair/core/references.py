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

Both calls read ``Trouve.sql``, which keeps its tokens. Thus the second call
needs no text substitution on the result of the first, and an address that the
author types as text stays as the author wrote it.

The object id is the join key, thus one file must give one Trouve object. See
``core/project_imports.py``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

from clair.core.project_imports import names_of_a_file_that_ran_two_times
from clair.environments.routing import TrouveAddress
from clair.trouves._refs import THIS_PLACEHOLDER, TROUVE_PLACEHOLDER_PREFIX
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.test import TestSql
from clair.trouves.trouve import Trouve, TrouveAbc, TrouveType

_PLACEHOLDER_RE = re.compile(re.escape(TROUVE_PLACEHOLDER_PREFIX) + r"(\d+)")


def resolve_sql(
    sql: str,
    id_to_address: dict[int, TrouveAddress],
    this_address: TrouveAddress,
) -> str:
    """Render the SQL of the author into SQL with true addresses.

    The function replaces a token that points to a different Trouve
    (``__CLAIR_TROUVE_<id>__``) with the address in ``id_to_address``. It also
    replaces the THIS marker (``__CLAIR_THIS__``) with ``this_address``.

    Clair calls this function two times, and the map decides the difference.
    discover_project() gives the logical addresses, because it does not know the
    selection. recompile_for_selection() gives the address that the selection
    decides. Both calls read the same source string, thus the second call needs
    no text substitution on the result of the first.
    """
    def replace(m: re.Match[str]) -> str:
        address = id_to_address.get(int(m.group(1)))
        return str(address) if address else m.group(0)
    result = _PLACEHOLDER_RE.sub(replace, sql)
    return result.replace(THIS_PLACEHOLDER, str(this_address))


def detect_imports(
    sql: str,
    id_to_logical_address: dict[int, TrouveAddress],
    own_logical_address: TrouveAddress,
) -> list[str]:
    """Give the logical address of each Trouve that the SQL points to with a token."""
    imports: list[str] = []
    for obj_id_str in _PLACEHOLDER_RE.findall(sql):
        dependency = id_to_logical_address.get(int(obj_id_str))
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
    # A file that ran two times is the probable cause, and that file is the
    # referenced file, not the file that keeps the token. Name each such file.
    duplicates = {
        file_path.name: names
        for _, _, file_path, _ in collected
        if (names := names_of_a_file_that_ran_two_times(file_path))
    }
    duplicate_text = ""
    if duplicates:
        listed = "; ".join(
            f"{file_name} as " + " and ".join(names)
            for file_name, names in sorted(duplicates.items())
        )
        duplicate_text = f" Python ran one file two times: {listed}."

    faults: list[str] = []
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
        tokens = sorted({
            match.group(0) for text in texts for match in _PLACEHOLDER_RE.finditer(text)
        })
        if not tokens:
            continue
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


def recompile_for_selection(
    trouves: Sequence[TrouveAbc], selected_addresses: set[str]
) -> None:
    """Resolve each address again, now that clair knows the selection.

    discover_project() resolves each reference to a logical production address,
    because it does not know the selection yet. This function resolves each
    reference a second time, and the selection now decides each address. See
    ``_reference_addresses_for_selection`` for the rule.

    The function reads the placeholder tokens of the author, and not the
    addresses that discovery wrote. ``Trouve.sql`` and ``TestSql.sql`` keep
    those tokens, thus clair renders the SQL again from the source. Only a token
    becomes an address. An address that the author types as text stays as it is,
    and it makes no DAG edge either.

    This function changes each Trouve in place. It writes three places: the
    resolved_sql of a SQL Trouve, the resolved_sql of each TestSql, and the
    input_addresses of a DataFrame Trouve. It changes nothing for a Trouve outside
    the selection, because this run does not execute that Trouve.

    Args:
        trouves: Each Trouve from discover_project().
        selected_addresses: The physical addresses of the Trouves for this run.
            The DAG selector gives them.
    """
    reference_addresses = _reference_addresses_for_selection(
        trouves, selected_addresses
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

        if isinstance(trouve, Trouve):
            trouve.compiled = trouve.compiled.model_copy(
                update={
                    "resolved_sql": resolve_sql(
                        trouve.sql, reference_addresses, this_address=this_address
                    )
                }
            )
        elif isinstance(trouve, DataframeTrouve):
            # A DataFrame Trouve names each input in a list, and not in SQL.
            trouve.compiled = trouve.compiled.model_copy(
                update={
                    "input_addresses": [
                        str(reference_addresses[id(upstream)])
                        for upstream in trouve.upstream_trouves()
                    ]
                }
            )

        for test in trouve.tests:
            if isinstance(test, TestSql):
                test.resolved_sql = resolve_sql(
                    test.sql, reference_addresses, this_address=this_address
                )
