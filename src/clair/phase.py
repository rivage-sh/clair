"""Phase -- one step of the lifecycle of a Trouve.

Each statement that clair sends to materialize a Trouve belongs to one phase.
``Trouve.plan()`` gives the statements of each phase, the runner executes the
phases in the order of this enum, and each statement of a run result names its
phase. ``clair compile`` and the artifact of ``clair run`` give each phase a
header, thus the plan and the record of the run read the same way.

This module imports nothing from clair, thus each layer can import it.
"""

from __future__ import annotations

from enum import StrEnum


class Phase(StrEnum):
    """One step of the lifecycle of a Trouve. The order of the members is the order of a run.

    PREPARE makes the target ready: an incremental run clones production, and
    adds each new column. BUILD writes the rows. CONFIGURE sets the properties
    of the table after the write. TEST gives the data quality tests as queries.
    PROMOTE makes production hold the tested data. DOCUMENT writes the docs as
    comments. RETIRE deals with the promoted staging object. CLEANUP removes each
    object that the run made for itself.
    """

    PREPARE = "prepare"
    BUILD = "build"
    CONFIGURE = "configure"
    TEST = "test"
    PROMOTE = "promote"
    DOCUMENT = "document"
    RETIRE = "retire"
    CLEANUP = "cleanup"

    @property
    def condition(self) -> str:
        """Tell when the runner executes this phase, or give an empty text for each run."""
        return _CONDITIONS.get(self, "")

    @property
    def header(self) -> str:
        """Give the SQL comment line that names this phase in compiled SQL and in a run artifact."""
        label = self.value if not self.condition else f"{self.value} · {self.condition}"
        return f"-- ── {label} " + "─" * max(4, 56 - len(label))


_CONDITIONS = {
    Phase.TEST: "a blocking test that fails stops the promotion",
    Phase.PROMOTE: "when should_promote() gives True",
    Phase.RETIRE: "after the promotion",
    Phase.CLEANUP: "always, also after a failure",
}
