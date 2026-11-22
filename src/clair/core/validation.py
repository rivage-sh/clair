"""The data of `clair validate`, and the text that the command shows.

``validate_project`` applies the routing entry to each Trouve and gives a
:class:`ValidationReport`. The report holds each problem as an object, thus a
caller reads the problem and parses no text. ``format_validation_report`` makes
the text for stdout.

The command finds four faults:

- An address that the routing entry makes, but Snowflake refuses.
- Two Trouves that go to one physical address.
- An address that an author writes as text, which makes no DAG edge.
- A build plan that clair cannot make, or that ignores the staging address.
  ``Trouve.plan()`` needs no connection, thus the command calls it for each run
  mode that a Trouve can reach, for each type of Trouve, at the routed address
  that a run writes to. An override of a phase gets the same test, and the name
  of each object that the run makes for itself gets the address rules.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from clair.core.compiler import offline_plan_context
from clair.core.discovery import discover_project
from clair.core.staging import PLACEHOLDER_RUN_ID, StagingAddressError
from clair.core.text_references import TextReference, find_text_references
from clair.environments.project_routing import (
    describe_unnamed_environment,
    load_project_routing,
)
from clair.environments.routing import (
    collect_routing_problems,
    describe_routing,
    detect_routing_collisions,
    route,
)
from clair.trouves.address import TrouveAddress
from clair.trouves.plan import PlanContext
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import MaterializedTrouve, TrouveAbc


class RoutingAddressProblem(BaseModel):
    """One Trouve whose physical address is not valid.

    Attributes:
        logical_address: The address of the Trouve in the project.
        detail: The reason that clair refuses the physical address.
    """

    logical_address: str
    detail: str


class RoutingCollision(BaseModel):
    """One physical address that two Trouves or more write to.

    Attributes:
        physical_address: The address that the Trouves share.
        logical_addresses: The Trouves that write to it, in name order.
    """

    physical_address: str
    logical_addresses: list[str]


class BuildPlanProblem(BaseModel):
    """One Trouve whose build plan clair cannot make, or that is not safe.

    Attributes:
        logical_address: The address of the Trouve in the project.
        run_mode: The effective run mode of the plan.
        staged: True if clair gave the plan a staging address.
        detail: The reason.
    """

    logical_address: str
    run_mode: RunMode
    staged: bool
    detail: str


def collect_build_plan_problems(trouves: list[TrouveAbc]) -> list[BuildPlanProblem]:
    """Make the plan of each Trouve for each run that it can get, and report each fault.

    Each Trouve must hold its physical address, as in a run.

    A run with tests writes to the staging address, and a run without tests
    writes to the physical address. Thus clair makes each plan with each target
    address, and the build phase must name the address that it got. A staged
    build that does not writes to the physical object, before the tests and
    without the promotion.
    """
    problems: list[BuildPlanProblem] = []
    for trouve in trouves:
        if not isinstance(trouve, MaterializedTrouve) or trouve.compiled is None:
            continue
        logical_address = str(trouve.logical_address)
        seen: set[tuple[RunMode, bool]] = set()
        for run_mode in RunMode:
            for staged in (True, False):
                try:
                    context = offline_plan_context(trouve, run_mode, PLACEHOLDER_RUN_ID, staged)
                except StagingAddressError as naming_error:
                    # The staging name does not depend on the run mode.
                    if any(problem.staged == staged and problem.detail == str(naming_error) for problem in problems):
                        continue
                    problems.append(BuildPlanProblem(
                        logical_address=logical_address,
                        run_mode=run_mode,
                        staged=staged,
                        detail=str(naming_error),
                    ))
                    continue
                except Exception as error:  # noqa: BLE001 — effective_mode() is user code, and it can raise anything
                    problems.append(BuildPlanProblem(
                        logical_address=logical_address,
                        run_mode=run_mode,
                        staged=staged,
                        detail=f"effective_mode() raised {type(error).__name__}: {error}",
                    ))
                    continue
                if (context.effective_mode, staged) in seen:
                    continue
                seen.add((context.effective_mode, staged))
                detail = _build_plan_fault(trouve, context)
                if detail is not None:
                    problems.append(
                        BuildPlanProblem(
                            logical_address=logical_address,
                            run_mode=context.effective_mode,
                            staged=staged,
                            detail=detail,
                        )
                    )
    return problems


def _build_plan_fault(trouve: MaterializedTrouve, context: PlanContext) -> str | None:
    """Give the fault of one plan, or None if the plan is correct."""
    try:
        plan = trouve.plan(context)
    except Exception as error:  # noqa: BLE001 — an override is user code, and it can raise anything
        return f"plan() raised {type(error).__name__}: {error}"
    if not plan.build:
        return None
    target_address = context.target_address
    if not any(str(target_address) in statement for statement in plan.build):
        return (
            f"the build phase got the target address {target_address}, and no statement "
            "names it. The plan then writes to a different object, and a staged run "
            "writes to the physical object before the tests run. Write to "
            "`context.target_address`."
        )
    return None


class ValidationReport(BaseModel):
    """Everything that `clair validate` found.

    Attributes:
        env_name: The environment that the routing entry belongs to.
        routing_file: The path of `__routing__.py`, or None.
        routing_description: The name of the entry class, or the passthrough text.
        routable_count: The number of Trouves that clair routed.
        address_problems: Each Trouve with an invalid physical address.
        collisions: Each physical address that more than one Trouve writes to.
        text_references: Each address that an author wrote as text.
        build_plan_problems: Each plan that clair cannot make, or that ignores
            the staging address.
        unnamed_environment_warning: The text that tells the user that the
            routing file does not name this environment, or None. This is a
            warning and not a problem: clair then writes to the logical
            addresses, which are the production addresses.
    """

    env_name: str
    routing_file: Path | None
    routing_description: str
    routable_count: int
    address_problems: list[RoutingAddressProblem]
    collisions: list[RoutingCollision]
    text_references: list[TextReference]
    build_plan_problems: list[BuildPlanProblem]
    unnamed_environment_warning: str | None

    @property
    def problem_count(self) -> int:
        """Give the number of problems of each kind together."""
        return (
            len(self.address_problems)
            + len(self.collisions)
            + len(self.text_references)
            + len(self.build_plan_problems)
        )

    @property
    def is_valid(self) -> bool:
        """Tell you if the project has no problem."""
        return self.problem_count == 0

    def render(self) -> str:
        """Give the complete report text for stdout."""
        return format_validation_report(self)


def validate_project(project_root: Path, env_name: str) -> ValidationReport:
    """Apply the routing entry of *env_name* to each Trouve of the project.

    Clair discovers the project with routing off. A bad entry then reports as a
    routing problem, and it does not stop discovery at the first Trouve.

    Args:
        project_root: The root directory of the project.
        env_name: The environment name that selects the routing entry.

    Returns:
        The complete report. The report holds no problem when the project is
        valid.

    Raises:
        ClairError: If clair cannot read the routing file or the project.
    """
    project_routing = load_project_routing(project_root, env_name)
    discovered = discover_project(project_root, routing=None)
    routing = project_routing.entry

    # Keep the (logical address, type) pair, and not the Trouve. The address is
    # the only part that the collision report needs, and here it is never None.
    routable = [
        (trouve.logical_address, trouve.type)
        for trouve in discovered
        if trouve.compiled is not None
    ]

    address_problems = [
        RoutingAddressProblem(logical_address=logical_address, detail=detail)
        for logical_address, detail in collect_routing_problems(discovered, routing)
    ]

    # A collision test needs a physical address for each Trouve. One bad address
    # means that clair cannot make them, thus the collision test waits. The build
    # plans wait too: a run writes to the physical address, and a run object
    # name comes from it.
    collisions: list[RoutingCollision] = []
    build_plan_problems: list[BuildPlanProblem] = []
    if not address_problems:
        logical_to_physical = {
            str(logical_address): route(logical_address, trouve_type, routing)
            for logical_address, trouve_type in routable
        }
        collisions = [
            RoutingCollision(
                physical_address=physical_address, logical_addresses=logical_addresses
            )
            for physical_address, logical_addresses in detect_routing_collisions(
                logical_to_physical
            )
        ]
        build_plan_problems = collect_build_plan_problems(
            _at_physical_addresses(discovered, logical_to_physical)
        )

    return ValidationReport(
        env_name=env_name,
        routing_file=project_routing.file_path,
        routing_description=describe_routing(routing),
        routable_count=len(routable),
        address_problems=address_problems,
        collisions=collisions,
        text_references=find_text_references(discovered),
        build_plan_problems=build_plan_problems,
        unnamed_environment_warning=describe_unnamed_environment(
            project_routing, env_name
        ),
    )


def _at_physical_addresses(
    trouves: list[TrouveAbc], logical_to_physical: dict[str, TrouveAddress]
) -> list[TrouveAbc]:
    """Give a copy of each compiled Trouve that holds its routed physical address, as in a run."""
    routed_trouves: list[TrouveAbc] = []
    for trouve in trouves:
        if trouve.compiled is None:
            continue
        physical_address = logical_to_physical[str(trouve.logical_address)]
        routed_compiled = trouve.compiled.model_copy(update={"physical_address": physical_address})
        routed_trouves.append(trouve.model_copy(update={"compiled": routed_compiled}))
    return routed_trouves


def format_validation_report(report: ValidationReport) -> str:
    """Give the text of one report for stdout."""
    lines: list[str] = []
    if report.unnamed_environment_warning:
        lines.append("")
        lines.append(f"Warning: {report.unnamed_environment_warning}")

    lines += [
        "",
        f"  environment: {report.env_name}",
        f"  routing file: {report.routing_file or 'none'}",
        f"  entry: {report.routing_description}",
        f"  Trouves to route: {report.routable_count}",
        "",
    ]

    for problem in report.address_problems:
        lines.append(f"  ✗ {problem.logical_address}")
        lines.append(f"    {problem.detail}")
        lines.append("")

    for collision in report.collisions:
        lines.append(f"  ✗ {collision.physical_address}")
        lines.append("    Two or more Trouves route to this one target:")
        lines.extend(
            f"      ↳ {logical_address}"
            for logical_address in collision.logical_addresses
        )
        lines.append("")

    for reference in report.text_references:
        lines.append(f"  ✗ {reference.logical_address}")
        lines.append(
            f"    The {reference.location} names '{reference.text_address}' as text."
        )
        lines.append(
            "    Import that Trouve and put it in an f-string. Clair then makes a DAG"
        )
        lines.append("    edge, and the routing entry moves the address.")
        lines.append("")

    for problem in report.build_plan_problems:
        staging_text = "with staging" if problem.staged else "without staging"
        lines.append(f"  ✗ {problem.logical_address}")
        lines.append(f"    The {problem.run_mode} plan, {staging_text}: {problem.detail}")
        lines.append("")

    if report.is_valid:
        lines.append(
            "  ✓ Every physical address is valid. No collisions. "
            "Each reference is a Trouve. Each build plan is correct."
        )
        lines.append("")
        return "\n".join(lines)

    label = "problem" if report.problem_count == 1 else "problems"
    lines.append(f"  {report.problem_count} {label} found.")
    lines.append("")
    return "\n".join(lines)
