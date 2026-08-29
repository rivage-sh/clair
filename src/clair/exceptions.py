"""All the custom exceptions for Clair."""


class ClairError(Exception):
    """The parent class of all the Clair errors."""


class CyclicDependencyError(ClairError):
    """Clair raises this error when the DAG contains a cycle."""

    def __init__(self, cycle: list[tuple[str, str]]) -> None:
        self.cycle = cycle
        nodes = [edge[0] for edge in cycle]
        cycle_str = " -> ".join(nodes + [nodes[0]])
        super().__init__(f"Clair found a cyclic dependency: {cycle_str}")


class EnvironmentNotFoundError(ClairError):
    """Clair raises this error when environments.yml has no such environment."""

    def __init__(self, env_name: str, available: list[str]) -> None:
        self.env_name = env_name
        self.available = available
        super().__init__(
            f"Clair cannot find the environment '{env_name}' in environments.yml. "
            f"These environments are available: {', '.join(available)}"
        )


class EnvironmentsFileNotFoundError(ClairError):
    """Clair raises this error when ~/.clair/environments.yml does not exist."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(
            f"Clair cannot find environments.yml at {path}. "
            "Run `clair init` to make one."
        )


class InvalidTrouveAddressError(ClairError):
    """Clair raises this error when a name is not a valid Trouve address."""

    def __init__(self, physical_address: str, detail: str) -> None:
        self.physical_address = physical_address
        self.detail = detail
        super().__init__(f"Clair cannot use '{physical_address}' as an address: {detail}")


class InvalidEnvironmentError(ClairError):
    """Clair raises this error when an environment block holds a bad value."""

    def __init__(self, env_name: str, path: str, detail: str) -> None:
        self.env_name = env_name
        self.path = path
        self.detail = detail
        super().__init__(
            f"Clair cannot read the environment '{env_name}' in {path}. "
            "An unknown key is a misspelt name, or a routing block that belongs "
            f"in the project __routing__.py. Detail: {detail}"
        )


class InvalidRoutingFileError(ClairError):
    """Clair raises this error when it cannot use the project __routing__.py."""

    def __init__(self, path: str, detail: str) -> None:
        self.path = path
        self.detail = detail
        super().__init__(f"Invalid routing file at {path}: {detail}")


class InvalidRoutingConfigError(ClairError):
    """Clair raises this error when a routing entry returns an unusable address."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)



class ProjectRootNotFoundError(ClairError):
    """Clair raises this error when it finds no project marker file."""

    def __init__(self, start_directory: str, marker_file_name: str) -> None:
        self.start_directory = start_directory
        self.marker_file_name = marker_file_name
        super().__init__(
            f"Clair cannot find {marker_file_name} in {start_directory}, or in a "
            "directory above it. That file marks the root of a clair project. "
            "Run `clair init` to make a project, or give --project."
        )


class ProjectMarkerMissingError(ClairError):
    """Clair raises this error when the project root holds no marker file.

    A directory that holds many projects gives this error. Without the marker,
    clair reads such a directory as one project, and it builds one DAG from
    every project below it.
    """

    def __init__(self, project_root: str, marker_file_name: str) -> None:
        self.project_root = project_root
        self.marker_file_name = marker_file_name
        super().__init__(
            f"{project_root} holds no {marker_file_name}, thus it is not a clair "
            "project root. A directory that holds many projects gives this "
            "error: give --project the path of one project. Run `clair init` to "
            "make a new project."
        )


class InvalidProjectFileError(ClairError):
    """Clair raises this error when it cannot read the project marker file."""

    def __init__(self, path: str, detail: str) -> None:
        self.path = path
        self.detail = detail
        super().__init__(f"Clair cannot read the project file at {path}: {detail}")


class DiscoveryError(ClairError):
    """Clair raises this error when it cannot load a Trouve file."""

    def __init__(self, file_path: str, reason: str) -> None:
        self.file_path = file_path
        self.reason = reason
        super().__init__(f"Clair cannot load {file_path}: {reason}")


class ProjectDiscoveryError(ClairError):
    """Clair raises this error when discovery cannot read each Trouve of a project.

    The error holds each fault, because one broken import can hide a second one.
    A run that continues would build fewer Trouves and report success, thus
    discovery stops instead.
    """

    def __init__(self, faults: list[str]) -> None:
        self.faults = faults
        count = "1 fault" if len(faults) == 1 else f"{len(faults)} faults"
        listed = "\n".join(f"  - {fault}" for fault in faults)
        super().__init__(
            f"Clair cannot read this project. Discovery found {count}:\n{listed}"
        )


class CompileError(ClairError):
    """Clair raises this error when it cannot compile the SQL."""


class RunError(ClairError):
    """Clair raises this error for a fatal runner fault, not for a Trouve failure."""


class ResultNotFoundError(ClairError):
    """A run summary holds no result for the address that the caller gave."""
