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

    def __init__(self, physical_name: str, detail: str) -> None:
        self.physical_name = physical_name
        self.detail = detail
        super().__init__(f"Clair cannot use '{physical_name}' as an address: {detail}")


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



class DiscoveryError(ClairError):
    """Clair raises this error when it cannot load a Trouve file."""

    def __init__(self, file_path: str, reason: str) -> None:
        self.file_path = file_path
        self.reason = reason
        super().__init__(f"Clair cannot load {file_path}: {reason}")


class CompileError(ClairError):
    """Clair raises this error when it cannot compile the SQL."""


class RunError(ClairError):
    """Clair raises this error for a fatal runner fault, not for a Trouve failure."""
