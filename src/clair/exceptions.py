"""All custom exceptions for Clair."""


class ClairError(Exception):
    """Base exception for all Clair errors."""


class CyclicDependencyError(ClairError):
    """Raised when the DAG contains a cycle."""

    def __init__(self, cycle: list[tuple[str, str]]) -> None:
        self.cycle = cycle
        nodes = [edge[0] for edge in cycle]
        cycle_str = " -> ".join(nodes + [nodes[0]])
        super().__init__(f"Cyclic dependency detected: {cycle_str}")


class EnvironmentNotFoundError(ClairError):
    """Raised when the requested environment doesn't exist in environments.yml."""

    def __init__(self, env_name: str, available: list[str]) -> None:
        self.env_name = env_name
        self.available = available
        super().__init__(
            f"Environment '{env_name}' not found in environments.yml. "
            f"Available environments: {', '.join(available)}"
        )


class EnvironmentsFileNotFoundError(ClairError):
    """Raised when ~/.clair/environments.yml doesn't exist."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(
            f"environments.yml not found at {path}. Run `clair init` to create one."
        )


class RoutingInEnvironmentsFileError(ClairError):
    """Raised when environments.yml still holds a routing block.

    Routing moved out of environments.yml and into the project __routing__.py.
    A silent skip of the old block would send writes to the production names, so
    clair stops and asks the user to move the rule.
    """

    def __init__(self, path: str, env_name: str) -> None:
        self.path = path
        self.env_name = env_name
        super().__init__(
            f"Environment '{env_name}' in {path} has a 'routing' block, but "
            "routing moved to the project. Delete the block, then add the rule "
            f"to __routing__.py under the key '{env_name}'. "
            "Run `clair validate` to test the new rule."
        )


class InvalidRoutingFileError(ClairError):
    """Raised when the project __routing__.py exists but clair cannot use it."""

    def __init__(self, path: str, detail: str) -> None:
        self.path = path
        self.detail = detail
        super().__init__(f"Invalid routing file at {path}: {detail}")


class InvalidRoutingConfigError(ClairError):
    """Raised when a routing rule fails, or returns an unusable name."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)



class DiscoveryError(ClairError):
    """Raised when a Trouve file cannot be loaded."""

    def __init__(self, file_path: str, reason: str) -> None:
        self.file_path = file_path
        self.reason = reason
        super().__init__(f"Failed to load {file_path}: {reason}")


class CompileError(ClairError):
    """Raised when SQL compilation fails."""


class RunError(ClairError):
    """Raised when a critical runner error occurs (not per-Trouve failures)."""
