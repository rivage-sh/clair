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
            f"environments.yml not found at {path}. "
            "Run `clair init` to create one, or rename your profiles.yml "
            "and add a routing block."
        )


class InvalidRoutingPolicyError(ClairError):
    """Raised when an unknown routing policy is specified."""

    def __init__(self, policy: str) -> None:
        self.policy = policy
        super().__init__(
            f"Unknown routing policy '{policy}'. "
            "Valid policies: database_override, schema_isolation"
        )


class InvalidRoutingConfigError(ClairError):
    """Raised when a routing config block is malformed."""

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
