"""All the custom exceptions for Clair."""


class ClairError(Exception):
    """The parent class of all the Clair errors."""


class CyclicDependencyError(ClairError):
    """Clair raises this error when the DAG contains a cycle."""

    def __init__(self, cycle: list[tuple[str, str]]) -> None:
        self.cycle = cycle
        nodes = [edge[0] for edge in cycle]
        cycle_str = " -> ".join(nodes + [nodes[0]])
        super().__init__(f"Cyclic dependency detected: {cycle_str}")


class EnvironmentNotFoundError(ClairError):
    """Clair raises this error when environments.yml has no such environment."""

    def __init__(self, env_name: str, available: list[str]) -> None:
        self.env_name = env_name
        self.available = available
        super().__init__(
            f"Environment '{env_name}' not found in environments.yml. "
            f"Available environments: {', '.join(available)}"
        )


class EnvironmentsFileNotFoundError(ClairError):
    """Clair raises this error when ~/.clair/environments.yml does not exist."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(
            f"environments.yml not found at {path}. "
            "Run `clair init` to create one, or rename your profiles.yml "
            "and add a routing block."
        )


class InvalidRoutingPolicyError(ClairError):
    """Clair raises this error when the config names an unknown routing policy."""

    def __init__(self, policy: str) -> None:
        self.policy = policy
        super().__init__(
            f"Unknown routing policy '{policy}'. "
            "Valid policies: database_override, schema_isolation"
        )


class InvalidRoutingConfigError(ClairError):
    """Clair raises this error when a routing config block has a bad structure."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)



class DiscoveryError(ClairError):
    """Clair raises this error when it cannot load a Trouve file."""

    def __init__(self, file_path: str, reason: str) -> None:
        self.file_path = file_path
        self.reason = reason
        super().__init__(f"Failed to load {file_path}: {reason}")


class CompileError(ClairError):
    """Clair raises this error when it cannot compile the SQL."""


class RunError(ClairError):
    """Clair raises this error for a fatal runner fault, not for a Trouve failure."""
