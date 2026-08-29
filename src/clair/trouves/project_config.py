"""The project marker file, ``__clair_project__.py``.

The file marks the root of a clair project. It gives clair three answers:

* **The root.** A CLI command walks up from the working directory to the first
  marker file, in the same way that git finds ``.git``. Thus you run clair from
  any directory of the project, and ``--project`` is only an override.
* **The boundary.** A directory that holds many projects holds no marker file,
  thus clair stops there. Without the marker, clair reads such a directory as
  one project and it builds one DAG from all of them.
* **The import root.** ``package`` names the dotted path of the project root,
  for a project inside a Python package. See ``ProjectConfig.package``.

The file holds Python, in the same way as ``__routing__.py`` and
``__database_config__.py``::

    from clair import ProjectConfig

    project = ProjectConfig()
"""

from __future__ import annotations

import keyword

from pydantic import BaseModel, ConfigDict, field_validator

PROJECT_FILE_NAME = "__clair_project__.py"
"""The name of the marker file at the project root."""


class ProjectConfig(BaseModel):
    """The configuration at the project root. Assign it to ``project``.

    The default configuration is correct for almost every project::

        from clair import ProjectConfig

        project = ProjectConfig()
    """

    model_config = ConfigDict(extra="forbid")

    package: str | None = None
    """The dotted name of the project root, as Python imports it.

    Give this value only when clair cannot find the import root itself.

    Clair loads each Trouve file as a Python module, and your Trouve files
    import each other. The two importers must agree on the name of a module,
    because Python keys ``sys.modules`` by name. Two names for one file give two
    module objects, thus two ``Trouve`` objects, and clair then loses the DAG
    edge between them.

    By default clair takes the name from ``sys.path``: it finds the entry that
    holds the project root, and it makes the module name from that entry. That
    is the name that your own import gives, thus the two importers agree.

    Set ``package`` when your project is inside an installed package that
    ``sys.path`` does not name, for example an editable install that uses an
    import finder. The value is the dotted path of the project root::

        # monorepo/clair_projects/analytics/__clair_project__.py
        project = ProjectConfig(package="clair_projects.analytics")

    Clair then reads ``monorepo/`` as the import root, and it names the Trouve
    files below it ``clair_projects.analytics.source.orders.raw``.
    """

    @field_validator("package")
    @classmethod
    def _validate_package(cls, value: str | None) -> str | None:
        """Refuse a value that Python cannot use as a module name."""
        if value is None:
            return None
        if not value:
            raise ValueError(
                "package is empty. Remove it, or give the dotted name of the "
                "project root, for example 'clair_projects.analytics'."
            )
        for part in value.split("."):
            if not part.isidentifier() or keyword.iskeyword(part):
                raise ValueError(
                    f"package holds '{part}', and Python cannot use that as a "
                    "module name. Each part must be a Python name, for example "
                    "'clair_projects.analytics'."
                )
        return value
