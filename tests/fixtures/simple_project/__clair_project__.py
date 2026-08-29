"""Clair project marker -- this file marks the root of the project.

Clair walks up from your working directory to the first file with this name,
in the same way that git finds .git. Thus you run a clair command from any
directory of the project, and --project is only an override.
"""

from clair import ProjectConfig

project = ProjectConfig()
