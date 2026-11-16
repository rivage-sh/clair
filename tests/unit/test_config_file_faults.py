"""A configuration file that clair cannot use stops discovery.

`__database_config__.py` and `__schema_config__.py` give the role, the
warehouse and the kind of table of each Trouve below them. If clair ignored a
file that raises, each of those Trouves would run with the values of the
profile, and no message would tell you. Thus the file is a fault, in the same
way as a Trouve file that raises.
"""

from __future__ import annotations

import pytest

from clair.core.discovery import discover_project
from clair.exceptions import ProjectDiscoveryError
from tests.helpers import write_project_files

TROUVE_FILE = 'from clair import Table\ntrouve = Table(sql="select 1 as n")\n'


@pytest.mark.parametrize(
    ("config_path", "config_text", "message"),
    [
        pytest.param(
            "db/s/__schema_config__.py",
            'from clair import SchemaDefaults\ndefaults = SchemaDefaults(role="finance")\nmissing_name\n',
            "NameError",
            id="schema_file_raises",
        ),
        pytest.param(
            "db/__database_config__.py",
            "raise RuntimeError('the file stops')\n",
            "RuntimeError",
            id="database_file_raises",
        ),
        pytest.param(
            "db/s/__schema_config__.py",
            'from clair import DatabaseDefaults\ndefaults = DatabaseDefaults(role="finance")\n',
            "SchemaDefaults",
            id="the_class_of_the_other_level",
        ),
        pytest.param(
            "db/s/__schema_config__.py",
            'role = "finance"\n',
            "SchemaDefaults",
            id="no_defaults",
        ),
    ],
)
def test_a_config_file_that_clair_cannot_use_stops_discovery(
    tmp_path, config_path, config_text, message
):
    project_root = write_project_files(
        tmp_path, {"db/s/orders.py": TROUVE_FILE, config_path: config_text}
    )

    with pytest.raises(ProjectDiscoveryError) as raised:
        discover_project(project_root)

    assert config_path in str(raised.value)
    assert message in str(raised.value)


def test_the_schema_file_replaces_the_database_file(tmp_path):
    project_root = write_project_files(
        tmp_path,
        {
            "db/s/orders.py": TROUVE_FILE,
            "db/__database_config__.py": (
                "from clair import DatabaseDefaults\n"
                'defaults = DatabaseDefaults(role="loader", warehouse="load_wh")\n'
            ),
            "db/s/__schema_config__.py": (
                'from clair import SchemaDefaults\ndefaults = SchemaDefaults(role="finance")\n'
            ),
        },
    )

    (orders,) = discover_project(project_root)

    config = orders.require_compiled().config
    assert (config.role, config.warehouse) == ("finance", "load_wh")
