"""The Snowflake helper of the integration tests.

The module does four things:

* It reads the CI connection settings from the environment.
* It writes the deterministic seed tables that the SOURCE Trouves name.
* It makes and drops the schemas of one CI run.
* It drops the schemas that an interrupted run left behind.

The pytest fixtures import this module. The GitHub Actions workflows call it on
the command line, for example::

    uv run python -m tests.integration.ci_snowflake drop-schemas --prefix PR_42

Every write goes to one database, and the name of that database comes from
CLAIR_CI_SNOWFLAKE_DATABASE. The module refuses a schema name that it did not
build, thus a wrong prefix cannot drop the seed data or another run.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from clair.adapters.snowflake import SnowflakeAdapter

# The database directory of tests/integration/pipeline_project gives the logical
# database name. A SOURCE Trouve never routes, thus the seed tables must live in
# a database with this name.
REQUIRED_DATABASE_NAME = "CLAIR_CI"

# The schema that holds the seed tables. Every run reads it and no run writes it
# during a test, thus the runs do not collide.
SEED_SCHEMA_NAME = "SEED"

# The logical schemas that the tests write. The run makes one physical schema for
# each of them, with the prefix in front. SCAFFOLD belongs to the project that
# `clair init` makes in test_init_project_runs.py.
RUN_SCHEMA_NAMES = ("REFINED", "DERIVED", "SCAFFOLD")

# A schema that the cleanup must never drop.
PROTECTED_SCHEMA_NAMES = frozenset({SEED_SCHEMA_NAME, "PUBLIC", "INFORMATION_SCHEMA"})

# A prefix starts with a letter and holds letters, digits and underscores only.
# The cleanup builds a DROP statement from the prefix, thus the pattern must
# stay strict.
PREFIX_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{0,40}$")

# The seed rows. The integration tests import these counts, thus one definition
# gives the data and the expected numbers.
SEED_EVENT_ROW_COUNT = 10
SEED_ORDER_ROW_COUNT = 8
SEED_RECENT_ORDER_ROW_COUNT = 3
SEED_USER_COUNT = 4
SEED_DAILY_EVENT_COUNT_ROW_COUNT = 7

_SEED_EVENTS_SQL = f"""
CREATE OR REPLACE TABLE {REQUIRED_DATABASE_NAME}.{SEED_SCHEMA_NAME}.EVENTS AS
SELECT
    column1::string             AS event_id,
    column2::string             AS user_id,
    column3::string             AS event_type,
    column4::timestamp_ntz      AS occurred_at,
    parse_json(column5)         AS properties
FROM VALUES
    ('e1',  'u1', 'page_view',    '2026-01-01 09:00:00', '{{"page": "/home"}}'),
    ('e2',  'u1', 'page_view',    '2026-01-01 09:05:00', '{{"page": "/pricing"}}'),
    ('e3',  'u1', 'purchase',     '2026-01-01 09:10:00', '{{"amount": 100.0}}'),
    ('e4',  'u2', 'page_view',    '2026-01-02 10:00:00', '{{"page": "/home"}}'),
    ('e5',  'u2', 'purchase',     '2026-01-02 10:15:00', '{{"amount": 50.5}}'),
    ('e6',  'u3', 'page_view',    '2026-01-02 11:00:00', '{{"page": "/docs"}}'),
    ('e7',  'u3', 'page_view',    '2026-01-03 08:00:00', '{{"page": "/docs"}}'),
    ('e8',  'u3', 'button_click', '2026-01-03 08:30:00', '{{"element": "signup"}}'),
    ('e9',  'u4', 'page_view',    '2026-01-03 12:00:00', '{{"page": "/home"}}'),
    ('e10', 'u4', 'form_submit',  '2026-01-04 12:00:00', '{{"form": "contact"}}')
"""

# The created_at of an order is relative to the run time. Three orders stay in
# the 3 day window of the incremental Trouve, and the other orders stay out of
# it. No order sits near the edge of the window, thus the count is stable.
_SEED_ORDERS_SQL = f"""
CREATE OR REPLACE TABLE {REQUIRED_DATABASE_NAME}.{SEED_SCHEMA_NAME}.ORDERS AS
SELECT
    column1::string                                             AS order_id,
    column2::string                                             AS user_id,
    column3::string                                             AS order_status,
    column4::float                                              AS amount,
    dateadd('day', column5::int, current_timestamp())::timestamp_ntz AS created_at
FROM VALUES
    ('o1', 'u1', 'complete', 100.0,  -1),
    ('o2', 'u1', 'complete',  25.0,  -2),
    ('o3', 'u2', 'complete',  50.5,  -2),
    ('o4', 'u2', 'refunded',  10.0, -10),
    ('o5', 'u3', 'complete',  75.0, -20),
    ('o6', 'u3', 'complete',  15.0, -30),
    ('o7', 'u4', 'pending',   60.0, -40),
    ('o8', 'u4', 'complete',  20.0, -50)
"""


class IntegrationConfigError(RuntimeError):
    """The environment does not hold a complete CI configuration."""


@dataclass(frozen=True)
class IntegrationConfig:
    """The Snowflake connection settings of the integration tests."""

    account: str
    user: str
    warehouse: str
    role: str
    database_name: str
    schema_prefix: str
    private_key_path: str | None = None
    private_key_passphrase: str | None = None
    password: str | None = None

    def to_profile(self) -> dict[str, Any]:
        """Give the profile dict that SnowflakeAdapter.connect() reads."""
        profile: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "role": self.role,
        }
        if self.private_key_path:
            profile["private_key_path"] = self.private_key_path
            if self.private_key_passphrase:
                profile["private_key_passphrase"] = self.private_key_passphrase
        elif self.password:
            profile["password"] = self.password
        return profile

    def to_environments_yaml(self, environment_name: str = "ci") -> str:
        """Give the text of an environments.yml file for this configuration."""
        lines = [
            f"{environment_name}:",
            f"  account: {self.account}",
            f"  user: {self.user}",
            f"  warehouse: {self.warehouse}",
            f"  role: {self.role}",
        ]
        if self.private_key_path:
            lines.append(f"  private_key_path: {self.private_key_path}")
            if self.private_key_passphrase:
                lines.append(f"  private_key_passphrase: {self.private_key_passphrase}")
        elif self.password:
            lines.append(f"  password: {self.password}")
        return "\n".join(lines) + "\n"


def normalise_prefix(schema_prefix: str) -> str:
    """Put the prefix in upper case and refuse a prefix with an unusual character.

    Snowflake puts an unquoted identifier in upper case. The cleanup compares the
    names as text, thus the prefix must be in upper case here too.
    """
    normalised = schema_prefix.strip().upper()
    if not PREFIX_PATTERN.match(normalised):
        raise IntegrationConfigError(
            f"The schema prefix '{schema_prefix}' is not valid. A prefix starts with a "
            "letter and holds letters, digits and underscores only."
        )
    if normalised in PROTECTED_SCHEMA_NAMES:
        raise IntegrationConfigError(f"The schema prefix must not be '{normalised}'.")
    return normalised


def load_config(schema_prefix: str | None = None) -> IntegrationConfig:
    """Read the CI configuration from the environment.

    Raises:
        IntegrationConfigError: If a mandatory variable is absent, or if the
            database name is not the name that the pipeline project needs.
    """
    missing = [
        name
        for name in (
            "CLAIR_CI_SNOWFLAKE_ACCOUNT",
            "CLAIR_CI_SNOWFLAKE_USER",
            "CLAIR_CI_SNOWFLAKE_WAREHOUSE",
            "CLAIR_CI_SNOWFLAKE_ROLE",
        )
        if not os.environ.get(name)
    ]
    if missing:
        raise IntegrationConfigError(
            "These environment variables are absent: " + ", ".join(missing)
        )

    private_key_path = os.environ.get("CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PATH")
    password = os.environ.get("CLAIR_CI_SNOWFLAKE_PASSWORD")
    if not private_key_path and not password:
        raise IntegrationConfigError(
            "Give CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PATH, or CLAIR_CI_SNOWFLAKE_PASSWORD."
        )

    database_name = os.environ.get(
        "CLAIR_CI_SNOWFLAKE_DATABASE", REQUIRED_DATABASE_NAME
    ).upper()
    if database_name != REQUIRED_DATABASE_NAME:
        raise IntegrationConfigError(
            f"The integration database must be '{REQUIRED_DATABASE_NAME}'. The database "
            "directory of tests/integration/pipeline_project gives this name, and a "
            "SOURCE Trouve never routes."
        )

    prefix = schema_prefix or os.environ.get("CLAIR_CI_SCHEMA_PREFIX") or _local_prefix()

    return IntegrationConfig(
        account=os.environ["CLAIR_CI_SNOWFLAKE_ACCOUNT"],
        user=os.environ["CLAIR_CI_SNOWFLAKE_USER"],
        warehouse=os.environ["CLAIR_CI_SNOWFLAKE_WAREHOUSE"],
        role=os.environ["CLAIR_CI_SNOWFLAKE_ROLE"],
        database_name=database_name,
        schema_prefix=normalise_prefix(prefix),
        private_key_path=private_key_path,
        private_key_passphrase=os.environ.get(
            "CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        ),
        password=password,
    )


def _local_prefix() -> str:
    """Give a prefix for a run on the machine of a developer."""
    user_name = re.sub(r"[^A-Za-z0-9]", "", os.environ.get("USER", "local"))[:12]
    return f"LOCAL_{user_name or 'dev'}_{os.getpid()}"


def connect(config: IntegrationConfig) -> SnowflakeAdapter:
    """Open a Snowflake connection with the CI credentials."""
    adapter = SnowflakeAdapter()
    adapter.connect(config.to_profile())
    return adapter


def execute(adapter: SnowflakeAdapter, sql: str) -> None:
    """Run one statement and raise if Snowflake refused it.

    SnowflakeAdapter.execute() puts a driver error in the result. A test setup
    step must stop, thus this function raises.
    """
    result = adapter.execute(sql)
    if not result.success:
        raise RuntimeError(f"Snowflake refused this statement:\n{sql}\n\n{result.error}")


def fetch_scalar(adapter: SnowflakeAdapter, sql: str) -> Any:
    """Give the first column of the first row of a query."""
    frame = adapter.fetch_dataframe(f"({sql})")
    if frame.empty:
        return None
    return frame.iloc[0, 0]


def row_count(adapter: SnowflakeAdapter, full_table_name: str) -> int:
    """Give the number of rows of a table or a view."""
    return int(fetch_scalar(adapter, f"SELECT count(*) FROM {full_table_name}") or 0)


def seed_source_tables(adapter: SnowflakeAdapter, config: IntegrationConfig) -> None:
    """Write the seed tables that the SOURCE Trouves name.

    The content is the same on each call, thus two runs at the same time do not
    disturb each other. CREATE OR REPLACE is atomic in Snowflake, so a reader
    always sees a complete table.
    """
    execute(
        adapter,
        f"CREATE SCHEMA IF NOT EXISTS {config.database_name}.{SEED_SCHEMA_NAME}",
    )
    execute(adapter, _SEED_EVENTS_SQL)
    execute(adapter, _SEED_ORDERS_SQL)


def create_run_schemas(adapter: SnowflakeAdapter, config: IntegrationConfig) -> None:
    """Make the schemas of one run.

    clair writes ``CREATE OR REPLACE TABLE database.schema.table``. It does not
    make the schema, thus the test setup makes it here.
    """
    for schema_name in RUN_SCHEMA_NAMES:
        execute(
            adapter,
            "CREATE SCHEMA IF NOT EXISTS "
            f"{config.database_name}.{config.schema_prefix}_{schema_name}",
        )


def drop_run_schemas(adapter: SnowflakeAdapter, config: IntegrationConfig) -> list[str]:
    """Drop every schema of one run and give the names that it dropped."""
    dropped: list[str] = []
    for schema_name in _list_prefixed_schemas(adapter, config):
        execute(adapter, f"DROP SCHEMA IF EXISTS {config.database_name}.{schema_name}")
        dropped.append(schema_name)
    return dropped


def drop_stale_schemas(
    adapter: SnowflakeAdapter, config: IntegrationConfig, max_age_hours: int
) -> list[str]:
    """Drop each run schema that is older than max_age_hours.

    A cancelled workflow leaves its schemas behind. A scheduled job calls this
    function, thus the database does not grow without a limit.
    """
    frame = adapter.fetch_dataframe(
        f"(SELECT schema_name, created FROM {config.database_name}."
        "INFORMATION_SCHEMA.SCHEMATA)"
    )
    limit = datetime.now(UTC) - timedelta(hours=max_age_hours)
    dropped: list[str] = []
    for _, row in frame.iterrows():
        schema_name = str(row["schema_name"]).upper()
        if schema_name in PROTECTED_SCHEMA_NAMES:
            continue
        created = _as_utc(row["created"])
        if created is None or created > limit:
            continue
        execute(adapter, f"DROP SCHEMA IF EXISTS {config.database_name}.{schema_name}")
        dropped.append(schema_name)
    return dropped



def _as_utc(value: Any) -> datetime | None:
    """Make one UTC datetime from a Snowflake timestamp, or give None.

    The connector gives a pandas Timestamp for most columns, and a plain
    datetime for some. The function accepts both.
    """
    if value is None:
        return None
    to_pydatetime = getattr(value, "to_pydatetime", None)
    moment = to_pydatetime() if callable(to_pydatetime) else value
    if not isinstance(moment, datetime):
        return None
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _list_prefixed_schemas(
    adapter: SnowflakeAdapter, config: IntegrationConfig
) -> list[str]:
    """Give the schemas of the run, from INFORMATION_SCHEMA.

    The function reads the names first and compares them in Python. Thus a
    protected schema cannot reach the DROP statement.
    """
    frame = adapter.fetch_dataframe(
        f"(SELECT schema_name FROM {config.database_name}.INFORMATION_SCHEMA.SCHEMATA "
        f"WHERE schema_name LIKE '{config.schema_prefix}\\_%' ESCAPE '\\\\')"
    )
    # SnowflakeAdapter.fetch_dataframe() puts each column name in lower case.
    names = [str(name).upper() for name in frame["schema_name"].tolist()]
    return [
        name
        for name in names
        if name not in PROTECTED_SCHEMA_NAMES
        and name.startswith(f"{config.schema_prefix}_")
    ]


def main(argv: list[str] | None = None) -> int:
    """Run one command of this module from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("seed", "create-schemas", "drop-schemas", "drop-stale"),
    )
    parser.add_argument("--prefix", default=None, help="The schema prefix of the run.")
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=48,
        help="drop-stale drops each run schema that is older than this age.",
    )
    args = parser.parse_args(argv)

    config = load_config(args.prefix)
    adapter = connect(config)
    try:
        if args.command == "seed":
            seed_source_tables(adapter, config)
            print(f"Seeded {config.database_name}.{SEED_SCHEMA_NAME}")
        elif args.command == "create-schemas":
            create_run_schemas(adapter, config)
            print(f"Made the schemas with the prefix {config.schema_prefix}")
        elif args.command == "drop-schemas":
            dropped = drop_run_schemas(adapter, config)
            print(f"Dropped {len(dropped)} schema(s): {', '.join(dropped) or 'none'}")
        else:
            dropped = drop_stale_schemas(adapter, config, args.max_age_hours)
            print(f"Dropped {len(dropped)} stale schema(s): {', '.join(dropped) or 'none'}")
    finally:
        adapter.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
