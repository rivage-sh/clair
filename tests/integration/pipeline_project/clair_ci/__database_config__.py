import os

from clair import DatabaseDefaults

# The CI account gives one small warehouse and one role. Both names come from
# the environment, thus a fork of this repository can use different names.
defaults = DatabaseDefaults(
    warehouse=os.environ.get("CLAIR_CI_SNOWFLAKE_WAREHOUSE", "clair_ci_wh"),
    role=os.environ.get("CLAIR_CI_SNOWFLAKE_ROLE", "clair_ci_role"),
)
