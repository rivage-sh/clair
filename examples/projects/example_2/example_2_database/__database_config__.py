from clair import DatabaseDefaults

defaults = DatabaseDefaults(
    warehouse="compute_wh",
    # Each statement of this database carries the label in its query tag.
    query_tags={"team": "analytics"},
    statement_timeout_seconds=3600,
)
