# Clair Project Memory

## Specs
- [specs/environments.md](../specs/environments.md) — Environments feature: routing policies (database_override, schema_isolation), environments.yml, --env flag, no backwards compat with profiles.yml.

## Feedback
- [memory/feedback_no_string_output_tests.md](memory/feedback_no_string_output_tests.md) — Functions must return Pydantic objects; tests assert on fields, not formatted output strings
- [memory/feedback_defensive_enum_branching.md](memory/feedback_defensive_enum_branching.md) — Use if/elif/else with explicit raise (ClairError subclass) when branching on enums, not ternaries
