# clair — memory index

Behaviour lives in `site_docs/docs/`, not here. Add a note only for a design rule or a
user correction. See [Docs are the source of truth](memory/project_docs_are_the_source_of_truth.md).

## Project

- [Design invariants](memory/project_design_invariants.md) — seven rules clair never breaks: no Jinja, no config YAML, path is the table name, compile stays offline, eager validation, plain Python for shared SQL, adapter behind an ABC.
- [Docs are the source of truth](memory/project_docs_are_the_source_of_truth.md) — grep `site_docs/docs/` to learn behaviour; update the page in the same PR that changes behaviour.

## Feedback

- [No string output tests](memory/feedback_no_string_output_tests.md) — functions return Pydantic objects; tests assert on fields, never on formatted output.
- [Defensive enum branching](memory/feedback_defensive_enum_branching.md) — branch on enums with if/elif/else and raise a `ClairError` subclass in the `else`.
