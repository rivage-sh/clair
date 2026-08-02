# clair — Claude context

## Layout

| Package | Source | Tests |
|---------|--------|-------|
| `clair` | `src/clair/` | `tests/` |

## Tooling: uv

This project uses **uv**. Always use `uv run` — never invoke `.venv/bin/python` or `.venv/bin/pytest` directly.

```bash
uv run clair                              # run the clair CLI
uv sync                                   # install/update all deps
uv run pytest tests/                     # run clair tests
```

If the editable install seems broken: `uv sync --reinstall`

## Worktrees

Features are developed in git worktrees under `.claude/worktrees/<branch-name>/`. Each worktree is an isolated checkout — run all commands from within the worktree directory, not the repo root.

```bash
# Inside a worktree:
uv venv
uv sync
uv run pytest tests/
```

The worktree shares git history with the main repo but has its own `.venv/`. Always `uv sync` after entering a new worktree.

## CLI entrypoints

- `clair = "clair.cli.main:cli"` — installed to `.venv/bin/clair` after `uv sync`

## Coding guidelines

- Use descriptive variable names for all code in `src/clair/`
  - e.g. `trouve` instead of `t`
- Use `database_name` instead of `database`, `schema_name` instead of `schema`, `table_name` instead of `table`
- Address git merge conflicts by pulling main, resolving conflicts, and pushing. Favour simplicity over clean commit history — PRs are squash-merged anyway.

## Backwards compatibility

The major version is 0. While the major version stays 0, clair does not keep backwards compatibility. clair has no users at this time, so the best system design wins against a stable interface.

- Change a public name, a file format, or a function signature when the change makes the system better.
- Do not add a deprecation shim, an alias for an old name, or a migration path.
- Delete the old code path. Do not keep it beside the new one.
- Name each behaviour change in the pull request description.

## Comments: Simplified Technical English

Write all code comments and docstrings in Simplified Technical English (ASD-STE100). STE is a controlled writing standard that makes technical text clear and unambiguous for readers who do not speak English as a first language.

Grammar rules:

- Use active verbs, not passive ones.
- Keep sentences short: 20 words maximum for instructions, 25 words maximum for descriptions.
- Use three nouns in a row maximum.
- Do not use `-ing` verb forms, unless they are part of a technical name.
- Do not use the present perfect tense.

Vocabulary rules:

- Give each word one meaning only.
- Use each word as one part of speech only (a noun or a verb, not both).
- Do not use words with many meanings or unclear actions, such as `check`, `verify`, or `ensure`. Use a specific word instead.
- Technical terms for this project are permitted, such as `trouve`, `database_name`, or `worktree`.

Examples:

```python
# Bad: "This function is used for checking if the schema has been loaded."
# Good: "This function tells you if the loader read the schema."
```
