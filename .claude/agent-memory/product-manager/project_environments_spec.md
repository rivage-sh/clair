---
name: Environments feature
description: Environments replace profiles — routing policies for dev/prod isolation. Full spec at specs/environments.md.
type: project
---

Environments are implemented. Auth and routing live in `~/.clair/environments.yml`. CLI flag is `--env`. Full spec: `specs/environments.md`.

**Key decisions:**
- No backwards compatibility with `profiles.yml` — users re-run `clair init`.
- Two routing policies: `database_override` (swap database) and `schema_isolation` (collapse to `{db}_{schema}_{table}` in a personal schema).
- SOURCEs are never routed — their full_name in SQL always resolves to the original location.
- Routing applies at compile time (`clair compile` also needs `--env`).
- `clair dag` and `clair docs` show logical (unrouted) names.
- Default env name is `"dev"`.
