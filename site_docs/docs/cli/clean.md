# clair clean

Remove compiled artifacts from `_clairtifacts/`.

```bash
clair clean [--project PATH] [--before AGE] [--dry-run] [--yes]
```

## Example

```bash
# Show what clair will delete (older than 7 days)
clair clean --project . --before 7d --dry-run

# Delete after you confirm
clair clean --project . --before 7d

# Skip confirmation (useful in CI)
clair clean --project . --before 7d --yes

# Delete everything
clair clean --project .
```

## `--before` formats

| Format | Example | Meaning |
|--------|---------|---------|
| Named | `today` | Artifacts from before today (local midnight) |
| Named | `yesterday` | Artifacts from before yesterday |
| Named | `last_week` | Artifacts from before last calendar week (Monday) |
| Duration | `7d` | Older than 7 days |
| Duration | `24h` | Older than 24 hours |
| Duration | `30m` | Older than 30 minutes |
| ISO date | `2026-03-01` | Before this date |
| ISO datetime | `2026-03-01T12:00:00` | Before this datetime |

If you do not give `--before`, clair removes all the artifact runs.

## Dry run

Always use `--dry-run` to see the artifacts before you delete them:

```
Would remove 3 artifact run(s):
  019607ab3e8a7f1c8b2d4e6f0a1b2c3d  (2026-03-22 09:14:03 UTC)
  01960612f3c17f2d9a8e5b3c4d2e1f0a  (2026-03-21 17:30:11 UTC)
  019604e8b21a6e3f7c9d0e1f2a3b4c5d  (2026-03-20 08:55:42 UTC)
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--before` | (none — all runs) | Remove the artifacts that are older than this age or date |
| `--dry-run` | `false` | Show the artifacts, but do not delete them |
| `--yes` | `false` | Skip the confirmation prompt |

## See also

- [clair compile](compile.md)
- [DAG — artifacts](../topics/dag.md#artifacts)
