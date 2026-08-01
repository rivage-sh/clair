# clair clean

Remove compiled artifacts from `_clairtifacts/`.

```bash
clair clean [--project PATH] [--before AGE] [--dry-run] [--yes]
```

## Example

```bash
# Preview what would be deleted (older than 7 days)
clair clean --project . --before 7d --dry-run

# Delete after confirming
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

Omitting `--before` targets all artifact runs.

## Dry run

Always preview with `--dry-run` before deleting:

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
| `--before` | (none — all runs) | Remove artifacts older than this age/date |
| `--dry-run` | `false` | Preview without deleting |
| `--yes` | `false` | Skip confirmation prompt |

## See also

- [clair compile](compile.md)
- [DAG concepts — artifacts](../concepts/dag.md#artifacts)
