# Selectors

All clair commands accept `--select` to filter which Trouves to act on. This lets you run, compile, test, or inspect a subset of your project.

## Syntax

`--select` takes a glob pattern matched against the Trouve's fully-qualified name (`database.schema.table`). The `*` wildcard matches within one segment.

```bash
clair run --project=. --env=dev --select='refined.orders.*'
```

## Examples

**Entire schema:**

```bash
clair run --project=. --env=dev --select='source.products.*'
```

**Single Trouve:**

```bash
clair run --project=. --env=dev --select='derived.products.top_reviewed'
```

**Name pattern across all databases and schemas:**

```bash
clair compile --project=. --select='*.*.top_*'
```

**Exact database, any schema, name pattern:**

```bash
clair run --project=. --env=dev --select='refined.*.daily_*'
```

## Unioning multiple selectors

Repeat `--select` to union multiple patterns. Trouves matching any pattern are included:

```bash
clair run --project=. --env=dev \
  --select='source.products.*' \
  --select='derived.products.*'
```

## No matches

If no Trouves match the selector, clair exits cleanly:

```
No Trouves selected to run.
```

## Selectors and tests

`clair test --select` includes SOURCE Trouves in the filter for convenience, but the test runner skips them internally (SOURCEs don't have tests).

## Which commands support `--select`

| Command | Supports `--select` |
|---------|---------------------|
| `clair run` | Yes |
| `clair compile` | Yes |
| `clair test` | Yes |
| `clair dag` | Yes |
| `clair init` | No |
| `clair docs` | No |
| `clair clean` | No |
