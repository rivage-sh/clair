# Selectors

The `clair run`, `clair compile`, `clair test`, and `clair dag` commands accept `--select`. Use it to filter the Trouves, then act on one part of your project only.

## Syntax

`--select` takes a glob pattern. clair matches the pattern against the fully-qualified name of the Trouve (`database.schema.table`). The `*` wildcard matches inside one segment.

```bash
clair run --project=. --env=dev --select='refined.orders.*'
```

## Examples

**A full schema:**

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

## Use more than one selector

Repeat `--select` to add more patterns. clair includes each Trouve that matches one pattern or more:

```bash
clair run --project=. --env=dev \
  --select='source.products.*' \
  --select='derived.products.*'
```

## No matches

If no Trouve matches the selector, clair stops with success:

```
No Trouves selected to run.
```

## Selectors and tests

`clair test --select` keeps SOURCE Trouves in the filter. But the test runner skips them, because SOURCE Trouves do not have tests.

## Which commands accept `--select`

| Command | Accepts `--select` |
|---------|---------------------|
| `clair run` | Yes |
| `clair compile` | Yes |
| `clair test` | Yes |
| `clair dag` | Yes |
| `clair init` | No |
| `clair docs` | No |
| `clair clean` | No |
