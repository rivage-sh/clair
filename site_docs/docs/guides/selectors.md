# Selectors

The `clair run`, `clair compile`, `clair test`, and `clair dag` commands accept `--select`. Use it to filter the Trouves, then act on one part of your project only.

## Syntax

`--select` takes a glob pattern. clair matches the pattern against the fully-qualified name of the Trouve (`database.schema.table`). The `*` wildcard matches inside one segment.

```bash
clair run --project=. --env=dev --select='refined.orders.*'
```

## Graph operators

A pattern can also carry the `+` graph operator. The operator tells clair to add the
neighbours of each matched Trouve from the DAG.

| Pattern | clair selects |
|---------|---------------|
| `pattern` | only the Trouves that match the glob |
| `+pattern` | the matches, and each parent upstream, at any distance |
| `pattern+` | the matches, and each child downstream, at any distance |
| `+pattern+` | the matches, and each parent and child, at any distance |
| `N+pattern` | the matches, and the parents to a distance of N levels |
| `pattern+N` | the matches, and the children to a distance of N levels |
| `N+pattern+M` | N levels upstream, and M levels downstream |

**Build a Trouve and all that it depends on:**

```bash
clair run --project=. --env=dev --select='+derived.products.top_reviewed'
```

**Build a Trouve and all that depends on it:**

```bash
clair run --project=. --env=dev --select='refined.orders.clean_orders+'
```

**Build the direct parents only:**

```bash
clair run --project=. --env=dev --select='1+derived.products.top_reviewed'
```

The glob rules of the previous section apply to the part of the pattern between the
operators. So `+refined.orders.*+` selects each Trouve in `refined.orders`, plus every
ancestor and every descendant of those Trouves.

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
