---
name: PandasTrouve design decisions
description: Key architectural decisions for PandasTrouve feature -- separate class, inputs dict for deps, adapter Protocol, full-refresh only v1
type: project
---

# PandasTrouve Design Decisions

Full spec: `.claude/specs/pandas_trouve_arch.md`

## Core decisions

1. **Separate class, not a Trouve subclass.** PandasTrouve is its own Pydantic model. Avoids weakening Trouve's validators and the Liskov substitution violation of a no-op `build_sql()`. Shared `CompiledAttributes` type is the integration point.

2. **Dependency declaration via `inputs={"name": upstream_trouve}` dict.** Keys become the dict keys in `transform(inputs)`. Discovery extracts deps from `id()` of input values -- same registry mechanism as SQL placeholder tokens, no function execution needed.

3. **Adapter stays pandas-free.** `WarehouseAdapter` ABC unchanged. `fetch_dataframe()` and `write_dataframe()` are concrete methods on `SnowflakeAdapter`. A `DataFrameCapableAdapter` runtime Protocol gates access.

4. **pandas is an optional dependency** (`clair[pandas]`). Lazy import at PandasTrouve construction time.

5. **Full-refresh only for v1.** Incremental pandas semantics are complex and speculative. Extension point for future: optional `incremental_filter` callable.

6. **Compile output:** `.json` manifest instead of `.sql` for PandasTrouve nodes.

**Why:** rule/fact.
**How to apply:** When implementing PandasTrouve, follow spec. When a third node type appears, extract a `BaseTrouve` Protocol.
