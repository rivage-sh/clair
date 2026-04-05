---
name: Defensive enum branching
description: Use if/elif/else with an explicit raise on exhaustive enum switches, not ternaries
type: feedback
---

Prefer `if/elif/else` over ternary expressions when branching on an enum. The `else` should raise `CompileError` (or the appropriate `ClairError` subclass) with a descriptive message. This catches unhandled enum values early instead of silently doing the wrong thing.

**Why:** Ternaries like `".py" if x == PANDAS else ".sql"` silently default if a new enum value is added. An explicit raise surfaces the gap immediately.

**How to apply:** Any time code branches on `ExecutionType`, `TrouveType`, `RunMode`, or similar enums, use `if/elif/else` with a `raise` in the `else`. Use the most specific `ClairError` subclass for the context (`CompileError`, `RunError`, etc.). Also pre-declare the result variable as `None` before the block — don't leave variables first-assigned inside an indented block. No type annotation needed on the `None` init; the type checker infers `str` after the block because the `else` always raises.
