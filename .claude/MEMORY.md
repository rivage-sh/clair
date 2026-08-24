# clair — memory index

`.claude/memory/` holds the design position and the quality bar for clair. This file is
the index to that directory.

| File | Holds |
|------|-------|
| [`memory/conventions.md`](memory/conventions.md) | Part 1 the seven design invariants; Part 2 docs give orientation and code gives truth; Part 3 the quality bar for code; Part 4 how to write a test; Part 5 why the existing code is not the standard. |

## What belongs in `.claude/memory/`

Only the design position, the quality bar, or a correction that the user gave. Behaviour
lives in `site_docs/docs/`, and `src/` is the final authority when the two disagree.

Do not copy a documentation page into memory. A copy becomes wrong when the source
changes.
