# clair — memory index

`.claude/memory/` holds the design rules and the code conventions for clair. This file is
the index to that directory.

| File | Holds |
|------|-------|
| [`memory/conventions.md`](memory/conventions.md) | The seven design invariants; docs give orientation and code gives truth; enum branch style; tests assert on object fields, not on strings. |

## What belongs in `.claude/memory/`

Only a design rule, or a correction that the user gave. Behaviour lives in
`site_docs/docs/`, and `src/` is the final authority when the two disagree.

Do not copy a documentation page into memory. A copy becomes wrong when the source
changes.
