# clair — memory index

`.claude/memory/` holds the design position and the quality bar for clair. This file is
the index to that directory.

| File | Holds |
|------|-------|
| [`memory/conventions.md`](memory/conventions.md) | The seven design invariants, and why invariant 3 is a correctness rule; docs give orientation and code gives truth; the quality bar for code, and the rule that an operation gives data while the caller writes the bytes; how to write a test; why the existing code is not the standard. |

## What belongs here

Only the design position, the quality bar, or a correction that the user gave. Behaviour
lives in `site_docs/docs/`, and `src/` is the final authority when the two disagree. Do
not copy a documentation page into memory — a copy becomes wrong when the source changes.
