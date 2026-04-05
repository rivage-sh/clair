---
name: Routing collision UX decision
description: Routing collisions are warnings (not hard errors) — they only occur in dev environments where routing is active
type: project
---

## Routing collisions are loud warnings, not hard errors

**Why:** Collisions are structurally impossible without a routing policy. Routing is a dev-time concept. Blocking a dev run over a collision in a personal dev database is disproportionate — it prevents running unrelated models until the collision is fixed.

**How to apply:**
- Collision detection runs at plan/DAG-resolution time, before any SQL executes.
- Collisions produce a WARNING; the run proceeds.
- Warning is printed prominently at the top of CLI output.
- Warning names both colliding Trouves, the resolved target, the responsible policy, and the active environment.
- Report ALL collisions in one pass.
- Self-routing (Trouve resolves to its own original location) is NOT a collision.
- `--strict` flag for CI pipelines (hard errors) is deferred to v2.
