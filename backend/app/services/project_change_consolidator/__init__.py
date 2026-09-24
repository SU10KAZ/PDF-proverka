"""ProjectChange Consolidator V1 — shadow consolidation of a completed V3 result.

Reads a completed, immutable ProjectChange V3 result and writes a SEPARATE,
run-scoped shadow result.  It never writes into a V3 run, never changes the
authoritative Dedupe result and is never invoked unless explicitly asked
(offline runner) or enabled by ``PROJECTCHANGE_CONSOLIDATOR_SHADOW=1``.
"""
