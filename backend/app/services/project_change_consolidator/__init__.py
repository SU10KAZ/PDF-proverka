"""ProjectChange Consolidator V1 — shadow consolidation of a completed V3 result.

Reads a completed, immutable ProjectChange V3 result and writes a SEPARATE,
run-scoped shadow result.  It never writes into a V3 run, never changes the
authoritative Dedupe result.  In production it is the last stage of a pair
run («Сведение дублей», ``stage.py``; ``PROJECTCHANGE_CONSOLIDATION_STAGE=0``
turns it off); offline it runs through ``scripts/consolidator_shadow_frozen.py``.
"""
