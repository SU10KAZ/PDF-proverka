"""PDF Evidence V2 — the deterministic schematic topology graph.

Research only.  No model calls, no deploy, no shadow, no materialization into
any production or pair directory, no production module changed.

V1 built a safe layer of *printed strings* with geometry and proved that the
next bottleneck is not textual: ``upstream`` and ``downstream`` are printed
literally 1 time in 1 074 and 16 times in 1 945.  A wire is not a word.  The
sheet states connectivity by drawing it, and the drawing was never read.

This package asks whether the drawing can be read deterministically.  It
inherits V1's asymmetric contract unchanged — a producer may assert what the
sheet shows and may never assert what it does not — and adds one rule of its
own, which the guards enforce structurally:

    **an intersection is not a connection.**

Two strokes crossing on a sheet are, by default, two strokes crossing.  A
connection exists when the drawing says so: a junction dot, a continuation, a
tee.  Where the drawing says the opposite — the crossover hop, a semicircle
drawn precisely so a reader does not mistake a crossing for a node — that is
recorded as proof of non-connection, not as a gap.
"""
