# Semantic Foundation V3

Offline, deterministic Ledger-First substrate. No production integration, matcher,
model calls, Table V3 continuation rules, or Section V3 tuning.

Foundation starts at `2d65913bc4c30f61ca93ab7eb8975c5cd06ae198` (`origin/main`).
The previous local tip and all ten local commits are preserved by the tag
`research/section-table-v2-frozen-0f4ea8a9`, pointing to
`0f4ea8a98624c147ffff7d0a8975613fbb705da4`. Foundation commits do not descend
from that candidate. No push or deployment is part of this experiment.

`frozen_v1.py` is an unchanged vendored dependency from
`20260909_comparison_unit_classifier_v1/tools/comparison_unit_v1.py`:
SHA-256 `5825abc4cbbb2eb045ac3e1537560ca0a2d0d245a967e3113f523512846f58b3`.
Foundation imports its proven primitives, never calls its materialization pipeline.
The full V1 pipeline is retained only for independent contract and performance checks.

Frozen human answers and individual evaluation details must not be opened during
implementation or DEV selection. The 52-document input manifest is permitted for
source-only traceability, performance, replay, and DEV exclusion checks. Published
independent audit reports are architecture input, not new validation evidence.
The old human truth is diagnostic only; a fresh independent holdout is required
before a future release gate.

Reports and corpus artifacts live outside the checkout at
`/home/coder/auditmanager/corpus-audits/20260911_semantic_foundation_v3/`.
