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

Run contract checks with:

```sh
python -m unittest experiments.semantic_foundation_v3.test_foundation experiments.semantic_foundation_v3.test_dev_packet
node experiments/semantic_foundation_v3/test_dev_ui.cjs
```

The `run` module takes `--documents`, a fresh `--output`, and optionally
`--producer v1`. It validates pinned inputs and keeps timings outside semantic
artifacts. The `freeze` module freezes Foundation checks before DEV selection.
DEV preparation uses `dev_packet scan`, followed by `select_dev --source-selection
<expanded_source_selection.json>` and `dev_packet build`. The explicit source
selection manifest records structural cohort expansion; case selection enforces
all quotas, per-document limits and unique page-content pairs. The selector uses
the installed SciPy/HiGHS solver, independently from the stdlib-only Foundation.
The final `closeout` module validates JSON schemas with installed jsonschema,
rederives source predicates and anchors, verifies frozen code and inputs, and
prepares a static isolated DEV UI. It never loads EVAL answers.

The actual first-wave cohort expanded to 21 real documents (11 retained, Mockup
excluded, 10 additional) to meet all 126 case quotas and feasible document-diversity
constraints without duplicated questions. This exceeds the audit's initial 4–6
additional-document estimate; the human budget stays at 104 plus 22 automatic controls.
The separate UI starts with no answers and exports boundary and ownership answers
under different keys. It does not start a service or alter the old annotation tool.
