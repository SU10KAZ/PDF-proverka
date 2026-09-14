# CODEX V2 architectural DEV diagnostic

Only object 272 / Садовническая 76, stage_1 → stage_2, logical v002.
Artifacts and frozen selection:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/semantic_codex_v2_diagnostic/`.

Read `V2_DIAGNOSTIC_PLAN.md` there before using this route. It contains all six FP,
eight uncertain, twelve stratified controls and four overlapping high-value cores.
V1 files and frozen outputs are not modified. V1 proposed claims are untrusted
diagnostic inputs; V1 gold decisions never enter model requests. This is not a fresh
full V2 candidate and its failure-enriched scores are not DEV precision.

`contracts.py` implements version 1 of ConditionSignature, CounterEvidenceEscalation,
MaterialityGate, RasterLocatorContract and CrossDocumentOwnership. Source proof is
represented by explicit evidence IDs and separate literal/visual witnesses.
Deterministic validation checks contracts/provenance; semantic validity still needs
source audit. `schema.py` describes input/output extraction instructions.

`packages.py` admits sources through the original DEV access guard and explicit
registry, creates new hashes and raster references, checks the direction and
physical versions, and reports V1 page gaps. All source page discovery in this
diagnostic is deliberately identified as manual scaffolding. The counter-search
function ranks within this bounded subject registry and never certifies absence.
This implementation does not establish autonomous scope retrieval over fresh DEV.

`run.py` reuses the existing pinned Codex provider, gpt-6-astra / xhigh / priority,
with fresh ephemeral isolated contexts, maximum two concurrent requests, no retries,
exact successful-call resume and immutable raw/normalized receipts. It has no full
DEV, provider fallback, credit purchase, quota-reset or reserve-access command.
`report.py` keeps unresolved cross-document ownership and source-audit limitations
as NO-GO, even if local model certificates pass all structural gates.

Use the existing V1 third-party dependency directory for jsonschema (code only,
no other corpus data):

```bash
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:.
python -m unittest experiments.project_change_semantic_codex_v2_272.test_contracts
python -m experiments.project_change_semantic_codex_v2_272.packages
python -m experiments.project_change_semantic_codex_v2_272.run freeze
python -m experiments.project_change_semantic_codex_v2_272.run run
python -m experiments.project_change_semantic_codex_v2_272.report
```

Do not execute the run command after rejection. A changed architecture requires a
new version, artifact root and freeze; never repair frozen results in place.
Full DEV requires a passed diagnostic and architecture approval, plus current
allowance sufficient for a wholly fresh candidate. Neither VALIDATION nor FINAL
HOLDOUT is opened by any command in this directory.

## Completed diagnostic outcome

Status: `REJECTED_DIAGNOSTIC_V2`; full DEV is not authorized. The 28 unique attempts
produced 25 valid responses and three timeouts, with no retries. The separately
frozen deterministic R3 replay retained 3/12 TP controls, kept all eight uncertain
cases in REVIEW, and demonstrated semantic REVIEW for 3/6 FP; the three timed-out
FP are not credited as recovered. Only fan-coil configuration reached ACCEPT among
the four high-value cores. Known cross-document under-grouping remains unresolved.

The original R2 outputs stay in the artifact directory above. The final report,
R3 freeze, per-case replay, usage and integrity receipts are in the sibling
`semantic_codex_v2_diagnostic_gate_r3/REPORT.md`. R3 code is in
`experiments/project_change_semantic_codex_v2_gate_r3_272/`; it changes only the
representation of unavailable counter-search levels and makes no model requests.
