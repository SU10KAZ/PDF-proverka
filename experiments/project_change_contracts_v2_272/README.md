# Controlled F1/F2 integration V2

This isolated implementation is based on `6c2024a3` and uses precisely the prior
12 known DEV cases of object 272, pairs 5 and 7. The user explicitly requested a
new V2 branch. Prior artifacts are read-only and hash-checked. No deployment,
OpenRouter transport, reserve evaluation, full DEV or third pair is included.

- `allocation.py`: fixed eight-image budget, primary state and graphic counterpart
  reservations for OLD and NEW, counter-evidence next, per-requirement receipts.
- `negative.py`: source-bound absence certificate with inspected OLD scope,
  explicit negative or verifiable complete representation; no-hit proves nothing.
- `sufficiency.py`: claim-specific evidence profiles, including declaration-only
  topology cores, strict aggregate comparability and bounded novelty.
- `normalization.py`: deterministic canonical states, structured conflict relevance,
  engineering phase versus version direction, functional identity versus labels,
  member-derived cardinality, witness checks and provenance for transformations.
- `prepare.py`: rebuilds previous admitted scopes, replays previous raw outputs
  without model calls, never reads source truth.
- `run.py`: freeze and verify code/config/prompt/package/image hashes, then exactly
  12 sequential isolated Codex CLI calls. No semantic retry or tuning after call 1.
- `report.py`: checks all 12 successes before opening frozen source truth; records
  raw and canonical results separately. Normalization never upgrades raw REVIEW.

All artifacts are stored in
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/controlled_inference_f1_f4_f2_v2/`.
The `offline_replay/` results preserve ambiguities in historical free text as
REVIEW. The new output schema explicitly represents those fields. Local contract
PASS is not a claim that old semantic verdicts have been repaired or that a partial
package is complete. The run uses the existing Codex ChatGPT allowance; no
OpenRouter requests are made.

Run offline tests, prepare, commit implementation, then freeze. Inference and
report are separate commands so failed local gates cannot fall through to calls.

```sh
python -m unittest experiments.project_change_contracts_v2_272.test_contracts
PYTHONDONTWRITEBYTECODE=1 python -m experiments.project_change_contracts_v2_272.prepare
PYTHONDONTWRITEBYTECODE=1 python -m experiments.project_change_contracts_v2_272.run --freeze
PYTHONDONTWRITEBYTECODE=1 python -m experiments.project_change_contracts_v2_272.run
PYTHONDONTWRITEBYTECODE=1 python -m experiments.project_change_contracts_v2_272.report
```
