# ProjectChange F2 evidence binding V4

Mechanical replay of exactly the 12 saved V3 responses, object 272, known DEV
pairs 5 and 7. No model/provider calls, fresh sample, reserve access or deployment.
The task's explicit V4 branch request overrides the ordinary main-only convention.

## Root cause

`EvidenceRequirement` → V3 delivery aliases → semantic packet → raw model
`subject_identity.evidence_ids` → V3 normalizer → F2 comparability/admission.
Delivery preserves the supporting source. V3 normalization copies only the flat
state `evidence_ids` into `EngineeringState`, then incorrectly requires the nested
identity IDs to be a subset of those IDs. Identity canonicalization consequently
fails even when its extra source was delivered and explicitly cited.

C01 NEW page 30 is requirement `K01_05` (`COMPLETE`, supporting text), delivered as
`source_c36b16327bd695c275b7377f`. The saved response cites it in NEW subject
identity and supplies a raster locator, but omits it from NEW value references.
V3 emits `new_SUBJECT_MAPPING_NOT_GROUNDED`; OLD becomes a canonical subject while
NEW retains its source label. F2 then reports different engineering subjects and
downgrades raw ACCEPT to REVIEW. The page's legend supports identity; it does not
replace a domestic-water connectivity scheme. The saved declaration remains
1 → 2 zones within one parent functional subject (comparison cardinality 1→1).

## Binding contract

Each reference has an evidence ID, role, side, subject, target claim, raw JSON path
and provenance (raw response hash, packet hash, physical document version, PDF
receipt, page, raster/region and matching requirement receipts when available).
One evidence ID can carry several explicitly supported roles. `state.evidence_ids`
is their union; value and identity IDs also remain separately available.

Bindings come only from explicit structured references in the saved response.
They must resolve to delivered evidence for the corresponding side and physical
version, with a matching engineering subject and source provenance. Package
membership alone never creates a binding. A claim-specific conflict reference
remains attached to its declared target; it does not acquire a state-value role.
Free-text condition/conflict explanations do not create evidence IDs.

Identity validation checks its own delivered references independently from value
references. F2 existence still requires OLD and NEW **value** witnesses. Extra
identity, scope, condition or counter references cannot satisfy that requirement.
Witness validation, F1/F4, profiles, bounded absence, materiality, prompts, packages
and all previous code/artifacts remain unchanged. This is a binding audit of
saved model observations, not an independent semantic reassessment of sources.

V4 artifacts: `/home/coder/auditmanager/corpus-audits/20260914_project_change_272/mechanical_replay_f2_binding_v4`.

## Single replay

```sh
PYTHONDONTWRITEBYTECODE=1 python -m experiments.project_change_f2_binding_v4_272.replay --prepare
PYTHONDONTWRITEBYTECODE=1 python -m experiments.project_change_f2_binding_v4_272.replay --replay
```

Preparation runs synthetic unit tests, verifies all previous V3 file hashes,
frozen code, source-truth hashes and the DEV access receipt, then freezes the V4
code before replay. Replay verifies saved SUCCESS receipts, packets, raw-response
aliases, admitted physical versions/pages and rasters. A Python audit hook blocks
network operations and child processes. A start receipt forbids another pass or
automatic resumption in the same iteration. There is no inference entrypoint.

Regression tests consume persisted replay outputs without invoking normalization
again. V3 raw verdicts and historical F2 results are compared with the V4 result.
All seven REAL cases must ACCEPT, C07/C22 must remain NOT_CHANGE, the other three
controls must remain REVIEW, and every previously correct verdict must survive.
The gate and case IDs appear only in the harness/tests, never in binding logic.
Stop after the replay, reports and commits; no fresh DEV work is authorized.
