# ИОС 2.1 functional candidate forensics

Status: complete, offline, artifact-only.

This directory explains why evidence-backed functional continuities for pair `pe336037597` / run `prun_68a933afd5b2bb083189d3c6` were absent from or poorly ranked in the bounded candidate set created around research commit `41d43625`.

## Result in one paragraph

The RIGHT graphic pages are physically reordered (`24/25/26/27/28/29/30` = sheets `3/4/1/2/7/6/8`), while their saved matcher records lost sheet number, title and stamp identity. Literal function/entity/topology overlap is then too weak to overcome irrelevant physically-near pages. The current option composer also cannot create non-contiguous single-function groups. B sheets 7 and 8 continue separated fire-water parts of old combined functions; old A sheet 5 is annulled only as a sheet and continues as the evidence-backed group RIGHT physical `[26,28,29]`.

## Files

- `forensic_report.md` — evidence, real-page map, candidate table, root-cause analysis, sheet-5 decomposition and Recall recommendations;
- `reference_map.json` — Sheet Passports, Function Passports, evidence-backed reference relations and saved authority mappings;
- `candidate_failures.json` — exact top-5/top-10/deep/full-corpus availability, ranks, signals, classifications and counts;
- `v4_candidate_contract.md` — design-only functional candidate contract;
- `README.md` — scope and provenance.

## Provenance

| Source | SHA-256 |
|---|---|
| LEFT original PDF | `12b3ecea9a843fa9fb3cfabbbfed03cc0f3229ef197add90be1eaa4e80a5f757` |
| RIGHT original PDF | `9340ca55017eac3b17c2d99abb83f845da685c75af024e33e77755292ab35a9e` |
| pair.json | `06479b3e7527ae0fb8a572937baa15ae712becad7fcdae4a5d8d60e17d047706` |
| sheet_links.json | `2c39ac60a9a2ffd2f96adb3f79b5bfa11c2ad2b65de0058685fce66ca86de189` |
| frozen production sheet_relations.json | `12512b45d6ca5e340e2ca61db56d42d0d23624ad1f17d73ec29a1e146009b14a` |
| saved research candidate_recall.json | `6c911ae0adcfbc5249029b279ccfa3e72e3b8cb86a5874eb63c1bd74f846a15a` |

Candidate sources are deliberately distinguished:

- “top-5” and “deep current” are the frozen production artifact;
- “top-10” and “deep top-10” are the existing saved read-only deterministic regeneration from commit `41d43625`;
- “full-corpus rank” is a forensic read-only invocation of the same v3 pass-1 scoring primitive over all RIGHT pages. It is not a new algorithm, model call or persisted experiment.

## Safety / isolation

- `production-sheet-matcher.v3` was not edited.
- Production thresholds and source pair artifacts were not edited.
- No model call was made.
- No deployment or push was performed.
- Only files in this directory belong to the forensic commit.

## Machine checks

```bash
python -m json.tool comparison/ai_sheet_matcher/20260902_ios21_forensics/reference_map.json >/dev/null
python -m json.tool comparison/ai_sheet_matcher/20260902_ios21_forensics/candidate_failures.json >/dev/null
git diff --check
```
