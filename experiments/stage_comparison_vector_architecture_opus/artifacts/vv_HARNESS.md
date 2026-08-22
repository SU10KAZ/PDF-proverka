# VV — the vision-verification harness

Track B (Opus), research only. Nothing here is production code and nothing outside
`experiments/stage_comparison_vector_architecture_opus/` was written.

The harness exists to answer one question with numbers instead of opinion:

> **Can a multimodal model check a deterministic `VectorBlockDescription` against the raster crop
> of the same block, BEFORE any П↔РД comparison happens?**

To measure that, you need descriptions whose corruption is known exactly. This module builds them,
turns each into a short list of picture-checkable claims, runs the real verifier, and records both
the verdict and the token bill.

| Artefact | What it is |
|---|---|
| `probes/vv_harness.py` | the module: `fact_sheet`, `mutate`, `render_crop`, `verify`, `materialize_case` |
| `probes/vv_build_cases.py` | builds the manifest |
| `probes/vv_smoke.py` | runs `verify` on named cases |
| `artifacts/vv_cases.json` | 60-case manifest (403 kB) |
| `artifacts/vv_smoke.json` | the two real calls run here |
| `artifacts/vv_verify/<case_id>.json` | full per-call record incl. the exact prompt sent |

---

## 1. Reproduction

```bash
cd /home/coder/projects/PDF-proverka

# 1. self-test: rebuild_derived must be a no-op on all 20 Track A blocks,
#    and every fact sheet must fit under 1200 characters
python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_harness selftest

# 2. rebuild the case manifest (deterministic; ~1 min)
python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_build_cases

# 3. run the verifier on two cases (~2.5 min each, 2 in parallel)
python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_smoke
python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_smoke --cases vv021 vv057 --workers 2

# inspect one case without calling any model
python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_harness sheet \
    --pair ss_scheme_text_changed --side left --mutation deleted_object --seed 20260849
```

Measured on this checkout:

* `vv_harness selftest` → **0 failures**, 20/20 blocks.
* `vv_build_cases` → 60 cases, 20 blocks, 0 mutations invisible in their own fact sheet.
* Re-deriving all 60 cases from `(block, mutation, seed, disclose_limits)` reproduces the stored
  fact sheet **60/60**, twice, in 55.2 s and 55.9 s — the manifest carries no state that the code cannot rebuild.

---

## 2. `rebuild_derived` — why the mutations are honest

A mutation that changes geometry or text but leaves `primitive_summary`, `topology`, `anchors`,
`repeated_elements`, `hatch_like_structures`, `dimensions`, `labels`, `structural_signature` and
`size_metrics` stale would be detectable by arithmetic, not by looking. So every such mutation is
followed by `rebuild_derived`, which recomputes the whole derived stack by calling **Track A's own
extractor functions** (`ex._topology`, `ex._anchors`, `ex._repeated_elements`,
`ex._hatch_like_structures`, `ex._summary`, `ex._signatures`, `ex._size_metrics`) with the same
constants (`DEFAULT_TOPOLOGY_CAP = 8000`, per-block `topology.tolerance_norm`).

Proof that this is faithful: run on an **untouched** description it reproduces every one of those
fields byte-for-byte on all 20 Track A blocks (`vv_harness selftest`, 0 failures; comparison is
done after a JSON round-trip because the extractor returns tuples/`Counter` where the stored file
holds lists/dicts).

Two mutations deliberately do **not** go through it, because they model a *reporting* error rather
than a geometry error: `wrong_count` and `wrong_topology` leave the geometry intact and change only
the stated number. Their ground truth carries `not_adjusted`, naming what stays stale
(`topology.components`, `degree_histogram`, `structural_signature.*`).

---

## 3. Fact-sheet format

`fact_sheet(description, *, disclose_limits=True, max_chars=1200) -> dict`

```json
{
  "block_id": "blk_13624dcbe2024b148a2027e1b68e2a0d",
  "schema": "v0.1",
  "disclose_limits": true,
  "claims": [{"id": "C1", "kind": "text_count", "claim": "…", "value": 100}, …],
  "text": "C1. …\nC2. …",
  "characters": 923,
  "dropped_for_length": []
}
```

`text` is what the verifier sees. Measured over the 60 cases: **731–1110 characters, mean 939,
11–14 claims per sheet** — the 1200-character budget is never hit, so no case in this manifest lost
a claim to trimming (`dropped_for_length` is empty everywhere). If a future block did overflow,
claims are dropped in the fixed order `C13, C8, C6, C11, C4, C9, C10, C5, C12, C7, C14, C3, C2, C1`
and the ids that went are listed in `dropped_for_length`.

### The claims (v0.1 schema)

| id | kind | wording | source in the description | why it is checkable from a picture |
|---|---|---|---|---|
| C1 | `text_count` | "The block contains **N** separate text strings." | `primitive_summary.text_items` — deliberately the description's *stated* count, not `len(texts)`, because a miscount lives in exactly that gap | countable in sparse blocks, plausibility-only in dense ones |
| C2 | `text_readable` | "All N text strings are readable words or numbers." / "**B** of the N text strings carry no readable letters (garbled)." | control characters in `texts[].text` | phrased as a claim about the **picture**, so a crop with crisp Cyrillic falsifies "207 are garbled" |
| C3 | `prominent_text` | "The largest lettering includes: …" (≤5 distinct, ≤16 chars each) | `texts` sorted by `font_size` desc | read the big words off the crop |
| C4 | `values` | "Numeric values that appear include: …" (≤6) | `texts` with category `numeric`/`engineering_value`, garbled ones excluded | read the numbers off the crop |
| C5 | `repeat_family` | "The most repeated shape occurs **C** times (type, S strokes each)." | `repeated_elements[0]` | count the repeated symbol |
| C6 | `repeat_families` | "**F** different shapes repeat two or more times." / "No shape … is drawn twice." | `len(repeated_elements)` | count symbol *kinds* |
| C7 | `components` | "The linework forms **C** separate connected networks (nothing joins them)." | `topology.connected_components` | plausible/implausible for the picture |
| C8 | `junctions` | "There are **B** junctions where 3+ lines meet and **K** closed outlines." | `topology.branch_points`, `closed_contours` | closed outlines are countable; junction counts are plausibility-only |
| C9 | `size` | "The drawing holds **P** paths and **S** line segments." (≥5000 → "dense: P paths, about Sk line segments") | `primitive_summary` | order-of-magnitude plausibility |
| C10 | `occupancy` | "These parts of the block hold no linework at all: …" / "Linework reaches every one of the nine parts…" | 3×3 grid over segment midpoints | look at the empty corners |
| C11 | `boundary` | "Content runs into the block edge on the …, so it may be cut off there." / "Nothing touches the block edges." | primitive+text bboxes within 0.004 of the unit square | see whether the crop cuts content |
| C12 | `cap` | "Geometry was truncated at a cap: only the **K** longest of **T** paths were kept." | `geometry.extraction` | only when `disclose_limits=True` |
| C13 | `self_rating` | "The extractor rates its own view of this block: GOOD/LIMITED_CAPPED/…" | `vector_quality` | only when `disclose_limits=True` |
| C14 | `elements_by_third` | "Big separate drawn elements by third of the block — left A, centre B, right C." | `topology.components` with ≥4 segments (the extractor lists the 50 largest) | count the separate groups per column |

**C14 exists for a specific reason.** Without it, deleting one visible element inside a busy block
moves only totals (C1, C7, C9) — numbers no human and no Vision model can check. C14 makes the
deletion land on a claim that is checkable by counting groups in one third of the picture. All six
`deleted_object` cases change C14; none of the 36 mutations is invisible in its own sheet
(`mutations invisible in their own fact sheet: none`).

`disclose_limits=False` removes **only** C12 and C13. It models a pipeline that truncates silently.
It is used for every `capped_geometry` case and for all four real-defect cases; the corresponding
clean control on the same block uses `True`, so the manifest also contains a built-in A/B on
whether self-disclosure changes the verdict.

### v0.2 (`artifacts/v02/<pair>/<side>.json`)

`fact_sheet` also accepts the orchestrator's objects-layer v0.2 schema and emits an analogous set
(C1–C4 from `texts`; C5/C6 from `repeated`; C7 object classes; C8 `linear_objects`; C9
`stroke_count`; C10 bound labels). **`mutate` is v0.1-only** and raises `NotImplementedError` on
v0.2 — the two v0.2 blocks that exist (`eom_singleline_changed`, `ss_scheme_text_changed`) are not
enough to build a balanced mutation set, and v0.2 has no `topology.components` for C14.
UNVERIFIED: no v0.2 case was run through `verify` here.

---

## 4. Mutation semantics

`mutate(description, kind, rng) -> (mutated_description, ground_truth)`

`ground_truth = {"kind": …, "corrupted": bool, "detail": {…}}`. All randomness comes from the
supplied `random.Random`, so `(block, kind, seed)` is a complete address for a case.

| kind | what it changes | internally consistent afterwards? | ground truth records | claims it moved across the 6 blocks |
|---|---|---|---|---|
| `clean` | nothing (control) | yes | — | — |
| `deleted_object` | picks a **topology component** with bbox area 0.4 %–30 % of the block and ≥4 segments (fallback: densest cell of a 4×4 grid), pads it by 0.004, deletes every segment whose midpoint falls inside and every text whose centre falls inside | yes — full `rebuild_derived` | region bbox, `component_id`, `where` (e.g. `middle-right`), segments removed, primitives fully removed, texts removed + first 8 removed strings | C1 C2 C3 C4 C5 C6 C7 C8 C9 C10 C11 **C14** |
| `wrong_count` | `repeated_elements[0].count` moved by `max(2, 40 %)`, `instances` truncated/padded to match; on blocks with no repeated family, falls back to `primitive_summary.text_items` | geometry deliberately still shows the true count | target field, `true_count`, `stated_count`, delta | C1 C2 C5 |
| `missing_labels` | drops 3 text items chosen by **exactly the ordering C3 uses** (largest lettering, letters/digits only, distinct strings), so the sheet's own named strings disappear | yes — full `rebuild_derived` | the 3 dropped strings with font size, bbox and `where` | C1 C2 C3 C4 |
| `wrong_topology` | mode `merged` (components ×0.15, branch points ×1.6) or `split` (components ×3, branch points ×0.4), mirrored into `primitive_summary` and `size_metrics.compact_payload`; forced to move by ≥2 absolute | geometry untouched; `not_adjusted` names the stale fields | mode, before/after, `not_adjusted` | C7 C8 |
| `broken_text` | replaces 60 % of Cyrillic spans with control characters drawn from the alphabet actually measured in `vk_nodes` (`\x04`, `\x10`–`\x1a`, `!#$%&()*+-/`), one stable substitute per source character — i.e. an incomplete `/ToUnicode` CMap, finding **O8**. Blocks with <3 Cyrillic spans (vk_nodes has 0 left) escalate to any still-readable span and record `target_selection: readable_spans_fallback` | yes — categories recomputed with the extractor's own regexes, then `rebuild_derived` | mimic note, selection mode, count broken, before/after samples, glyph-table size | C2 C3 C4 |
| `capped_geometry` | reproduces `_extract_primitives`' longest-first cap — same sort key `(type not in {line,polyline}, closed, length_norm)` desc — but sized to a **segment** budget of 15 %, sets `storage_capped=True`, and the sheet does **not** disclose it | yes — full `rebuild_derived` | mimic note, primitives/segments before and after, `segment_fraction_kept` | C5 C6 C7 C8 C9 C10 C11 C14 |

Every case carries `changed_claims` (the claim ids the mutation actually moved) and the full
`claim_delta` with before/after text. `must_name` lists what a correct verifier should surface, for
recall scoring.

**One weak case, flagged as such.** `vv038` (`ss_table_graphic` + `capped_geometry`) keeps 89 % of
segments: that block has 11 paths and its single largest path alone holds 1437 of 1609 segments, so
a primitive-level cap cannot bite. It carries `"strength": "weak"`; every other case is `"normal"`.

---

## 5. `render_crop`

```python
render_crop(pdf, page_index, bbox_norm, out_png, zoom=1.35) -> Path
crop_for(pair_id, side) -> Path   # reuses Track A's diagnostics PNG
```

Same recipe as Track A: `page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=block_rect, alpha=False)`.

Verified: re-rendering `eom_singleline_changed/left` produces a file **byte-identical** to
`experiments/stage_comparison_vector_blocks/artifacts/diagnostics/eom_singleline_changed/left.png`
(661×1041, 88 859 bytes both). The manifest therefore points every case at the existing Track A PNG
rather than re-rendering — 20 crops, 7.85 MB total, from 661×1041 (`eom` left) to 4506×2498
(`ss_plan_dense`). Note for the arms: the largest crops will be downscaled by the reader before the
model sees them; that limits what can be checked on `ss_plan_dense` and `ar_plan` and should be
reported as a constraint, not treated as a model failure.

---

## 6. `verify` — the exact call

```python
verify(crop_png, fact_sheet, out_json=None, *, timeout=300, retries=1, model=None) -> dict
```

Mechanics: a fresh temp dir per call, the crop copied in as `crop.png`, then

```bash
cd <tmpdir>
claude -p "<prompt>" --allowed-tools Read --output-format json < /dev/null
```

`stdin` is `/dev/null`, the subprocess has a timeout, and a non-zero exit or an unparseable answer
triggers one retry (`retries=1`). Every attempt is kept in `attempts[]`. The answer JSON is pulled
out of `result` robustly: ```` ```json ```` fences first, then the whole string, then the first
`{...}` span; a candidate counts only if it parses and contains `status`.

### The prompt, verbatim

`{png}` is `crop.png` and `{sheet}` is the fact sheet's `text`.

```text
You are verifying a machine-generated description of ONE block cut out of an engineering drawing (Russian design documentation). Read the image file ./{png} with the Read tool. It is the raster rendering of exactly that block.

Below is a FACT SHEET: a short list of claims a program derived from the PDF's vector layer, without looking at the picture. Your only job is to check those claims against the picture.

FACT SHEET
{sheet}

RULES — follow them exactly:
- Do NOT re-describe the drawing. Do not produce your own inventory of what is in it.
- Do NOT invent coordinates and do NOT invent exact numbers. If a claim states a count you cannot count in the picture (hundreds of items, sub-millimetre strokes), do not guess a number — judge only whether the stated number is plausible for what you see, and say so.
- You may confirm a claim, reject a claim, or report that something plainly visible in the picture is absent from the fact sheet. Nothing else.
- "suspicious" is for a claim that contradicts the picture or is implausible for it. "missing" is for something clearly visible in the picture that no claim covers (name it in words, e.g. "a symbol in the lower-left corner", "the label QF3").
- Judge each claim by its id (C1, C2, ...).

Status rule:
- VERIFIED — every claim you could check holds, and nothing plainly visible is unaccounted for.
- PARTIAL — the claims that hold are usable, but a named gap exists (something missing, or one claim you cannot confirm).
- FAILED — at least one claim contradicts the picture, so the description is not a safe basis for comparing two versions of this drawing.

Answer with a single JSON object and nothing else:
{"status": "VERIFIED"|"PARTIAL"|"FAILED", "verified": ["C1", ...], "missing": ["short phrase", ...], "suspicious": [{"claim_id": "C4", "why": "short phrase"}, ...], "confidence": "high"|"medium"|"low"}
```

The prompt actually sent is stored in every `artifacts/vv_verify/<case_id>.json` under `prompt`, so
no reconstruction is needed to audit a run.

### Token accounting

`verify` records `usage_raw` exactly as the CLI reports it and, separately,

```
usage_payload_attributable = input_tokens + cache_creation_input_tokens + output_tokens
```

`cache_read_input_tokens` is **excluded** because it is dominated by the Claude Code system prompt
(~50 k on a single-turn call; a control call on a 12 kB PNG with a one-line prompt reported
`cache_read = 51 752` against `cache_creation = 8 556`). Reporting the raw total as "the cost of
verification" would overstate it by roughly an order of magnitude. Both numbers are in the output;
the note travels with them in the `usage_note` field.

---

## 7. The case manifest — `artifacts/vv_cases.json`

**60 cases / 20 blocks / 403 kB.**

| family | n | share |
|---|---:|---:|
| `control` (clean) | 20 | **33.3 %** |
| `mutation` (6 kinds × 6 blocks) | 36 | 60.0 % |
| `real_defect` (not synthetic) | 4 | 6.7 % |

Clean controls cover **all 20 Track A blocks** — both sides of all 10 pairs — so the false-alarm
rate is measured on the full range of density and text quality, not only on the mutated blocks.

### The 6 mutation blocks

| block | paths | segments | texts | garbled | components | class | quality |
|---|---:|---:|---:|---:|---:|---|---|
| `ss_simple_node:left` | 2 | 45 | 31 | 0 | 7 | sparse / readable | GOOD |
| `ss_scheme_text_changed:left` | 39 | 710 | 100 | 0 | 407 | sparse / readable | GOOD |
| `ss_table_graphic:left` | 11 | 1 609 | 54 | 0 | 6 | medium / readable | GOOD |
| `eom_singleline_changed:left` | 704 | 3 297 | 73 | 0 | 303 | medium / readable | GOOD |
| `ar_wall_sections:left` | 20 000 | 20 324 | 119 | 0 | 40 | dense / readable | LIMITED_CAPPED |
| `vk_nodes:left` | 20 000 | 20 086 | 421 | **207** | 107 | dense / **broken** | LIMITED_CAPPED |

Six blocks, every mutation kind on all six — sparse to dense, readable to broken, uncapped to
capped. `eom_singleline_changed:right` appears as a clean control only; adding it as a seventh
mutation block would have pushed the set to 66 cases and the clean share below one third.

### The 4 real cases (`synthetic: false`)

All four present the fact sheet **without** self-disclosure (`disclose_limits: false`), which is how
a pipeline that does not confess its own limits would present it.

| case | block | measured here | expected | why |
|---|---|---|---|---|
| `vv057` `real_vk_nodes_capped_and_broken_text` | `vk_nodes:left` | 20 000 of **164 738** paths kept (12.1 %); 207 of 421 texts garbled (49.2 %) | FAILED (PARTIAL ok) | genuinely truncated and genuinely half-unreadable, and the sheet says neither. O11 measures the comparator seeing 8.5 % of this block with an uncapped extractor |
| `vv058` `real_ss_table_graphic_crop_cuts_first_row` | `ss_table_graphic:left` | content touches **all four** block edges | PARTIAL (FAILED ok) | the bbox cuts the first row of the specification table — the defect that made Track A's Vision run assert "position 1 was added" |
| `vv059` `real_eom_left_microsegment_explosion` | `eom_singleline_changed:left` | 59.6 % of segments shorter than 0.001 of the block | PARTIAL (FAILED / VERIFIED ok) | the stated 3 297 segments are an exporter property (O12), not a drawing property |
| `vv060` `real_ss_plan_dense_downstream_blindness` | `ss_plan_dense:left` | 88 752 segments, comparator cap 12 000 → **13.5 %** | **VERIFIED** | scope control: the sheet is accurate, the loss is downstream. Verification of a description cannot catch a cap applied after it — and that is the finding |

Two measurement caveats are written into the manifest itself (`ground_truth.detail.measured._notes`)
so no arm mistakes them for contradictions of the orchestrator's findings:

* `share_of_segments_the_comparator_would_see` is the cap divided by the segments this description
  **retained**. On a storage-capped block the extractor already dropped paths first, so the share of
  the *block* is smaller — O11 measures 8.5 % for `vk_nodes` with an uncapped independent extractor.
* `tiny_segment_share` is measured on this v0.1 description's **anisotropically** normalised
  coordinates (59.6 % for `eom` left). O12 reports 75.3 % from an independent isotropic extractor.
  Two measurements of the same defect, not a disagreement.

### Case record

```json
{
  "case_id": "vv027", "block": "ss_scheme_text_changed:left",
  "pair_id": "…", "side": "left", "crop_png": "experiments/…/diagnostics/…/left.png",
  "mutation": "deleted_object", "family": "mutation", "synthetic": true,
  "seed": 20260849, "disclose_limits": true,
  "fact_sheet": {"text": "…", "characters": 919, "claims": [...]},
  "ground_truth": {"kind": "deleted_object", "corrupted": true, "detail": {...}},
  "changed_claims": ["C1","C14","C2","C4","C5","C7","C8","C9"],
  "claim_delta": [{"claim_id": "C1", "before": "…", "after": "…"}, …],
  "detectable_in_fact_sheet": true,
  "expected_status": "FAILED", "acceptable_status": ["FAILED","PARTIAL"],
  "must_name": ["middle-right", "ОСПД", "6.1"],
  "strength": "normal", "notes": "…"
}
```

Suggested scoring for the arms, given these fields:

* **false-alarm rate** = share of the 20 `control` cases whose status ≠ `VERIFIED`;
* **detection rate** = share of the 36 `mutation` cases whose status ∈ `acceptable_status`;
* **evidence precision** = share of detections whose `suspicious[].claim_id` intersects
  `changed_claims`, or whose `missing[]` names something in `must_name`. Report this separately —
  the smoke test below shows the two can come apart badly.

---

## 8. Smoke test — two real calls

Run: `python -m …probes.vv_smoke` (cases `vv009` and `vv027`, both `ss_scheme_text_changed:left`,
same 62 416-byte 2183×584 crop, sheets 923 and 919 characters). Model `claude-opus-5[1m]` via the
Claude Code subscription CLI. Raw records: `artifacts/vv_verify/vv009.json`, `…/vv027.json`.

| | vv009 — **clean control** | vv027 — **deleted_object** |
|---|---|---|
| expected | VERIFIED | FAILED |
| **returned** | **PARTIAL** ✗ (false alarm) | **FAILED** ✓ |
| confidence | medium | medium |
| suspicious | `C6` | `C6`, `C5`, `C8`, `C11` |
| missing | — | "repeated crossed-square switch symbols (ОСПД blocks, about five of them) not covered by any shape claim" |
| ground-truth changed claims | — | C1 C2 C4 C5 C7 C8 C9 **C14** |
| raw usage | in 10 / cache_creation 17 893 / cache_read 162 924 / out 11 122 (10 188 thinking) | in 4 / cache_creation 10 771 / cache_read 52 759 / out 9 881 (9 424 thinking) |
| **payload-attributable** | **29 025** | **20 656** |
| duration | 155.3 s | 132.0 s |

Verbatim answers:

```json
{"status": "PARTIAL", "verified": ["C1","C2","C3","C4","C5","C7","C8","C9","C10","C11","C13","C14"],
 "missing": [],
 "suspicious": [{"claim_id": "C6", "why": "more than one symbol type visibly repeats: besides the 15 camera symbols, the square-with-cross ОСПД symbol appears 5 times and the small PoE label box 3 times"}],
 "confidence": "medium"}
```

```json
{"status": "FAILED", "verified": ["C1","C2","C3","C4","C7","C9","C10","C13"],
 "missing": ["repeated crossed-square switch symbols (ОСПД blocks, about five of them) not covered by any shape claim"],
 "suspicious": [{"claim_id": "C6", "why": "picture shows at least two clearly distinct repeating symbols (detector/camera symbol many times, crossed-square switch symbol several times, plus repeated PoE label boxes), not just one repeating shape"},
                {"claim_id": "C5", "why": "the obvious repeated symbol appears about fifteen times, more than the stated top repeat count"},
                {"claim_id": "C8", "why": "drawing is full of closed rectangular boxes, so '1 closed outline' does not match; the 3-line junction count also looks high for this simple box-and-wire layout"},
                {"claim_id": "C11", "why": "drawing looks complete on all sides — closed dashed frame, whole boxes and full text at top, left and bottom; nothing appears truncated"}],
 "confidence": "medium"}
```

Three things the arms must carry forward from n=2 (all of them hypotheses to be measured on the
full set, not conclusions):

1. **The clean control failed.** And the model was *right about the drawing*: C6 says one shape
   family repeats, the picture plainly shows three. That is finding **O5** — `repeated_elements`
   fingerprints paths, not symbols — surfacing as a verification failure. So a false alarm against
   this manifest is not necessarily a model error; it can be the extractor being caught. The arms
   must report false alarms **split by claim id**, or C5/C6 will poison the false-alarm rate on
   every block that has real repeated symbols.
2. **Right verdict, wrong evidence.** vv027 returned FAILED but named C5/C6/C8/C11, and its C11
   objection ("nothing appears truncated") is itself wrong — content does touch three edges. The
   deletion (`middle-right`, the ОСПД 6.1 switch, 46 segments, 2 texts) was not named; C14, the claim
   built specifically to carry deletions, was not challenged. Verdict accuracy and evidence accuracy
   must be scored separately.
3. **Budget.** 132–155 s and ~21–29 k payload-attributable tokens per verification (~10 k of it
   thinking), against a raw reported total of 33–191 k that is mostly system-prompt cache reads.
   For 60 cases at concurrency 6 that is roughly 25–30 minutes of wall time per arm.

---

## 9. Limits — stated, not hidden

* `mutate` is **v0.1-only**; v0.2 gets a fact sheet but no mutations (`NotImplementedError`).
* `wrong_count` and `wrong_topology` leave the geometry consistent with the *true* value on purpose;
  their `not_adjusted` lists which fields stay stale. A verifier could in principle catch them by
  cross-checking `topology.components` against `connected_components` — it never sees that, only the
  sheet, so this does not leak.
* `vv038` is a weak `capped_geometry` case (89 % of segments survive); flagged in the manifest.
* One mutation per kind per block, one seed each. Seed sensitivity is **UNVERIFIED** — re-running
  `vv_build_cases` with a different `SEED` produces a different, equally valid set.
* Verifier determinism on identical input is **UNVERIFIED**: no case was run twice here.
* The two smoke calls are n=2. Everything in §8 beyond the recorded numbers is a hypothesis.
* Nothing in this harness measures whether verification *improves the final П↔РД comparison*. It
  measures only whether a description's claims survive contact with the picture. `vv060` is the
  deliberate reminder that a description can pass verification and still be a bad basis for
  comparison, because the loss happens downstream.
