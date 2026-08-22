# HYBRID — Vector vs Vision division of labour, and the AI payload budget

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|---|---|---|---|
| H1 | Track A's vector arm cost more than Vision because **bytes are not the billing unit**: its Cyrillic minified JSON tokenizes at 2.73 bytes/token while PNG crops bill at 158.9 bytes/token — images are 58.2× more byte-efficient here. | `hybrid_token_budget_reconciliation.json`: vector prompt 144,669 B → 52,983 tok; 10 crops 3,095,180 B → 19,479 tok | high | tokenize the prompt with the provider's real tokenizer (not o200k_base) and get a materially different count |
| H2 | 75.0 % of the vector arm's 70,631-token bill is the prompt itself; reasoning is only 2,285 tokens (3.2 %). For Vision, images are 51.2 % and the codex harness prompt alone is 36.5 %. | same file; harness baseline **13,892 tok measured** over 3 zero-image calls (13,985/13,892/13,892) | high | show `tokens used` in codex excludes cached input, which would change the residual |
| H3 | Half the vector prompt is one field: **`L3.texts` = 26,471 tok (49.7 %)**. The next four are unmatched-text lists (5,264), value_changes (4,508), sha256 signatures (4,038) and pattern fingerprints (2,535). | `hybrid_prompt_composition.json` | high | a different bucketing of the same JSON that moves >10 % between buckets |
| H4 | **39.9 % of the vector payload (21,251 of 53,299 tok) is provably non-informative**: 13,232 tok of text the extractor itself flags UNDECODABLE, 6,105 tok of sha256/pattern ids the model cannot cross-reference, 1,914 tok of method telemetry. On `vk_nodes` alone the waste is 60.7 %. | `hybrid_wasted_tokens.json` | high | show the model demonstrably used a hash or a pattern id in its answer |
| H5 | A minimal payload carrying only *what changed / where / before→after / structural context / uncertainty* costs **3,940 tok for the same 5 pairs vs 53,299 — a 13.5× reduction**, and 127×/143× on the two pairs where nothing meaningful changed. | `hybrid_minimal_payload_sizes.json`; per pair ar_plan 15,035→118, vk_nodes 23,199→162, ss_table 2,961→273, ss_scheme 3,953→604, eom 8,151→2,783 | high | show the minimal payload loses a change the fuller payload catches (see H7 for the one case where it does) |
| H6 | Run end-to-end on the same model, schema and pairs, the minimal payload costs **20,483 tokens against 70,631 (vector) and 38,069 (Vision)** — 3.45× and 1.86× cheaper — at the same coarse accuracy (3/5 exact vs 4/5 and 3/5). | `hybrid_minimal_ai_run_base.json`, `hybrid_arm_comparison.json` | medium (n=5, single sample) | repeat the run; classification is unstable (H8) |
| H7 | The **page** (not the block) settles the "new element vs moved crop" question deterministically. On `ss_table_graphic` 4 of 16 change events — the whole «1 Монтажная коробка RVi 2BM» row Track A's vector arm reported as a design change — are provably crop artefacts: the row sits at page y 77.6–88.7, above the left crop's top edge (100.5) and intersecting the right crop's (86.1). Zero false attributions on the 7 quiet pairs. | `hybrid_crop_attribution.json`; block bbox Δ = (+2.38, −14.48, −15.26, +14.59) pt | high | find a pair where the predicate attributes a genuine design change to the crop |
| H8 | Feeding that page-context evidence to the model flips `ss_table_graphic` from a false positive to the correct **NEAR_IDENTICAL with zero major_changes** — the only arm of four with no false claim on the quiet pairs. It simultaneously over-called `ss_scheme_text_changed` on a ~5-token payload difference, i.e. **single-sample classification is unstable**. | `hybrid_arm_comparison.json`: false major_changes on quiet pairs — minimal_pagectx 0, vision 0, trackA_vector 1, minimal_base 2 | medium | run each arm 5× and show the flip is not noise |
| H9 | `repeated_elements` **cannot** carry «Количество аппаратов 12 → 14». Pattern-id Jaccard is **0.000 on `eom_singleline_changed`** — the one pair with a real structural change (22 left ids, 28 right ids, 0 shared) — while on `ar_plan`, which compares **the same PDF bytes** (O1), 27 shared motifs have different counts, max delta 16 (4 → 20). Across the 7 quiet pairs it generates **169 false "count changed" statements**. | `hybrid_pattern_stability.json`, `hybrid_designation_counts.json` | high | show the pattern ids are stable across two PDF producers for the same symbol |
| H10 | A **text-object layer** does carry that sentence. Counting letter-prefix designations over merged text *lines* yields exactly `Wh 0→4, QD 0→4, QF 0→4, ЩМкв 0→4` on `eom_singleline_changed` and **0 false statements on all 7 quiet pairs**, including the two same-bytes AR pairs. | `hybrid_designation_counts.json` (S3 column) | high | find a discipline where designations are not letter+index and the rule silently drops real objects |
| H11 | Merging spans into text lines — the cheapest possible object layer — collapses `eom_singleline_changed` from **328 span-level change events to 101 (3.25×)** and turns fragments into readable strings («ПуГПнг ( А )-HF 5 х (1 х 6) мм ²», «Принципиальная схема этажного щита тип 1 …»). | `hybrid_object_layer_gain.json` | high | show the merged lines are wrong (bad line grouping) on a rotated/dense block; it already adds 3 spurious events on `ss_plan_dense` |
| H12 | Even so, the minimal payload plateaus at **2.9× reduction on the one pair with a real structural change** (8,151 → 2,783 tok, 32 change clusters), because a span/line diff has no notion that C11/C15/C25… are *the same repeated object*. That plateau is the architectural boundary between backbone and object layer. | `hybrid_minimal_payload_sizes.json`, `hybrid_minimal_payloads.json` | high | a grouping rule over the same data that gets the same pair under ~1,000 tok without losing content |
| H13 | The deterministic layer already performs low-level matching **badly** and ships the guesses as facts: `value_changes` pairs by nearest position + same category only, producing 76/93 pairings over undecodable text on `vk_plan`, 78/100 on `vk_node_plan`, and 66/100 pairings between wildly dissimilar strings on `vk_nodes` (`13!`→`!`), 12/20 on `eom` (`L1`→`выключатель`). | `hybrid_boundary_evidence.json`; comparator.py `_text_diff` position threshold 0.04 | high | show the mispairings do not reach the report |
| H14 | Asking any model "which line corresponds to which" is not a policy question but a scale question: the assignment space is **10^9.85 on `ss_plan_dense`** (84,439 × 84,298 segments) and ≥10^8.5 on four more pairs. | `hybrid_boundary_evidence.json` | high | n/a — arithmetic |
| H15 | Vision cost is **measurable and small**: `image_tokens = min(1.2014·⌈w/32⌉·⌈h/32⌉ + 48.67, 3051)`, fitted on 4 synthetic sizes and validated on 11 real images with max residual **4.3 tokens** below the ceiling. A tight zoom-2 crop of a real annotation block = **1,538 tok**; a 400×160 px value crop = **127 tok**; no single image ever exceeded **3,051 tok**. | `hybrid_image_token_curve.json`, `hybrid_vision_crop_cost.json`, `hybrid_image_token_formula.json` | high (measured); the provider's *published* formula is **UNVERIFIED** | measure on another account/region and get a different slope |
| H16 | Above ≈2,500 patches the cost saturates, so **zoom is the budget lever, not crop area**: `ss_plan_dense` at 4506×2498 costs 2,809 tok while a 966×1276 tight crop costs 1,538 — 9.6× the pixels for 1.8× the price. A 3770×943 crop costs only 1,279 tok because the long side is clamped. | `hybrid_vision_crop_cost.json` | high | show a >3,051-token single image |
| H17 | On Track A's benchmark only **4 of 10 pairs** fire any Vision trigger under a deterministic gate set (T1 undecodable ×3, T3 unresolved truncation ×4, T4 encoding rewrite ×1); 6 pairs need no model image at all. | `hybrid_trigger_budget.json` | medium (thresholds are mine, not validated on a larger corpus) | a corpus where the triggers fire on most pairs, making the hybrid no cheaper than Vision-always |


## Environment

Probe prefix `hybrid_`. All commands run from `/home/coder/projects/PDF-proverka`.
`tiktoken` is not installed in the repo interpreter; probes that tokenize were run with a
throw-away venv (`python -m venv --system-site-packages <dir>/tokvenv && <dir>/tokvenv/bin/pip install tiktoken`).
Model calls used the same `codex` binary and model Track A used
(`/home/coder/.vscode-server/extensions/openai.chatgpt-26.818.41705-linux-x64/bin/linux-x86_64/codex`,
`codex-cli 0.149.0-alpha.4.1`, `gpt-5.6-sol`).

Reproduction:

```bash
V=<scratch>/tokvenv/bin/python           # venv with tiktoken
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p1_prompt_composition
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p2_minimal_payload
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p3_object_layer_gain
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p4_wasted_tokens
python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5_vision_crop_cost --render
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5_vision_crop_cost --measure   # ~11 model calls
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5b_image_token_curve            # ~8 model calls
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p6_designation_counts
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p7_boundary_evidence
python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p8_crop_attribution
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p9_budget_reconciliation
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p10_minimal_ai_run                # 1 model call
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p10_minimal_ai_run --page-context # 1 model call
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p11_trigger_budget
```

---

## 1. Where the 70,631 tokens went

`run_ai_experiment._vector_prompt` builds, per pair, `{left_level_3, right_level_3,
deterministic_diff}`. Rebuilding it today gives 138,885 chars against the archived
137,896: the only difference is the `hatch_candidates` key, added to Level 3 after the AI
run. Bucket shares below are on the rebuilt payload (53,299 tok); the archived prompt
itself tokenizes to **52,983** o200k tokens.

| bucket | tokens | % of pair payload |
|---|---:|---:|
| `L3.texts` — positioned text list, both sides | **26,471** | **49.67** |
| `diff.text.added` + `removed` — unmatched-id lists | 5,264 | 9.88 |
| `diff.text.value_changes` | 4,508 | 8.46 |
| `L3.signatures` — 3 sha256 + degree histogram, both sides | 4,038 | 7.58 |
| `L3.patterns` — repeated-element fingerprints | 2,535 | 4.76 |
| `diff.repeated_patterns` — pattern-id diagnostics | 2,255 | 4.23 |
| `diff.differences` — the human-readable lines | 2,216 | 4.16 |
| `L3.topology` | 1,704 | 3.20 |
| `diff.geometry.tolerance_experiment` — the 4-row tolerance table | 1,001 | 1.88 |
| `L3.summary` | 628 | 1.18 |
| `diff.topology` | 622 | 1.17 |
| `diff.caveats` — identical boilerplate repeated 5× | 465 | 0.87 |
| `L3.hatch_candidates` | 384 | 0.72 |
| `diff.text` scalars + layer_quality | 351 | 0.66 |
| `diff.geometry` scalars | 77 | 0.14 |
| `L3.quality` | 22 | 0.04 |
| `diff.status` | 21 | 0.04 |
| JSON structure not attributed to a bucket | 737 | 1.38 |

Per pair: `vk_nodes` 23,199 · `ar_plan` 15,035 · `eom_singleline_changed` 8,151 ·
`ss_scheme_text_changed` 3,953 · `ss_table_graphic` 2,961. Two pairs — one of them a
comparison of a PDF with itself (O1) — are 72 % of the bill.

**Provably wasted: 21,251 tok = 39.9 %.**

| class | tokens | note |
|---|---:|---|
| W1 undecodable text and every diff entry derived from it | 13,232 | `vk_nodes` alone 13,212 (60.7 % of that pair); the extractor had already labelled both layers UNDECODABLE (suspicious ratio 0.257 / 0.273) |
| W2 opaque identifiers — 3 sha256 per side + `pattern_xxxxxxxx` ids | 6,105 | ids have no cross-side meaning: see H9 |
| W3 method telemetry — tolerance table, caveats ×5, «Число примитивов» line, hatch candidates | 1,914 | O6 and O7 already showed these are noise |

### Why Vision was cheaper despite 3.1 MB of PNG

| | vector arm | vision arm |
|---|---:|---:|
| reported total | 70,631 | 38,069 |
| codex harness prompt (measured) | 13,892 | 13,892 |
| payload | 52,983 (prompt) | 19,479 (10 images) |
| task prompt | — | 192 |
| output | 1,471 | 935 |
| residual = reasoning + overhead | 2,285 | 3,571 |
| payload share of bill | **75.0 %** | 51.2 % |
| bytes per token | **2.73** | **158.9** |

The reported gap (32,562) is almost exactly prompt − images (33,504). Minified Cyrillic
JSON is the worst possible tokenization regime; a raster crop is billed by patch area with
a hard per-image ceiling. "Radically smaller AI input" therefore does **not** mean smaller
on disk — Track A's 145 KB payload was 7.4× *more* expensive than 3.1 MB of PNG.

## 2. The minimal payload

`probes/hybrid_p2_minimal_payload.py` emits, per pair: `context` (6 scalars),
`changes` (spatially clustered events with `at`, `where`, nearest **unchanged** labels as
object context, `values`/`added`/`removed`), `geometry_only_in_{left,right}` (localised
uncovered-segment clusters with bbox and segment count), `crop_window`, `uncertainty`
(typed codes). Two rules do most of the work:

* text the extractor flagged UNDECODABLE is **never** shipped — the block is routed to a
  Vision task instead (VQ-1);
* dense blocks (>6,000 segments) say «геометрия не локализована» instead of shipping
  sampled coverage numbers.

| pair | Track A L3+diff | minimal | reduction | change clusters | uncertainty codes |
|---|---:|---:|---:|---:|---|
| `ss_scheme_text_changed` | 3,953 | 604 | 6.5× | 4 | ENCODING_REWRITE, EXTRA_CONTOUR_ONLY_IN_RIGHT |
| `ss_table_graphic` (nothing meaningful changed) | 2,961 | 273 | 10.9× | 3 | — |
| `ar_plan` (same PDF bytes) | 15,035 | 118 | **127×** | 0 | GEOMETRY_TOO_DENSE_TO_LOCALISE |
| `vk_nodes` | 23,199 | 162 | **143×** | 0 | TEXT_LAYER_UNDECODABLE, GEOMETRY_TOO_DENSE_TO_LOCALISE |
| `eom_singleline_changed` (real structural change) | 8,151 | 2,783 | **2.9×** | 32 | — |
| **total** | **53,299** | **3,940** | **13.5×** | | |
| with page context (H7/H8) | 53,299 | 3,927 | 13.6× | | |
| with the text-line object layer (H11) | 53,299 | 4,434 | 12.0× | | |

*(The archived `hybrid_minimal_ai_run_base.json` was produced from a 3,915-token variant of
the same payload, before the `crop_window` key existed; today's rebuild is 3,940.)*

Order of magnitude: achieved (13.5×), but **not uniformly**. The reduction is 100×+ where
nothing changed and 2.9× where something really changed. The residue is 32 disjoint change
clusters that a span/line diff cannot fuse into «добавлены четыре ветви» — that is the
object+relation layer's job, not the payload format's.

End-to-end, same model/schema/pairs: **20,483 tokens** (input 18,197 incl. the 13,892
harness prompt, output 2,286 of which 1,360 reasoning) versus 70,631 and 38,069.

| arm | tokens | exact classification | false `major_changes` on the two quiet pairs |
|---|---:|---:|---:|
| Track A vector (L3 + diff) | 70,631 | 4/5 | 1 («Добавлена позиция «1 Монтажная коробка RVi 2BM»») |
| Track A Vision (10 crops) | 38,069 | 3/5 | 0 |
| minimal payload | 20,483 | 3/5 | 2 |
| minimal + page context | 21,271 | 3/5 | **0** |

## 4. The boundary

**Must stay deterministic — never given to any model.**

1. *Which primitive corresponds to which.* Scale alone forbids it: 10^9.85 candidate
   assignments on `ss_plan_dense`, ≥10^8.5 on four more pairs. There is no way to audit
   such an answer, and a wrong alignment silently produces a fabricated engineering claim.
2. *Which text span corresponds to which.* Same reason; note the current deterministic
   rule is itself poor (H13) and its output must be **suppressed**, not upgraded to the
   model. On `vk_nodes` it manufactured 66 pairings between dissimilar strings.
3. *Counting.* The number of objects before and after must come from a rule with a
   measured false-positive rate on unchanged documents. Designation counting scores 0/169
   against `repeated_elements`.
4. *Crop-window attribution.* Whether a "new" element is new or newly inside the frame is
   decided by re-querying the page, not by looking at a picture (H7).
5. *Whether the text layer is trustworthy.* `layer_quality` is a rule; the model must never
   be shown text the rule rejected.
6. *Geometric equality / tolerance selection.* Never an opinion.

**May go to Vision** (always closed-form, always one named rectangle):
reading values from an undecodable region; counting repeated units in a marked area;
"new element / cropped element / sheet frame" for a text-free contour;
"any new drawn element here — yes/no" after an encoding-rewrite flag; verbatim transcription
of one rotated or overlapped value. All six are specified in `hybrid_TASK_CONTRACT.md`.

**May go to a text LLM:** turning a *finished* deterministic change record into Russian
prose for the expert, and grouping several records into one topic. It must not be given raw
geometry, raw span lists, or the freedom to add a change that is not in the record.

## 5. Vision cost

Measured with `--json` `turn.completed` usage, image cost = `input_tokens` with the image
minus a 13,892-token zero-image baseline, `gpt-5.6-sol`, reasoning `low`, identical
one-word prompt.

| image | px | raw patches | measured tokens |
|---|---|---:|---:|
| synthetic 320×320 | 320×320 | 100 | 169 |
| synthetic 640×640 | 640×640 | 400 | 529 |
| synthetic 1024×1024 | 1024×1024 | 1,024 | 1,279 |
| synthetic 1280×1280 | 1280×1280 | 1,600 | 1,971 |
| synthetic 2048×2048 | 2048×2048 | 4,096 | 3,051 |
| synthetic 2560×2560 | 2560×2560 | 6,400 | 3,051 |
| `ss_simple_node/left.png` | 709×304 | 230 | 321 |
| `eom_singleline_changed/left.png` | 661×1041 | 693 | 877 |
| `ss_table_graphic/left.png` | 1297×668 | 861 | 1,079 |
| tight `eom_branch_group` z2 | 619×703 | 440 | 577 |
| tight `ss_table_extra_contour` z2 | 1885×472 | 885 | 1,113 |
| tight `vk_nodes_notes_block` z2 | 966×1276 | 1,240 | 1,538 |
| tight `eom_branch_group` z4 | 1238×1405 | 1,716 | 2,108 |
| tight `ss_table_extra_contour` z4 | 3770×943 | 3,540 | 1,279 |
| `ar_plan/left.png` | 1928×2237 | 4,270 | 2,968 |
| tight `vk_nodes_notes_block` z4 | 1931×2552 | 4,880 | 2,991 |
| `ss_plan_dense/left.png` | 4506×2498 | 11,139 | 2,809 |

**Formula (measured, not documented):**

```
image_tokens = min(1.2014 · ⌈w/32⌉ · ⌈h/32⌉ + 48.67 , 3051)
```

Least squares on the four synthetic points ≤1,600 patches; **max |residual| 4.3 tokens**
across all 11 non-saturated images. Source: my own measurement, `hybrid_image_token_formula.json`.

**UNVERIFIED:** the provider's published rule. The 32-px patch grid matches OpenAI's
documented GPT-5-family patch model, but the 1.2014 multiplier, the 48.67 offset and the
3,051-token ceiling are fitted, not published, and I could not confirm them for
`gpt-5.6-sol`. Also UNVERIFIED: the exact downscale above the ceiling. Assuming a long-side
clamp near 2,048 px plus a ~2,500-patch budget reproduces the four saturated measurements
to within 3–69 tokens (and reproduces 3770×943 → 1,279 exactly), but I could not confirm it.

Practical consequence: **zoom, not area, is the lever.** Above ~2,500 patches every image
costs the same, so a full dense plan (11,139 patches) is not meaningfully dearer than a
1,600-patch tight crop — but drop the zoom and a tight crop falls to a few hundred tokens.
A per-value crop is ~127 tokens, i.e. a targeted "read this number" task is essentially free
compared with the 13,212 tokens Track A spent shipping unreadable text for one block.

## 6. What this says about the audit's question

The division of labour is not "vector, with Vision as a fallback". Measured, the two
sources are good at different things and the split is **decidable in advance by a rule**:

| the question | who answers it | measured basis |
|---|---|---|
| did the geometry change at all, and where | vector, always | coverage at 4 tolerances; 0 false localisations on the 7 quiet pairs |
| what does a value say | vector when `layer_quality == GOOD` (7 of 10 pairs), Vision when UNDECODABLE (3 of 10) | suspicious control-char ratio 0.22–0.42 on the VK pairs |
| is a "new" element new or newly inside the frame | **the page**, not Vision and not the block | 4 of 16 `ss_table_graphic` events resolved by page re-query, 0 false attributions |
| how many objects of a kind there are | vector, but only over *text designations*, never over `repeated_elements` | 0 vs 169 false "count changed" statements |
| which primitive matches which | nobody — deterministic or nothing | 10^9.85 assignments on the densest pair |
| how to phrase it for the expert | text LLM, over a finished record | — |

On the A/B/C/D axis this probe lands on **B, with a strong caveat about what "object layer"
must mean.** Three measurements push that way:

* the payload cannot get below ~2,800 tokens on the one pair with a real structural change
  while the same change is *one* engineering sentence (H12);
* Track A's only counting primitive is unusable — Jaccard 0.000 exactly where a real change
  happened, 169 fabricated count statements where nothing happened (H9);
* the cheapest conceivable object layer — merging text spans into lines — already collapses
  that pair 3.25× and yields the correct «Wh 0→4, QD 0→4, QF 0→4, ЩМкв 0→4» with zero false
  positives across the benchmark (H10, H11).

The caveat: the object layer that pays off in these measurements is built on **text and page
context**, not on geometry clustering. The two changes that fixed real errors here were
"merge spans into lines" and "ask the page, not the block". Neither needs a symbol
recogniser. A geometry-side object layer (symbols, branches, contours as objects) is still
required for disciplines whose designations are not text — but it is not what unlocks the
first order of magnitude.
