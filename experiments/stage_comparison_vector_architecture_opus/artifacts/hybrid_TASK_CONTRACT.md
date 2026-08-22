# HYBRID — targeted Vision task contract

Research artefact. Every trigger below is a **deterministic predicate over Track A's own
output**; every crop cost is computed with the token formula measured in
`hybrid_image_token_formula.json`:

```
image_tokens = min(1.2014 · ⌈w/32⌉·⌈h/32⌉ + 48.67, 3051)
```

fitted on 4 synthetic images and validated on 11 more (max residual 4.3 tokens below the
ceiling). Reproduce: `probes/hybrid_p5_vision_crop_cost.py --measure`,
`probes/hybrid_p5b_image_token_curve.py`.

Trigger firing rates on Track A's 10-pair benchmark (`hybrid_trigger_budget.json`):
4 pairs fire at least one trigger, 8 triggers total, 6 pairs need no Vision call at all.

## Invariants of the contract

1. **The question is closed-form.** Either a fixed enum, an integer, or a verbatim
   transcription of a named rectangle. Never "what changed?", never "compare these".
2. **One rectangle, one question.** The model is given the crop and told what is inside it.
   It is never given both versions and asked to align them.
3. **A refusal is a first-class answer.** `UNREADABLE` must be cheaper for the model than
   a guess; the system must have a defined behaviour for it that is not "assume no change".
4. **The answer never sets the verdict alone.** Vision fills one field of a deterministic
   record; the classification is computed from the record.

---

## VQ-1 — undecodable embedded font (grounded in `vk_nodes`)

| | |
|---|---|
| **Trigger** | `comparison.text.left_layer_quality.status == "UNDECODABLE"` or the right one. Measured on `vk_nodes`: suspicious control-char ratio 0.2572 / 0.2729, threshold 0.02. Fires on 3 of 10 pairs (`vk_plan`, `vk_nodes`, `vk_node_plan`). |
| **Crop** | The named sub-region only, not the block. For `vk_nodes` the notes/annotation region, block-relative rect `(0.55, 0.62, 1.00, 1.00)`, zoom 2 → 966×1276 px → **1,538 tokens (measured, not estimated)**. At zoom 4 the same region costs 2,991 tokens for no extra information, so zoom 2 is the default. |
| **Question** | «Внутри рамки выпиши все числовые значения, размеры и буквенно-цифровые обозначения — символ в символ, слева направо, сверху вниз. Не объясняй их назначение.» |
| **Allowed answers** | `{"values": [ "…", … ]}` or `{"values": null, "reason": "UNREADABLE"}`. Nothing else. |
| **System action** | `values` → written into the change record as `source: "vision"`, then diffed against the identical task run on the other version's matching sub-region. `UNREADABLE` → the pair is reported as «текстовый слой не читается, значения не сверялись», **never** as NEAR_IDENTICAL. |
| **Never asked** | whether anything changed; what the annotation means; which annotation matches which. |
| **What it replaces** | Track A shipped 13,212 tokens of this undecodable text to the model for `vk_nodes` alone (60.7 % of that pair's payload) and let the comparator invent 30 `value_changes` between mojibake strings (`hybrid_wasted_tokens.json`, `hybrid_boundary_evidence.json`). |

---

## VQ-2 — extra contour: new element or crop artefact (grounded in `ss_table_graphic`)

| | |
|---|---|
| **Trigger** | a `geometry_only_in_<side>` cluster (≥ 8 segments) whose bbox lies **entirely inside the crop-difference band** `rect_own \ rect_other`, **and** the page-context predicate returned nothing because the region contains no text. Measured band for `ss_table_graphic`: Δbbox = (+2.38, −14.48, −15.26, +14.59) pt. |
| **Crop** | the band strip only, both versions, zoom 2. Measured for this pair: 1885×472 px → **1,113 tokens** per side. |
| **Question** | «На правом изображении внутри рамки есть контур, которого нет на левом. Это (A) новый элемент чертежа, (B) продолжение элемента, срезанного границей кадра, (C) линия рамки или штампа листа?» |
| **Allowed answers** | `A` \| `B` \| `C` \| `UNREADABLE`. |
| **System action** | `A` → emit «Появился новый элемент» with `source: vision`. `B`/`C` → drop the geometry delta **and** flag the pair as crop-mismatched so the next extraction widens both rects. `UNREADABLE` → uncertainty, no change reported. |
| **Measured caveat** | For `ss_table_graphic` this task **would not fire**: the text-based page-context predicate already settles it. 4 of 16 change events («1», «Монтажная», «коробка», «RVi 2BM») are provably crop artefacts — the row sits at page y 77.6–88.7, above the left crop's top edge (100.5) and intersecting the right crop's (86.1) (`hybrid_crop_attribution.json`). Both Track A's vector arm and my first minimal payload reported it as a design change. Vision is the *second* line of defence here, not the first. |

---

## VQ-3 — repeated-unit count (grounded in `eom_singleline_changed`)

| | |
|---|---|
| **Trigger** | designation counting (`hybrid_designation_counts.json`) reports a prefix whose count changed by ≥ 2, text layer GOOD on both sides, geometry similarity < 0.5. Measured on `eom_singleline_changed`: `Wh 0→4`, `QD 0→4`, `QF 0→4`, `ЩМкв 0→4`, geometry similarity 0.1739. |
| **Crop** | the region spanning the designations on the *new* version, zoom 2. Measured: 619×703 px → **577 tokens**. |
| **Question** | «Сколько одинаковых отходящих ветвей изображено внутри рамки? Ответь одним числом.» |
| **Allowed answers** | integer `0…99` \| `UNREADABLE`. |
| **System action** | equal to the deterministic count → «Ветвей 2 → 4», `confidence: confirmed`. Different → report the deterministic count with `confidence: disputed` and attach the crop for the expert. `UNREADABLE` → deterministic count with `confidence: vector_only`. |
| **Never asked** | what the branches feed; which branch matches which; whether the change matters. |

---

## VQ-4 — encoding rewrite (grounded in `ss_scheme_text_changed`)

| | |
|---|---|
| **Trigger** | `geometry.encoding_rewrite_suspected == true` **and** primitive count changed ≥ 2× **and** geometry similarity ≥ 0.80 **and** text effective similarity ≥ 0.90. Measured: 39 → 159 primitives, closed contours 1 → 121, geometry 0.8664, text 0.9137. Fires on 1 of 10 pairs. |
| **Crop** | the union bbox of the right-only geometry clusters, both versions, zoom 2. Measured quarter-block crops for this pair: **1,812 tokens** two-sided. |
| **Question** | «Появились ли внутри рамки новые изображённые элементы — не заливка и не штриховка уже существующего контура? Да / Нет.» |
| **Allowed answers** | `ДА` \| `НЕТ` \| `UNREADABLE`. |
| **System action** | `НЕТ` → primitive-count and topology deltas are removed from the report entirely. This is exactly the line Track A's comparator emits as «Число примитивов: 39 → 159» (orchestrator finding O7). `ДА` → keep and localise. |

---

## VQ-5 — dense capped geometry (guard; grounded in `ar_plan` / `ss_plan_dense`)

| | |
|---|---|
| **Trigger** | `vector_quality` ∈ {LIMITED_CAPPED} on either side **and** geometry similarity < 0.99 **and** the text diff is empty. |
| **Measured firing** | capped is true on 5 of 10 pairs, but similarity < 0.99 on **none** of them → this task fires **0 times** on the benchmark. It is a guard against the sampled-comparison blind spot, not a routine cost. |
| **Crop** | the quadrant with the lowest local segment coverage, both versions, zoom 2. Measured cost for `ar_plan` quarter-block crops: **5,720 tokens** two-sided — the most expensive task in the set, which is why the trigger is deliberately narrow. |
| **Question** | «Различаются ли изображения внутри рамки? Да / Нет / Не могу определить.» |
| **Allowed answers** | `ДА` \| `НЕТ` \| `UNREADABLE`. |
| **System action** | `НЕТ` → NEAR_IDENTICAL stands. `ДА` → escalate the crop pair to the expert; the model is **not** asked what differs, because at this density its answer would not be checkable. |

---

## VQ-6 — unreadable value at source (rotated or overlapped dimension text)

| | |
|---|---|
| **Trigger** | a value change whose spans are non-orthogonally rotated (|rot − k·90°| > 2°) or overlapped by geometry. Measured: **935 of 5,427 spans (17.2 %)** across the 20 benchmark blocks are non-orthogonal — 345°, 75°, 299°, 241°; these are the inclined dimension strings. |
| **Crop** | the two span bboxes padded 3 mm, zoom 4. A 400×160 px crop costs **127 tokens** by the measured formula — the cheapest task in the set. |
| **Question** | «Выпиши текст внутри рамки символ в символ.» |
| **Allowed answers** | string \| `UNREADABLE`. |
| **System action** | replaces the vector string in the record; if vector and vision agree → «Номинал 250 → 315 А» with `confidence: confirmed`; if they disagree → report the PDF text-layer value and mark `vision_disagrees`. |
| **Never asked** | what the value means; whether it is correct; whether it changed. |

---

## Budget

| item | tokens | source |
|---|---:|---|
| Track A vector arm, 5 pairs | 70,631 | `invocation_metadata.json` |
| Track A vision arm, 5 pairs (10 full-block crops) | 38,069 | `invocation_metadata.json` |
| Minimal payload, same 5 pairs, same model, same schema | **20,483** | measured, `hybrid_minimal_ai_run_base.json` |
| VQ-1 on `vk_nodes` (both sides, zoom 2) | 3,076 | measured crops |
| VQ-4 on `ss_scheme_text_changed` (both sides, zoom 2) | 1,812 | measured formula |
| VQ-6, per value | ~127 | measured formula |

A hybrid run over the 5 pairs = 20,483 + 3,076 + 1,812 ≈ **25,371 tokens**, i.e. 0.36× the
vector arm and 0.67× the vision arm, with the undecodable block *actually answered* instead
of being guessed at from 13,212 tokens of mojibake.
