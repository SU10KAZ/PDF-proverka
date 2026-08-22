# TCF — TOPOLOGY CONFIDENCE: which topological facts are trustworthy?

Track B probe `tcf`. Everything below was computed by scripts in
`experiments/stage_comparison_vector_architecture_opus/probes/`, run from the repository root,
against the 20 real Track A block descriptions in
`experiments/stage_comparison_vector_blocks/artifacts/descriptions/`.

`probes/tcf_topo.py` is an instrumented **fork** of `extractor._topology` (Track A files untouched).
It reproduces Track A's stored numbers exactly on all 10 dense and sparse blocks tested
(`selftest()` diff = `{}` — see "Reproduction" at the end), so every re-measurement below is
comparable to the shipped values.

---

## Headline table

| claim | evidence (measured) | confidence | how it could be falsified |
|---|---|---|---|
| **T1.** The comparator's topology similarity is dominated by the node-merge tolerance, not by the drawing. Comparing **one and the same block** against itself at another tolerance yields similarity **0.43–0.77**; the comparator's own NEAR_IDENTICAL gate is 0.85. All **10/10** benchmark blocks fall below that gate under pure tolerance jitter. | `tcf_p1_tolerance.json → summary[].min_self_similarity`: vk_node_plan 0.4300, vk_plan 0.5011, vk_nodes 0.5148, ar_plan 0.5283, ar_wall_sections 0.5462, ss_plan_dense 0.5592, eom 0.6775, ss_scheme 0.6802, ss_table 0.7138, ss_simple_node 0.7723 | high | show a tolerance-invariant reformulation (ratios/densities) that keeps ≥0.85 self-similarity over 0.0005…0.01 |
| **T2.** The nine counts the comparator averages are not properties of the drawing. Over tolerance 0.0005→0.01: `connected_components` moves **282×** (ar_wall_sections 3393→12), `endpoints` **259×** (8288→135), `node_count` **10×**, `branch_points` **7.7×**, `t_junctions` **6.9×**. Movement is also **non-monotonic** (ss_scheme components 407→71→138). | `tcf_summary_tables.md §1a`; e.g. ar_wall_sections `connected_components` [3393, 2337, 40, 22, 12] | high | show the same corpus with per-block max/min ≤ ~1.5 at any fixed pair of settings |
| **T3.** Tolerance change of a factor 2.5 (0.001→0.0025) is a **larger** perturbation than a real document revision. `eom_singleline_changed` — the only П↔РД pair with real engineering change — scores topology similarity **0.6102** left↔right, while ar_plan scores **0.5283** against *itself* re-run at a different tolerance. | `tcf_p1_tolerance.json → summary` | high | pair the tolerance sweep against a corpus of genuine revisions and show change signal > jitter |
| **T4.** Two-thirds of a dense block's linework is shorter than the node tolerance and **collapses into a self-loop**, i.e. is invisible to the graph: ss_plan_dense **55 754 / 84 439 = 66.0 %**, vk_node_plan 60.2 %, vk_nodes 59.2 %, ar_wall_sections 53.6 %, ar_plan 41.6 %. The 8 000-segment cap hides this (it keeps long segments, so only 0.2–38 % of the *retained* set collapses). | `tcf_p2_cap.json → blocks[*].uncapped.collapsed_segments` vs `selection.segments_total` | high | measure at a tolerance small enough to keep them and show the graph stays tractable |
| **T5.** The 8 000-segment cap is not a sampling detail, it is a different graph. Same block, cap 8000 vs uncapped: topology similarity **0.6063–0.8611** (ss_plan_dense 0.6063). Uncapped ss_plan_dense: node_count 6153→19 136, t_junctions 12 591→44 854, crossings 4626→5000 (truncation now fires). | `tcf_p2_cap.json → pairs[].similarity_cap_vs_uncapped_same_block`; `blocks[].cap8000/uncapped` | high | show the two graphs agree on any statement an expert would read |
| **T6.** Longest-first capping is a **length threshold**, not a sample: median retained length is 3.3× (ar_plan) to **14×** (ss_plan_dense) the block median; the cut-off is block-specific (0.00104–0.00422 normalized) and **differs between the two versions of the same block** (vk_node_plan 0.00104 vs 0.00110), so the two sides are not even computed on the same sub-population. | `tcf_p2_cap.json → blocks[].selection` (`length_cutoff`, `median_length_kept`, `median_length_all`) | high | show cut-off stability < 1 % across versions on a larger corpus |
| **T7.** "Biased toward frame and background lines" is only **partly** true and is block-dependent — this probe partially falsifies its own hypothesis. Retained set is more axis-aligned on ss_plan_dense (0.789 vs 0.491 dropped) but **less** on ar_wall_sections (0.249 vs 0.540). Spatial coverage barely changes (85.6–100 % of occupied 40×40 cells still touched). What the cap really destroys is **objects**: 50.7–62.5 % of primitives are dropped entirely (ar_plan 7484/14 753, vk_node_plan 12 495/20 000), and on ss_plan_dense **39.7 %** of primitives enter the graph **mutilated** (some segments kept, some dropped). | `tcf_p2_cap.json → selection.axis_aligned_share_*`, `cell_coverage_kept`; `tcf_p2b_capobjects.json → A_object_mutilation` | high | show a length-blind cap (e.g. spatial stratification) with the same cost |
| **T8.** `x_crossings_unconnected` does not measure unconnected crossings. **90.0–99.8 %** of them join segments **already in the same connected component** (vk_node_plan 99.79 %, ar_wall_sections 99.9 %), **34.9–62.2 %** were **already glued by the T-junction pass at that very place**, and **35.1–97.2 %** sit within the node tolerance of an endpoint of one of the two segments. | `tcf_p3_crossings.json → per_block` (`share_same_component`, `share_joined_by_t_junction`, `share_near_endpoint`) | high | show the recorded pairs are disjoint from the T-junction set |
| **T9.** Visually, at most **4 of 15** sampled crossings carry any engineering meaning: 1 REAL_CONNECTION (a junction dot on an EOM bus — labelled "unconnected"), 3 informative crossings, 3 intra-symbol, **8 pure background** (hatch, axis grid, a dimension glyph, and a hand **signature inside the title block**). | `tcf_p3_visual_verdicts.json`, crops `artifacts/tcf_crops/tcf_x_montage_{tight,wide}.png` | medium (my visual judgement, 15 samples) | have the domain expert re-label the same 15 crops |
| **T10.** The 5000-crossing truncation fires on **2/20** blocks at ship settings (ar_plan left and right) and on ss_plan_dense once the cap is lifted. When it fires on both sides the comparator sees `5000 vs 5000` and awards that key a **perfect 1.0** — truncation manufactures agreement. | `tcf_inventory.json` (`crossings_truncated`), `comparator._topology_diff` averages `_numeric_similarity(5000, 5000) = 1.0` | high | show the truncated subsets are order-stable and comparable |
| **T11.** Curve flattening is **not** a problem in this corpus — hypothesis falsified. `CURVE_STEPS = 6` gives max chord error **0.067 pt / 0.00012 normalized** (eom, 316 cubics), 0 % above the 0.0025 tolerance; the 24-gon radial inset is `r·0.008555`, so a circle would need normalized radius **> 0.2922** to break a tangent, while the largest circle in the corpus is **0.0231**. Both real extractor routes for a circle (24-gon from the rect vs 4 cubics flattened at 6 steps) score **1.0** segment coverage against each other at every tolerance. | `tcf_p4_curves.json → A_cubic_flattening, B_circle_census`; `tcf_p4b_bezier.json → cases[].route1_vs_route2` | high | find a corpus where arcs are exported as polylines with a different vertex count |
| **T12.** The residual re-flattening risk is real but latent: if the two versions encode the same circle with different vertex counts or a half-step phase, segment coverage collapses to **0.0** at tolerances ≤0.005 (cause: the angle gate `max(1.0, tolerance·500)` = 1.0–2.5°, while a half-step of a 24-gon is 7.5°). In this corpus all near-circular closed shapes have exactly **24** vertices, so it never fires. | `tcf_p4_curves.json → C_synthetic_reflattening_coverage`; `comparator.py:333–336`; `tcf_p2b_capobjects.json → B_round_shape_census` (24: 93/6/34/204) | high (synthetic, labelled) | show CAD exporters never change arc tessellation between П and РД |
| **T13.** Block normalization is **anisotropic**: aspect up to **3.75** (ss_scheme), so tolerance 0.0025 means **4.04 pt horizontally and 1.08 pt vertically** on the same block, and a drawn circle becomes an ellipse of normalized aspect **0.323–5.84**. Node merging is therefore direction-dependent, and the aspect differs slightly between the two versions (3.747 vs 3.721), so the two graphs use slightly different metrics. | `tcf_p4_curves.json → D_normalization_anisotropy`; `tcf_p2b_capobjects.json → C_circle_aspect_after_normalization` | high | normalize isotropically and show the counts do not move |
| **T14.** `nested_contours` is close to information-free. On ss_plan_dense **all 381** nested contours share **one** container, which is the block frame, and **50.4 %** of those containments are false when the inner centroid is tested against the container's real polygon. On ar_plan **70.3 %** of 310 share the largest contour. Proper depth ≥2 is 0 on 7 of 10 blocks. The documented 1000-primitive limit **never fires** here (max 382 closed contours) — that criticism is latent, not realized. | `tcf_p5_nesting.json` | high | show a corpus where containers are diverse and depth ≥2 is common |
| **T15.** The degree histogram *is* a usable **identity** signature at fixed settings — nearest neighbour is the block's own pair partner in **18/20** cases, and the only exact collision (`ss_simple_node/left == right`) is a legitimate one — but it is a signature of *(block × tolerance)*: distance to the same block at tolerance 0.001 reaches **1.1075** (of max 2.0) while distance to its own partner is **0.0714**. | `tcf_p6_degree.json` | high | show cross-tolerance distance < within-pair distance |
| **T16. (new, not in O1–O7)** On pages with `/Rotate 90|270` the extractor reads the block window in the **wrong coordinate frame**. `extract_block` builds `block_rect` from `page.rect` (display frame) and clips `page.get_drawings()`, which PyMuPDF returns **unrotated**. **7 of 20** benchmark blocks sit on rotated pages; mean IoU between the region described and the region the bbox denotes is **0.1187**, and for `vk_node_plan` (both sides) it is **0.0** — the description named "node plan" actually describes an equipment specification table. `vk_plan`'s window even falls partly outside the unrotated page. | `tcf_p7_rotation.json`; visual proof `artifacts/tcf_crops/tcf_rot_regions.png`; `extractor.py:947–951` vs `extractor.py:378` | high | show `page.get_drawings()` honours `/Rotate` in PyMuPDF 1.27.2.2 (it does not: the point (451.14, 646.74) renders blank, its image under `page.rotation_matrix` renders the geometry) |
| **T17. (new)** The **only** pair carrying real engineering change, `eom_singleline_changed`, has **rotation 270 on the left and 0 on the right**, so its two sides are extracted in different frames. Its measured similarities (topology 0.6102, and everything downstream) describe two different regions of two sheets. | `tcf_p7_rotation.json` rows `eom_singleline_changed` | high | re-extract both sides in the same frame and show the numbers are unchanged |
| **T18. (new, decisive for the audit question)** Signal is far below noise on dense blocks. Injecting **20 real branches + 20 device circles** (540 segments) into ar_plan changes topology similarity by **0.0136** — **34.7×** less than the tolerance artefact (0.4717) and 13.7× less than the cap artefact (0.1864). Both the geometry gate (0.9973 ≥ 0.985) and the topology gate (0.9864 ≥ 0.85) still pass, so the comparator would call that sheet **NEAR_IDENTICAL**. | `tcf_p8_injection.json → blocks.ar_plan.runs["k=20"]`, `blocks.ss_plan_dense.runs["k=20"]` (0.9827/0.9957) | high | show a dense-block change of this size that the comparator does flag |
| **T19. (new)** Under the cap, **adding geometry deletes geometry**. Injecting 20 branches into ss_plan_dense keeps `segments_used` at exactly 8000 and moves `branch_points` **4636 → 4632, i.e. down**, because the injected segments displace previously retained ones. `endpoints` moves 946 → 972 with no relation to the 20 attachments. | `tcf_p8_injection.json → blocks.ss_plan_dense.runs["k=20"]` | high | show a capped block where counts move by the injected amount |
| **T20. (new)** The same metric is **over-sensitive** where it is not blind: one single injected branch drops `ss_simple_node` to topology similarity **0.7669** and geometry **0.8125** → STRUCTURE_CHANGED. So the identical threshold set is simultaneously blind (T18) and hair-trigger, depending only on block density. | `tcf_p8_injection.json → blocks.ss_simple_node.runs["k=1"]` | high | show a density-normalized threshold that fixes both ends |
| **T21. (new)** PDF path packaging bears **no relation to drawn objects**, so there is nothing to build a topology *of objects* on. A path maps to ~1 primitive in all 10 blocks (`primitives_per_pdf_path` 1.000–1.028), but what that primitive contains differs by two orders of magnitude between exporters: AR/VK paths are single lines (**0.99–1.22 segments per primitive**; ar_plan = 14 800 primitives from 14 752 paths), while SS paths pack unrelated linework (**ss_table_graphic 144.8**, ss_plan_dense 52.6 segments per primitive, its `primitive-7` alone holding **17 486 segments across 74 % of the block area**). Neither extreme is an object; an object layer must be *constructed*, it cannot be read off. Consequence inside the extractor: the crossing filter "skip if same `primitive_id`" suppresses everything inside that 17 486-segment mega-path and nothing at all in ar_plan. | `tcf_p3b_same_path_crossings.json`; `tcf_inventory.json` (segments/primitives); `ss_plan_dense primitive-7`: `segment_count = 17486`, bbox [0.05, 0.073, 0.936, 0.912] | high | find CAD exports whose PDF paths correspond to drawn symbols |

---

## Tier table (the deliverable asked for)

Stability column = measured movement of the fact under settings that a human would call "the same
extraction" (tolerance 0.0005…0.01, cap 8000 vs uncapped), on the same block.

| topological fact | measured stability | recommended tier |
|---|---|---|
| `segments_total` | invariant to tolerance (max/min = 1.00 across all 5 tolerances) | **high-confidence fact** — but it is a size measure, not topology |
| `segments_capped` / `vector_quality` | deterministic flag; true for 10/20 blocks | **high-confidence fact** (as a *warning*, and it must gate every count below) |
| `closed_contours` | invariant to tolerance (1.00), but a function of PDF packaging (`re`/`qu`/closePath and the 0.05 pt polyline-closure heuristic) | **candidate evidence** |
| `node_count` | 1.36×–10.06× over tolerance; +36 % (ar_plan, 6003→8190) to +211 % (ss_plan_dense, 6153→19 136) when the cap is lifted | **candidate evidence** at frozen settings; drop as an absolute |
| `edge_count` | 1.07×–4.16× over tolerance; saturates at the cap (= 8000 exactly on 3 blocks) | **candidate evidence** at frozen settings |
| `degree_histogram` | NN retrieval 18/20 at frozen settings; L1 distance to itself at another tolerance up to 1.1075 vs 0.0714 to its own partner | **candidate evidence** — usable for block *matching*, never as a description |
| `t_junctions` | 1.33×–6.88×; 22 064 junctions over 8000 segments on ar_wall_sections (2.8 per segment) | **candidate evidence** (as raw junction *hypotheses*), drop as a count |
| `branch_points` | 1.18×–7.74×; moves the wrong way under the cap (4636 → 4632 after adding 20 branches) | **drop** |
| `endpoints` | 1.07×–259× (ar_wall_sections 8288 → 135 between tolerance 0.001 and 0.0025) | **drop** |
| `connected_components` | 4.67×–282.75× | **drop** |
| `x_crossings_unconnected` | 90–99.8 % already in one component; 35–62 % already glued by the T-junction pass; 8/15 visually pure background; truncates at 5000 on 2/20 blocks and then scores a free 1.0 | **drop** |
| `nested_contours` | invariant to tolerance, but 100 % single-container on ss_plan_dense with 50.4 % false containment; depth ≥2 = 0 on 7/10 blocks | **drop** (replace with true depth ≥2 count if anything) |
| `components[]` rows (`node_count`, `segment_count`, `max_degree`) | inherits the instability of `connected_components` (282×) | **drop** |
| the comparator's averaged `topology.similarity` | self-similarity 0.43–0.77 under tolerance jitter alone, below its own 0.85 gate on 10/10 blocks | **drop** as a status gate; keep only as a debug number |

---

## What this means for the audit question

1. **Not a single count in the topology layer survives as a fact.** The only tolerance-invariant
   numbers (`segments_total`, `closed_contours`) are not topology; every genuinely topological
   number moves by 1.2× to 283× under settings a human would call identical. A layer whose outputs
   move that much cannot support the sentence «Добавлены два ответвления» — T18/T19/T20 show it
   directly: 20 new branches are invisible on a dense plan and 1 new branch is a catastrophe on a
   sparse node.
2. **The failure is not tuning.** Any single tolerance is simultaneously too coarse (66 % of
   ss_plan_dense's segments collapse) and too fine (ar_wall_sections goes from 3393 to 40 components
   across one step). A drawing has structure at several scales at once; one global ε cannot express it.
   This is an argument for grouping geometry into **objects with their own scale** before asking any
   connectivity question — i.e. an object layer, not a better ε.
3. **The junction evidence is there, the interpretation is not.** T-junction hits and crossing
   candidates are plentiful and cheap; what is missing is the ability to say *what* touches *what*.
   T8 shows the extractor cannot even keep its own two answers consistent (it labels as "unconnected"
   pairs it glued one pass earlier), and T21 shows the PDF offers no grouping to fix that with.
   That is exactly the gap a **graphical-object + relation-graph** layer fills.
4. **Two blocking defects must be fixed before any of this is measured again**: the rotated-page
   coordinate frame (T16/T17 — 7/20 blocks describe the wrong region, including both sides of
   `vk_node_plan` and one side of the only real-change pair) and the anisotropic normalization (T13).
   Until then, published topology numbers on those blocks describe a different part of the sheet.
5. **Falsified sub-hypotheses, reported as such**: curve flattening error (T11) and the 1000-contour
   nesting limit (T14) are *not* problems in this corpus, and longest-first capping is not uniformly
   biased to frame lines (T7). The damage from capping is object loss, not frame bias.

---

## Reproduction

All commands from `/home/coder/projects/PDF-proverka`.

```bash
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_inventory       # ~3 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p1_tolerance    # ~36 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p2_cap          # ~25 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p2b_capobjects  # ~3 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p3_crossings    # ~9 s (renders crops)
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p3b_samepath    # ~5 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p4_curves       # ~10 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p4b_bezier      # ~1 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p5_nesting      # ~1 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p6_degree       # ~1 s (needs p1)
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p7_rotation     # ~2 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p7b_render_regions # visual proof
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p8_injection    # ~20 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_tables          # renders tcf_summary_tables.md
```

Fork fidelity check (must print `{}` for every block):

```bash
python - <<'PY'
import json
from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo
for pair in ("ss_simple_node","ss_scheme_text_changed","eom_singleline_changed","ss_table_graphic",
             "vk_plan","ar_plan","ss_plan_dense","vk_nodes","ar_wall_sections","vk_node_plan"):
    d = json.load(open(f"experiments/stage_comparison_vector_blocks/artifacts/descriptions/{pair}/left/vector_block.json"))
    print(pair, tcf_topo.selftest(d))
PY
```

Rotation-frame proof (blank at the raw point, geometry at its rotated image):

```bash
python - <<'PY'
import collections, fitz
p = "projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К4/versions/v001/02_work/document.pdf"
pg = fitz.open(p)[8]
print("rotation", pg.rotation, "display", pg.rect, "unrotated", pg.mediabox)
for label, pt in (("as stored", fitz.Point(451.14, 646.74)),
                  ("rotated", fitz.Point(451.14, 646.74) * pg.rotation_matrix)):
    pix = pg.get_pixmap(matrix=fitz.Matrix(22, 22), clip=fitz.Rect(pt.x-7, pt.y-7, pt.x+7, pt.y+7), alpha=False)
    c = collections.Counter(pix.pixel(x, y) for y in range(pix.height) for x in range(pix.width))
    print(label, c.most_common(2))
PY
```

## Artifacts

| file | content |
|---|---|
| `artifacts/tcf_inventory.json` | topology fields of all 20 shipped descriptions |
| `artifacts/tcf_p1_tolerance.json` | 20 blocks × 5 tolerances, all counts + comparator similarities |
| `artifacts/tcf_p2_cap.json` | cap 8000 vs uncapped, plus the longest-first selection profile |
| `artifacts/tcf_p2b_capobjects.json` | objects lost/mutilated by the cap; round-shape census; circle aspect after normalization |
| `artifacts/tcf_p3_crossings.json` | per-block crossing audit + the 15 sampled crossings |
| `artifacts/tcf_p3_visual_verdicts.json` | my labels for the 15 sampled crossings |
| `artifacts/tcf_p3b_same_path_crossings.json` | same-PDF-path share and PDF grouping stats |
| `artifacts/tcf_p4_curves.json`, `artifacts/tcf_p4b_bezier.json` | flattening error, circle census, re-flattening coverage, anisotropy |
| `artifacts/tcf_p5_nesting.json` | nesting containers, false containment, true depth |
| `artifacts/tcf_p6_degree.json` | degree-histogram distances, collisions, NN retrieval |
| `artifacts/tcf_p7_rotation.json` | coordinate-frame audit of all 20 block windows |
| `artifacts/tcf_p8_injection.json` | injected-change signal vs noise floor |
| `artifacts/tcf_summary_tables.md` | every table above, generated from the JSONs |
| `artifacts/tcf_crops/` | crossing crops + montages, and `tcf_rot_regions.png` (meant vs used region) |
