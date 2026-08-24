# Strong P → RD: structural comparison of the GRSh pair

This is a research-only proof of concept for two already prepared IMAGE blocks of one GRSh solution. It does not modify or integrate with Production Stage Comparison.

## Scope

- LEFT: `blk_039909ec039649a1b8209f059c95167b`
- RIGHT: `blk_2d72a6705eaf4d8c9ee1d6ff459b15a6`
- pairing: supplied explicitly; no bbox detector, sheet matcher, or 1→N matcher runs;
- text is used only as an EOM identifier/topology anchor;
- text/table differences are outside the experiment.

The two blocks are first described independently as system graphs. Cross-version coordinates are never used for identity. The graphs are then compared at three levels: system backbone, functional groups, and individual devices/connections.

## Reproduce

From the repository root:

```bash
python experiments/strong_stage_graph_comparison_grsh/extract_structural_graph.py
python experiments/strong_stage_graph_comparison_grsh/compare_structural_graph.py
```

The first command resolves the existing upstream polygons, extracts vector anchors, verifies the outgoing-device positions, and writes both independent graph JSON files. The second command compares those graphs, renders the blocks, measures registration/ink failure, and runs the existing generic prepared-object comparator as a control.

## Artifacts

- `human_ground_truth.md` — manual raster adjudication fixed before the generic automatic comparison;
- `left_structural_description.{json,md}` and `right_structural_description.{json,md}` — independent system graphs;
- `structural_comparison.{json,md}` — graph/identity comparison;
- `overlay_comparison_diagnostic.json` — registration, ink, and generic-object diagnostics;
- `renders/side_by_side.png` and `renders/overlay_stretch_diagnostic.png` — visual evidence;
- `STRONG_P_RD_GRSH_REPORT.md` — final conclusions and future Mode 2 design.

## Result

The backbone is preserved: two sources, two inputs, two bus sections, and one inter-section tie. The implementation and presentation are not unchanged: the section device changes from motorized `QF3` with AVR representation to `QS1`, outgoing devices change from `15+15` to `13+14`, and branch order is heavily reorganized.

Verdict: **B — structural graph comparison works, but reliable correspondence on this pair requires an EOM/single-line profile.**
