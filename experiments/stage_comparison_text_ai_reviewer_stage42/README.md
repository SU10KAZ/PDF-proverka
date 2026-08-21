# Stage 4.2 page-membership regression

This experiment replays all 140 `MOVED_PAGE_SEMANTICS` decisions from the
committed Stage 4.1 `uncertain_inventory.json`. Each diagnosed decision is an
independent reviewer group; its accepted parent `left_pages`/`right_pages`,
exact referenced source fragments and preliminary evidence are preserved.

Run from the repository root:

```bash
python3 experiments/stage_comparison_text_ai_reviewer_stage42/regression.py build
python3 experiments/stage_comparison_text_ai_reviewer_stage42/regression.py run --no-resume
python3 experiments/stage_comparison_text_ai_reviewer_stage42/regression.py summarize
```

The run uses the unchanged production reviewer model `gpt-5.6-luna`, hint and
`medium` effort with native JSON Schema. Artifacts retain every normalized
decision and its original case/group/page provenance.
