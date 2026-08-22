# Vector 0.3 — prepared graphic objects (Codex)

Independent research experiment for object-level comparison of already prepared graphic blocks. It does not modify or integrate with Production Stage Comparison.

The only benchmark input is an explicit prepared-block reference:

```json
{"blocks_json": "<version>/02_work/blocks.json", "block_id": "blk_...", "block_group_id": "pair-id"}
```

Coordinates are always resolved from the existing `blocks.json`; benchmark bbox duplication, block detection, sheet matching and 1→N matching are forbidden. Text metadata may address an object but text changes never enter the graphic ledger. Upstream text/table blocks return `GRAPHIC_NOT_APPLICABLE`.

Main commands:

```bash
python -m experiments.stage_comparison_graphic_objects_v03_codex.run_benchmark
python -m experiments.stage_comparison_graphic_objects_v03_codex.run_hybrid_experiment
python -m pytest -q experiments/stage_comparison_graphic_objects_v03_codex/test_graphic_objects_v03.py
```

`artifacts/object_descriptions/**` and `artifacts/object_comparisons/**` contain readable sampled JSON indexes. Each index points to a deterministic lossless `*.full.json.gz` payload and records its uncompressed SHA-256 and full counts. `.page_cache/` is local, keyed by PDF SHA + page + extractor version, and is not committed.

The research result and verdict are in [CODEX_GRAPHIC_OBJECTS_V03_REPORT.md](CODEX_GRAPHIC_OBJECTS_V03_REPORT.md). Verdict: **C — the generic object layer is insufficient; discipline-specific profiles are needed earlier.**
