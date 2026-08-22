# Stage Comparison vector blocks v0.2 — Codex research only

Isolated continuation of `experiments/stage_comparison_vector_blocks/`. It adds a page cache, style diff, deterministic routing gates, crop/text/cap diagnostics, a 39-pair benchmark, change-only L3 payloads, a three-arm AI comparison, and a pre-comparison Vision verification experiment.

Nothing in this directory is imported by production Stage Comparison.

## Files

- `page_cache.py` — SHA-256 + page + extractor-version disk/RAM cache.
- `extractor.py` — cached `VectorBlockDescription` extraction and quality signals.
- `comparator.py` — geometry/topology/text/style/crop comparison and evidence.
- `gates.py` — fail-safe `VECTOR_OK` / `VECTOR_WITH_VISION` / `VISION_ONLY` routing.
- `l3_change_only.py` — compact changed-evidence payload.
- `benchmark_data.py`, `run_benchmark.py` — explicit real-pair manifest and benchmark.
- `run_style_experiment.py` — controlled and real style calibration.
- `run_prompt_size_experiment.py` — byte/token estimate plus real model probe.
- `run_hybrid_experiment.py`, `evaluate_hybrid.py` — Vector/Vision/Hybrid experiment.
- `run_description_verification.py` — raster verification of one vector description.
- `run_verified_pipeline_experiment.py` — end-to-end Pipeline B on the ten hard pairs.
- `evaluate_verified_pipeline.py` — retained human claim-level evaluation of Pipeline B.
- `artifacts/` — retained compact results and human ground truth.
- `CODEX_VECTOR_V02_REPORT.md` — methods, results, limitations and recommendation.

## Reproduce deterministic parts

From the repository root:

```bash
python -m unittest experiments.stage_comparison_vector_blocks_v02_codex.test_vector_v02 -v
python -m experiments.stage_comparison_vector_blocks_v02_codex.run_style_experiment
python -m experiments.stage_comparison_vector_blocks_v02_codex.run_benchmark
```

The 39-pair cold benchmark is intentionally expensive. `.page_cache/` is local and ignored by Git.

The following commands invoke authenticated `codex exec` calls with `gpt-5.6-sol`; their retained outputs are nondeterministic model evidence, not unit-test fixtures:

```bash
python -m experiments.stage_comparison_vector_blocks_v02_codex.run_prompt_size_experiment
python -m experiments.stage_comparison_vector_blocks_v02_codex.run_hybrid_experiment
python -m experiments.stage_comparison_vector_blocks_v02_codex.evaluate_hybrid
python -m experiments.stage_comparison_vector_blocks_v02_codex.run_description_verification
python -m experiments.stage_comparison_vector_blocks_v02_codex.run_verified_pipeline_experiment
python -m experiments.stage_comparison_vector_blocks_v02_codex.evaluate_verified_pipeline
```

The page cache stores trusted local pickle payloads and must not load files supplied by an untrusted source.
