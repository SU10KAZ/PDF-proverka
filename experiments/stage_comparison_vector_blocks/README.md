# Stage Comparison vector blocks — research only

This directory is an isolated proof of concept for describing and comparing already-paired PDF graphic blocks through the PDF vector/text layers. It has no imports from, hooks into, or writes to production Stage Comparison.

Core extraction uses only:

- `fitz.Page.get_drawings()`;
- vector text spans and their coordinates;
- a supplied page index plus normalized bbox/polygon.

OCR, Vision, raster recognition, embeddings, ORB and affine alignment are excluded. PNGs are emitted only for human validation and for the separately labelled Vision-vs-vector experiment.

## Contents

- `extractor.py` — `VectorBlockDescription` research schema, Markdown rendering and diagnostic crop.
- `comparator.py` — order/path-packaging-independent segment coverage, text/topology/pattern diff and five research statuses.
- `run_research.py` — reproduces 20 descriptions, 10 comparisons, PNGs, overlays and benchmark summary from the manual manifest.
- `run_ai_experiment.py` — optional two-arm `gpt-5.6-sol` experiment on five pairs.
- `test_vector_blocks.py` — six synthetic regression tests.
- `artifacts/block_pairs.json` — explicit manual pairing of real V1/V2 or V2/V3 blocks.
- `artifacts/descriptions/<pair>/<side>/` — required JSON and Markdown.
- `artifacts/comparisons/<pair>/` — comparison JSON/Markdown and normalized SVG overlay.
- `artifacts/diagnostics/<pair>/` — raster crops for human checking only.
- `artifacts/human_validation.*` — visual judgement for every pair.
- `artifacts/ai_experiment/` — retained prompts, outputs, invocation metadata and evaluation.
- `VECTOR_BLOCK_RESEARCH_REPORT.md` — conclusions and the one requested recommendation.

## Reproduce

From the repository root, with PyMuPDF available:

```bash
python -m experiments.stage_comparison_vector_blocks.run_research
python -m unittest experiments.stage_comparison_vector_blocks.test_vector_blocks -v
```

To iterate only on comparator logic using existing descriptions:

```bash
python -m experiments.stage_comparison_vector_blocks.run_research --reuse-descriptions
```

The optional model experiment requires an authenticated `codex` CLI and access to `gpt-5.6-sol`:

```bash
python -m experiments.stage_comparison_vector_blocks.run_ai_experiment --arm all
```

Model outputs are nondeterministic; the exact evaluated prompts and responses are retained under `artifacts/ai_experiment/`.

## Important limits

- Bbox normalization works only when the two bboxes enclose the same semantic content. It does not repair a wrong block match or materially different crop.
- Dense CAD blocks hit explicit primitive/topology/segment caps and are labelled `LIMITED_CAPPED`.
- Some VK PDFs expose unreadable character codes through their embedded-font mapping. These text layers are labelled `UNDECODABLE`; the experiment does not use OCR to repair them.
- Generic topology is endpoint/T-junction based. X crossings are not connected without evidence; discipline-specific electrical/hydraulic meaning is intentionally absent.
- Thresholds are calibrated only on this small benchmark and are not production policy.
