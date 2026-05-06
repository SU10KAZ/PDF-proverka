# Блоки и Stage 02

## Production Flow

```text
Markdown PDF representation
→ blocks.py crop / document_graph
→ gemma_enrichment base pass (`_output/blocks_gemma_100`, 100 DPI)
→ optional targeted gemma_enrichment high-detail (`_output/blocks_gemma_300`, 300 DPI)
→ Stage 01 text analysis
→ Stage 02 findings_only_gemma_pair + GPT-5.4 (`_output/blocks_stage02_100`, 100 DPI)
→ blocks.py merge / coverage summary → 02_blocks_analysis.json
```

Основное правило: Stage 02 читает готовый `02_blocks_analysis.json`, а не
строит своё понимание напрямую из Gemma crop-ов.

## Production Profile

- Stage 02 model: `openai/gpt-5.4`
- Stage 02 batch mode: `findings_only_gemma_pair`
- Runtime mode: `single_block`
- Runtime plan: `_output/block_batches.runtime.json`
- Gemma base source of truth: `_output/blocks_gemma_100/`
- Gemma high-detail source of truth: `_output/blocks_gemma_300/` only for selected candidates
- Stage 02 source of truth: `_output/blocks_stage02_100/`
- `render_profile = r800` (`min_long_side = 800`, `dpi = 100`) for Stage 02 image input

`block_batches.json` may still exist as the raw plan from `blocks.py`, but
progress/resume/retry must read `_output/block_batches.runtime.json`.

## Split Responsibilities

- Gemma base 100 DPI covers all image blocks quickly and stably.
- Gemma high-detail 300 DPI is optional and must not be sent for every block.
- Stage 02 crops are independent from Gemma and must not overwrite or validate
  against high-detail Gemma crops.

## Candidate / Safety Rules

High-detail retry is reserved for blocks where base 100 DPI is weak, too short,
or clearly text-dense. Before sending 300 DPI to Gemma, enforce:

- `size_kb_300 <= 300`
- `long_side_300 <= 3500`
- `estimated_image_tokens <= 3500`

Oversized candidates are recorded as `skipped_large_block`; Stage 02 then works
from the best available base enrichment instead of pretending the block is clean.

## LM Studio Runtime Policy

- runtime pipeline does not `load` / `reload` / `unload` LM Studio models while
  Stage 00/01/02 is running
- base 100 DPI and high-detail 300 DPI do not reload the Gemma model between
  passes
- backend concurrency for high-detail may drop to `1`, but that is not the same
  thing as changing LM Studio model `parallel`
- post-queue cleanup may unload allowlisted Gemma models only after all queues
  are idle and grace period passes
