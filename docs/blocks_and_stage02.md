# Блоки и Stage 02

## Production Flow

```text
Markdown PDF representation
→ blocks.py crop / document_graph
→ gemma_enrichment base pass (`_output/blocks_gemma_100`, 100 DPI)
→ optional targeted gemma_enrichment high-detail (`_output/blocks_gemma_300`, 300 DPI)
→ Stage 01 text analysis
→ Stage 02 findings_only_gemma_pair + GPT-5.4 (`_output/blocks_stage02_100`, 100 DPI)
→ blocks.py merge / coverage summary → 01_blocks_analysis.json
```

Основное правило: Stage 02 читает готовый `01_blocks_analysis.json`, а не
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

## Векторные графы блоков: параллелизм разбора

Стадия `block_context` («Векторные графы блоков») — чистый CPU: `fitz` + разбор
геометрии профиля, без модели и без сети. Замер на 16-ядерной машине (АР-листы,
профили `ar_floor_plan` / `ar_marking_plan`): один блок ≈ 0,7–1,5 с.

Раньше блоки считались строго по одному через `asyncio.to_thread`, а очередь
подготовки запускает по таску на проект — из-за GIL несколько проектов
«Выполняется» одновременно, но реально считало одно ядро: бэкенд держал 85 % CPU
из 1600 % доступных, интервал между блоками доходил до 5–22 с при чистых 1,5 с.

Сейчас разбор идёт в пуле **процессов** (`builder._get_pool`):

- пул один на процесс бэкенда и общий для ВСЕХ проектов очереди — иначе
  N проектов × M воркеров вынесли бы машину;
- размер: `BLOCK_CONTEXT_WORKERS`, по умолчанию `min(8, ядра - 2)`;
  `BLOCK_CONTEXT_WORKERS=1` возвращает прежний последовательный режим;
- метод старта — `spawn` (fork из многопоточного uvicorn рискует дедлоком).
  Поэтому любая точка входа, доходящая до стадии, обязана быть под
  `if __name__ == "__main__":` — иначе дочерний процесс переисполнит скрипт;
- порядок блоков сохранён: результаты отдаются строго в порядке `index.json`,
  поэтому артефакты, счётчики и прогресс «N/M блоков» не меняются;
- fail-soft: смерть воркера (`BrokenExecutor`) гасит пул и досчитывает остаток
  в потоке — стадия не падает; пул поднимается заново после рестарта бэкенда.

Замер на 24 блоках АР: 17,1 с → 2,5 с (×6,8, вместе со стартом воркеров).
Артефакты `block_vector_graphs/*.json` при 1 и 8 воркерах побайтово идентичны.

## LM Studio Runtime Policy

- runtime pipeline does not `load` / `reload` / `unload` LM Studio models while
  Stage 00/01/02 is running
- base 100 DPI and high-detail 300 DPI do not reload the Gemma model between
  passes
- backend concurrency for high-detail may drop to `1`, but that is not the same
  thing as changing LM Studio model `parallel`
- post-queue cleanup may unload allowlisted Gemma models only after all queues
  are idle and grace period passes
