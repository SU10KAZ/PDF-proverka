# Блоки и stage 02 (пакетный анализ)

## Пакетный анализ блоков

```
blocks.py crop → blocks/ + index.json → blocks.py batches → block_batches.json
  → N Claude-сессий → blocks.py merge → 02_blocks_analysis.json
```

**Правило:** основная сессия аудита читает готовый `02_blocks_analysis.json`, а НЕ блоки напрямую.

## Production profile (закреплено 20–21.04.2026, КЖ5.17)

- `render_profile = r800` (`MIN_LONG_SIDE_PX = 800`, `TARGET_DPI = 100`)
- `claude_batch_profile = baseline` (heavy 5/6, normal 8/8, light 10/10)
- `parallelism = 3` (production winner: `baseline_p3`)
- `safe_fallback = r800 + baseline_p2` (parallelism=2 при rate-limit)
- Источник истины: `blocks.STAGE02_PRODUCTION_PROFILE`, `blocks.get_stage02_production_profile()`
- Decision doc: [docs/stage02_production_profile.md](stage02_production_profile.md)

## Claude Vision batching (`claude_risk_aware`)

Классификация блоков по metadata (без повторного OCR):
`is_full_page` / `quadrant` / `merged_block_ids` / `size_kb` / `render_size` / `ocr_text_len` / `crop_px`.

- Целевые размеры пакета: heavy ≈ 5 (max 6), normal ≈ 8, light ≈ 10
- **Hard cap = 12 блоков/пакет** (никакой compact-путь не пробивает)
- Параллелизм Claude CLI: default 3, hard cap 3 (`get_block_batch_parallelism()` в [webapp/config.py](../webapp/config.py))
- OpenRouter (Gemini/GPT): `MAX_PARALLEL_BATCHES = 5`, см. `MODEL_BATCH_LIMITS`

## ENV overrides (только для экспериментов)

Production defaults не меняют. Hard cap 12 не пробивается ничем.

- `CLAUDE_BATCH_{HEAVY,NORMAL,LIGHT}_{TARGET,MAX}` — риск-профиль
- `CLAUDE_BLOCK_BATCH_PARALLELISM` — clamp ≤ 3
- `BLOCK_RENDER_MIN_LONG_SIDE`, `BLOCK_RENDER_TARGET_DPI` — для resolution-эксперимента

## A/B матрица Claude stage 02

```bash
# dry-run (только план батчей)
python scripts/run_claude_block_batch_matrix.py --pdf "..." --dry-run

# полный matrix (3 профиля × 3 parallelism)
python scripts/run_claude_block_batch_matrix.py --pdf "..."

# подмножество
python scripts/run_claude_block_batch_matrix.py --pdf "..." --only-profile baseline --parallelism 2

# финальное сравнение (baseline_p3 full + aggressive_p3 full + одинаковый fixed subset)
python scripts/run_claude_block_batch_matrix.py --pdf "..." --final-comparison
# опции: --subset-size 60, --subset-file PATH
```

**Артефакты:** `<project>/_experiments/block_batch_final/<ts>/` или `block_batch_ab/<ts>/`
(`fixed_subset_block_ids.json`, `summary.{json,csv,md}`, `winner_recommendation.md`,
`gate_report.json`, `subset_side_by_side.md`, `subset_divergence_report.md`)

**Rule-based winner gate:** full-run (coverage / missing / failed) → stability (unreadable / parse_errors)
→ quality на subset (findings ≥95%, bwf ≥95%, kv ≥90%) → speed.
Aggressive побеждает только если прошёл все gates И быстрее baseline.
Gate 3 инвалидируется при rate-limit (coverage subset < 50% + fast-fails < 10s) и заменяется fallback full-run quality (95%/95%).

**Изоляция:** runner НЕ перетирает основной `_output`; каждый run в `runs/<run_id>/shadow/` с симлинком на `_output/blocks/` (no recrop).

## Resolution A/B (ось — `MIN_LONG_SIDE_PX`)

```bash
python scripts/run_block_resolution_matrix.py --pdf "..." --dry-run
python scripts/run_block_resolution_matrix.py --pdf "..." --single-block-subset
python scripts/run_block_resolution_matrix.py --pdf "..." --single-block-subset --full-validation
python scripts/run_block_resolution_matrix.py --pdf "..." --single-block-subset --reuse-subset PATH
```

- Ось: `MIN_LONG_SIDE_PX ∈ {800, 1000, 1200}` (профили r800/r1000/r1200)
- `TARGET_DPI = 100` и batching зафиксированы — не варьируются
- Production `_output/blocks/` НЕ затирается: каждый профиль кропается в `_experiments/block_resolution_ab/<ts>/crop_roots/<profile>/blocks/`
- Артефакты: `crop_semantics_report.md`, `crop_stats_by_profile.{json,csv,md}`, `subset_summary.{json,csv,md}`, `subset_divergence_report.md`, `gate_report.json`, `resolution_recommendation.md`

**Subset gate:** hard requirements (coverage 100%, no missing/dup/extra, no unreadable regression)
**И** хотя бы одно quality-улучшение (findings ≥105% ИЛИ median_kv ≥110% ИЛИ empty_kv ≤80% ИЛИ empty_summary ≤80%)
**И** batch-cost sanity (planned batches +≤20%, median_batch_kb +≤50%).

**Full-validation gate:** coverage 100%, unreadable не хуже, findings ≥95%, blocks_with_findings ≥95%.

## Параметры кропинга (`blocks.py crop`)

- `TARGET_LONG_SIDE_PX = 1500` — оптимальный размер для Claude
- `MIN_BLOCK_AREA_PX2 = 50000` — фильтр мелких блоков и штампов
- Масштабирование 1.0–8.0×

## Почему блоки, а не тайлы

| Параметр | Тайлы (старый) | Блоки (новый) |
|----------|----------------|---------------|
| Токенов на изображения | ~300K | ~58K (5× меньше) |
| Информационная плотность | Низкая | Высокая |
| Контекст | Фрагмент сетки | Целый чертёж |
