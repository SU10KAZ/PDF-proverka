# Stage Refactor — Stabilization Report

**Дата:** 2026-05-06
**Pass:** 14 (stabilization pass after passes 4–13)

---

## Итоговая структура `backend/app/pipeline/stages/`

```
stages/
  block_analysis/
    runner.py          ← run_block_analysis_findings_only + all helpers
    gemma_findings_only.py
  crop_blocks/
    runner.py          ← run_crop_blocks, run_policy_recrop
  findings_merge/
    runner.py          ← run_findings_merge + backfill helpers
    backfill_highlights.py
  findings_review/
    runner.py          ← run_findings_review (critic + corrector, chunked, WS)
  gemma_enrichment/
    runner.py          ← run_gemma_enrichment_stage
    gemma_enrich.py
    gemma_enrichment_contract.py
    gemma_gate.py
  norms/
    runner.py          ← run_norm_verification + enrich/fix/count helpers
    _core.py
    _native_verify.py
    external_provider.py
  optimization/
    runner.py          ← run_optimization, run_optimization_review
  prepare/
    runner.py          ← run_prepare
  report/
    runner.py          ← run_excel_report
  text_analysis/
    runner.py          ← run_text_analysis (standard + triage, rate-limit retry)
```

---

## Thin methods в manager.py

| Метод | Runner |
|-------|--------|
| `_run_gemma_enrichment_stage` | `gemma_enrichment.runner.run_gemma_enrichment_stage` |
| `_run_block_analysis_findings_only` | `block_analysis.runner.run_block_analysis_findings_only` |
| `_run_findings_review` | `findings_review.runner.run_findings_review` |
| `_run_norm_verification` | `norms.runner.run_norm_verification` |
| `_run_optimization` | `optimization.runner.run_optimization` |
| `_run_optimization_review` | `optimization.runner.run_optimization_review` |
| `_run_prepare` | `prepare.runner.run_prepare` |
| inline crop calls | `crop_blocks.runner.run_crop_blocks / run_policy_recrop` |
| inline text_analysis (×3) | `text_analysis.runner.run_text_analysis` |
| inline findings_merge (×2) | `findings_merge.runner.run_findings_merge` |
| inline excel (×3) | `report.runner.run_excel_report` |

---

## Orchestration methods — остаются в manager.py

- `_run_smart_pipeline` — smart parallel audit (triage + tile + findings)
- `_run_ocr_pipeline` — основной OCR pipeline
- `_run_resumed_pipeline` — resume с произвольного stage
- `_run_batch_queue` — batch queue оркестратор
- `_run_precrop_loop` — batch precrop loop
- `_run_block_retry` — retry механизм с fallback логикой
- `_run_post_findings_parallel` — параллельный critic + norms + optimization
- `_run_tile_audit` — legacy batch tile audit
- `_run_main_audit` — legacy main audit
- `_ensure_gemma_ready_or_run` — gate helper
- `_assert_gemma_ready_for_stage` — prerequisite check
- `_ensure_stage02_crops` — Stage 02 crop management

---

## Smoke-проверки — статус

| Проверка | Результат |
|----------|-----------|
| `python -m compileall backend -q` | **OK** |
| Smoke import всех runner-ов | **OK** |
| Backend startup (uvicorn :8081) | **OK** (был уже запущен) |
| `GET /` | 200 OK |
| `GET /model-control` | 200 OK |
| `GET /static/js/app.js` | 200 OK |
| `GET /api/projects` | 200 OK — возвращает список проектов |
| `GET /api/audit/{id}/resume-info` | 200 OK — `migration_required: true` для legacy |
| `GET /api/audit/{id}/status` | 200 OK — `is_running: false` |
| Frontend `npm run build` | **OK** — 1 module, 108 KB |
| `backfill_highlights` smoke | **PASS** — `{fixed: 1, checked: 1}` |
| Bare imports check | **OK** — нет webapp/blocks/process_project imports |
| `sys.path.insert` | только в `norms/_native_verify.py` (внешний venv) |
| Runtime data paths | `backend/app/data/` (корректно) |

---

## JSON output check (проект `133_23-ГК-ЭГ`)

| Файл | Статус | Детали |
|------|--------|--------|
| `01_text_analysis.json` | OK | keys: stage, text_source, text_findings, normative_refs_found |
| `02_blocks_analysis.json` | OK | block_analyses: 49 |
| `03_findings.json` | OK | findings: 15 |
| `03_findings_review.json` | OK | reviews: 15 |
| `norm_checks.json` | OK | checks: 10 |
| `pipeline_log.json` | OK | 3 keys |

---

## Исправления в pass 14 (stabilization)

1. **`gemma_enrichment/runner.py`** — `_project_rel_path` исправлен: импорт
   `PROJECTS_DIR` перенесён из `project_service` в `config` (правильный источник).
2. **`context.py`** — обновлён docstring (устаревшая ссылка на "pass 4").

---

## Остаточные долги

- `norms/__init__.py` — bare relative import; ROOT_DIR в sys.path работает, но лучше явный пакет
- `_run_tile_audit`, `_run_main_audit` — legacy batch методы; определить судьбу перед удалением
- `_run_post_findings_parallel` — сложная asyncio.gather схема; рефакторить отдельно
- `_backfill_highlight_regions` в manager.py — статический метод-алиас (OK, работает)
- **critic/corrector improvement** — отдельный pass; текущий critic уже в `findings_review/runner.py`
- **Smoke-прогон с реальным LLM** — тест полного пайплайна на реальном проекте

---

## Вывод

Stage Refactor завершён успешно. manager.py сокращён с 7277 до 4747 строк (−35%).
Все stage runner-ы изолированы и импортируются без ошибок.
Backend и frontend работают. API отвечает корректно.

**Можно переходить к отдельной задаче critic improvement.**
