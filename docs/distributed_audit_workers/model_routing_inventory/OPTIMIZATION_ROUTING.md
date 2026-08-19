# 05 Оптимизация + C OPT Critic + F OPT Fix — внутренняя механика

Точка входа: `backend/app/pipeline/stages/optimization/runner.py:133`
→ `optimization/ensemble.py:476 run_optimization_ensemble()`.
Оба пресета кладут `ensemble/claude-codex-opt` → **схема 05 в двух пресетах идентична**.
Различается только C OPT Critic.

---

## Фактическая схема

```
             один снимок входных артефактов
  (_snapshot_inputs копирует В КАЖДУЮ папку ноги:
   02_text_analysis.json, 01_blocks_analysis.json,
   03_findings.json, document_graph.json)
                        │
        ┌───────────────┴───────────────┐
        │      asyncio.gather(2)        │
        ▼                               ▼
  [нога Claude]                   [нога Codex]
  OPTIMIZATION_ENSEMBLE_          OPTIMIZATION_ENSEMBLE_
  CLAUDE_MODEL = claude-opus-5    CODEX_MODEL = codex/gpt-5.6-sol
  provider = claude_cli           provider = codex_cli
  claude -p, tools OPTIMIZATION_  codex exec, reasoning_effort = xhigh
  TOOLS, пишет свой               (OPTIMIZATION_ENSEMBLE_CODEX_
  optimization.json в claude/     REASONING_EFFORT), + вложения:
                                  collect_optimization_visual_context()
                                  прикладывает PNG чертёжных блоков
                                  (AUDIT_CODEX_OPTIMIZATION_IMAGES, вкл.)
        │                               │
        └───────────────┬───────────────┘
                        ▼
      merge_optimization_documents() — ЧИСТЫЙ PYTHON, БЕЗ модели
      · токенная похожесть current/proposed/spec_items (Jaccard + containment)
      · семейства действий ACTION_PATTERNS, совпадение type, пересечение страниц
      · duplicate только при строгих порогах либо точном совпадении текста
      · неоднозначные пары НЕ схлопываются → possible_duplicates_kept
      · перенумерация OPT-001…; provenance found_by=[claude|codex|оба]
                        ▼
            optimization.json + optimization_merge_report.json
            (+ optimization_claude.json / optimization_codex.json — сырьё)
                        ▼
   ждёт corrector_done (Верификатор) — manager.py:4640
                        ▼
      [C OPT Critic]  модель = get_stage_model("optimization_critic")
        пресет A: claude-sonnet-5 (claude -p, OPTIMIZATION_REVIEW_TOOLS)
        пресет B: codex/gpt-5.4  (_run_codex_json_stage → optimization_review.json)
                        ▼
      Python-аугментация критика (OPTIMIZATION_CRITIC_DETERMINISTIC=true):
      run_deterministic_critic_augment — структурные вердикты
      (no_traceability, basis-aware unrealistic_savings) + вердикт КАЖДОМУ
      предложению, включая неотрецензированные при обрыве агентного критика
                        ▼
              total_issues == 0 ?  → Corrector skipped
                        ▼
      [F OPT Fix]  OPTIMIZATION_CRITIC_DETERMINISTIC=true →
      run_deterministic_corrector (Python), НИЧЕГО не удаляет,
      неотрецензированное сохраняет как pass.
      **Агентная ветка (claude-sonnet-5 / codex/gpt-5.4) НЕ достигается —
      return стоит выше вызова claude_runner.run_optimization_corrector.**
                        ▼
              restore_ensemble_provenance() → авторство Claude/Codex
```

---

## Ответы на вопросы задания

| Вопрос | Ответ |
|---|---|
| Кто выполняет первый анализ | `claude-opus-5` через Claude CLI |
| Кто второй | `codex/gpt-5.6-sol` через `codex exec`, reasoning effort `xhigh` |
| Параллельно ли | Да, один `asyncio.gather` (ensemble.py:502) |
| Что получает каждая модель | Один и тот же замороженный снимок 4 артефактов, скопированный в отдельную папку каждой ноги. Отличие: **Codex дополнительно получает PNG чертёжных блоков**, Claude — нет |
| Кто объединяет | Python `merge_optimization_documents` |
| Модельное ли объединение | **Нет, детерминированное.** Модель на объединении не голосует |
| Кто реально делает C OPT Critic | пресет A — `claude-sonnet-5`; пресет B — `codex/gpt-5.4`; плюс Python-аугментация в обоих |
| Кто реально делает F OPT Fix | **Никакая модель.** Python-корректор при `OPTIMIZATION_CRITIC_DETERMINISTIC=true` |

---

## Разбор «противоречия» из §8 задания

Наблюдение: на экране алгоритма — «C OPT Critic → Codex GPT-5.4», «F OPT Fix → Codex GPT-5.4»,
а в таблице пресета «Claude+GPT+Codex» стоит Sonnet 5.

Причина найдена в коде, это **не рассинхрон текста и не разные версии UI**:

* Панель «Алгоритм этапа» строится функцией `optimizationAlgorithm()` (app.js:4247)
  **динамически** из `stageEnsembleDetails.optimization`, которое приходит с backend:
  `GET /api/audit/model/stages` → `judge_model = STAGE_MODEL_CONFIG["optimization_critic"]`,
  `fix_model = STAGE_MODEL_CONFIG["optimization_corrector"]` (audit.py:148-149).
  То есть панель показывает **сохранённое на сервере** состояние.
* Таблица в модалке показывает `stageModelConfig.value` — локальный, ещё **не сохранённый**
  результат клика по пресету.
* Сохранённое сейчас состояние (`stage_models.json`) = Full Codex → `codex/gpt-5.4`.
  Отсюда «Codex GPT-5.4» на схеме при выбранном, но не сохранённом пресете A.

**Вердикт: DYNAMIC.** Источник истины для схемы — backend `STAGE_MODEL_CONFIG`;
источник истины для таблицы — несохранённое состояние формы. После нажатия
«Запустить аудит» (оно же Save) обе величины сходятся.

**Но остаётся настоящая проблема, независимая от этого:** обе величины врут про
F OPT Fix, потому что при `OPTIMIZATION_CRITIC_DETERMINISTIC=true` там нет модели вообще.
