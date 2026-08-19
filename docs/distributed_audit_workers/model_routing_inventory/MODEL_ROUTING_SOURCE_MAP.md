# Карта источников: где живёт маршрутизация моделей

Дата инвентаризации: 2026-08-10. Ветка: `feature/block-vector-graphs`.
Метод: чтение кода от кнопки пресета до вызова CLI/HTTP. Скриншоты источником истины не считались.

---

## 1. Полный путь конфигурации

```
[UI] frontend/index.html:1778-1787            кнопки «Claude+GPT +Codex» / «Full Codex»
   → applyPreset(key)                         frontend/static/js/app.js:3190
   → modelPresets[key].config                 app.js:3089-3118
   → resolvePresetModelId("__codex_exec__")   app.js:3170  → codexModelId() → id модели provider=codex_cli
   → stageModelConfig.value (локальный ref)   app.js:3053
[SAVE] saveAndStartAudit()                    app.js:3342
   → POST /api/audit/model/stages  {stage: model_id, ...}     app.js:3304
   → POST /api/audit/model/batch-modes {block_batch: mode}    app.js:3305
[API] backend/app/api/routers/audit.py:156-186
   → validate_stage_model_choice()            config.py:569
   → STAGE_MODEL_CONFIG[stage] = model        ГЛОБАЛЬНЫЙ словарь процесса
   → _save_stage_model_config()               → backend/app/data/stage_models.json
[START] POST /api/audit/start/{pid}           моделей в теле НЕТ
[PIPELINE] PipelineManager → stage runner
   → get_stage_model(stage)                   config.py:611 — читает тот же глобальный словарь
   → is_codex_model / is_claude_stage / is_optimization_ensemble_model
[RUNNER] backend/app/services/llm/claude_runner.py
   → _run_cli()                               claude_runner.py:284
       ├ is_codex_model → codex_runner.run_codex_exec  → `codex exec --model <bare>`
       └ иначе          → process_runner.run_command   → `claude -p --model <id>`
   → _run_codex_json_stage()                  claude_runner.py:482  (codex в JSON-режиме, 3 попытки)
   → llm_runner.run_llm()                     OpenRouter HTTP (только если модель не claude-/codex-)
```

### Ключевой вывод о конфигурации

**Пресет — это глобальное состояние процесса backend, а не свойство задания.**
`STAGE_MODEL_CONFIG` — модуль-левел словарь в `config.py:375`. Запуск аудита не несёт
моделей в payload и не делает снапшот в задание. Смена пресета во время работы очереди
меняет модели уже запущенных этапов конвейера (те читают `get_stage_model()` в момент вызова).

Подсказка UI «Full Codex … перед запуском сохраняется snapshot» (app.js:3102) — про
**сохранение конфигурации на диск** (`stage_models.json`), а не про снапшот задания.

---

## 2. Реестр моделей (backend/app/core/config.py:530-537)

| id | label в UI | provider | реальный транспорт |
|---|---|---|---|
| `claude-opus-5` | Opus 5 (CLI) | `claude_cli` | `claude -p --model claude-opus-5` |
| `claude-sonnet-5` | Sonnet 5 (CLI) | `claude_cli` | `claude -p --model claude-sonnet-5` |
| `openai/gpt-5.4` | GPT-5.4 | `openrouter` | HTTPS OpenRouter, ключ `OPENROUTER_API_KEY` |
| `codex/gpt-5.4` (`CODEX_STAGE_MODEL_ID`) | Codex | `codex_cli` | `codex exec --model gpt-5.4` |
| `ensemble/gpt-codex` | GPT + Codex | `ensemble` | псевдо-модель, разворачивается в этапе 01 |
| `ensemble/claude-codex-opt` | Claude + Codex (OPT) | `optimization_ensemble` | псевдо-модель, разворачивается в этапе 05 |

`CODEX_STAGE_MODEL_ID` = `codex/${AUDIT_CODEX_MODEL или "gpt-5.4"}` (config.py:448-449).
В `.env` переменной нет → **`codex/gpt-5.4`**.

Колонки таблицы UI = `visibleStageModels` (app.js:3140) — из реестра выброшены
`codex_cli`, `ensemble`, `optimization_ensemble`. Остаются 3 колонки радио
(Opus 5 / Sonnet 5 / GPT-5.4) + отдельная колонка-чекбокс **Codex**.

### Что значит чекбокс «Codex» (app.js:3246 `toggleStageCodex`)

* этап `block_batch` + базовая модель `openai/gpt-5.4` → `ensemble/gpt-codex`;
* этап `optimization` + базовая `claude-opus-5` → `ensemble/claude-codex-opt`;
* любой другой этап → `codexModelId()` = `codex/gpt-5.4` (радио перестаёт быть отмеченным).

То есть «GPT-5.4 + Codex» в строке 01 — это **не** два независимых поля конфигурации,
а один составной id `ensemble/gpt-codex`.

---

## 3. Файлы, которые реально решают, какая модель отработает

| Область | Файл | Что определяет |
|---|---|---|
| Пресеты UI | `frontend/static/js/app.js:3076-3118` | `BASE_STAGE_MODEL_CONFIG`, `modelPresets` |
| Схема алгоритм-панели | `app.js:4188-4290` | что нарисовано в «Алгоритм этапа» (динамика от backend) |
| Реестр/дефолты | `backend/app/core/config.py:266-278, 530-552` | `_STAGE_MODEL_DEFAULTS`, `AVAILABLE_MODELS`, restrictions |
| Персист | `backend/app/data/stage_models.json` | переживает рестарт, грузится без валидации |
| Диспетчер стадий | `backend/app/services/llm/claude_runner.py` | codex / claude / openrouter по `get_stage_model` |
| Этап 01 | `backend/app/pipeline/stages/block_analysis/gemma_findings_only.py:2122-2300` | ноги ансамбля, судья, gap-search |
| Судья 01 | `backend/app/pipeline/stages/block_analysis/dual_review.py` | сравнение и gap-search, модель `STAGE01_DUAL_REVIEW_MODEL` |
| Этап 05 | `backend/app/pipeline/stages/optimization/ensemble.py` | две ноги + детерминированный merge |
| Верификатор | `backend/app/pipeline/stages/findings_verify/runner.py` | **игнорирует** stage-модели |
| Страж отсутствия | `backend/app/pipeline/stages/text_analysis/absence_guard.py:189-235` | жёстко `claude -p --model get_claude_model()` |
| Нормы | `backend/app/pipeline/stages/norms/runner.py` | native Python + LLM только по условиям |
| Привязка пунктов | `backend/app/pipeline/stages/norms/clause_binding_runner.py:43-50` | модель = `get_stage_model("norm_verify")` |

---

## 4. Живые флаги окружения, меняющие состав моделей

Из корневого `.env` (загружается `config.py:11 load_dotenv()`):

| Переменная | Значение | Влияние на маршрутизацию |
|---|---|---|
| `STAGE01_THIRD_LEG_ENABLED` | `true` | **третья нога этапа 01** |
| `STAGE01_THIRD_LEG_MODEL` | (нет) → `codex/gpt-5.6-sol` | модель третьей ноги |
| `STAGE01_DUAL_REVIEW_ENABLED` | (нет) → `true` | судья 01 включён |
| `STAGE01_DUAL_REVIEW_MODEL` | `codex/gpt-5.6-sol` | **судья — не gpt-5.4** |
| `STAGE01_DUAL_GAP_SEARCH_ENABLED` | `true` | gap-search включён, картинка уходит судье |
| `STAGE01_PROTECTION_TABLE_CHECK_ENABLED` | (нет) → `false` | детерминированная 4-я нога выключена |
| `AUDIT_STAGE02_CODEX_PARALLELISM` | `2` | блоков одновременно |
| `OPTIMIZATION_CRITIC_DETERMINISTIC` | `true` | **F OPT Fix = Python, 0 вызовов модели** |
| `NORM_CLAUSE_BINDING_ENABLED` | `true` | **доп. LLM-шаг в этапе 04** |
| `AUDIT_CODEX_TARGETED_FINDINGS` | (нет) → вкл. | доп. codex-проходы после свода (только codex-путь) |
| `PAID_API_ENABLED` / `PAID_API_DAILY_LIMIT_USD` | `true` / `0` | платная нога GPT разрешена, дневной лимит выключен |
| `PIPELINE_VERIFIER_ENABLED` | (нет) → `true` | Верификатор работает |
| `CRITIC_V2_LLM_ENABLED` | `false` | critic_v2 без модели |
| `PIPELINE_NORMS_AFTER_MERGE_ENABLED` | `true` | нормы — последовательно после debt_control |

**Вывод:** без чтения `.env` матрицу восстановить нельзя. Пресет задаёт 11 значений,
а фактический состав вызовов определяют пресет **и** ~10 переменных окружения.

---

## 5. Текущее сохранённое состояние (backend/app/data/stage_models.json)

```json
{"text_analysis":"codex/gpt-5.4","block_batch":"ensemble/gpt-codex",
 "findings_merge":"codex/gpt-5.4","findings_critic":"codex/gpt-5.4",
 "findings_corrector":"codex/gpt-5.4","norm_verify":"codex/gpt-5.4",
 "norm_fix":"codex/gpt-5.4","norm_requote":"codex/gpt-5.4",
 "optimization":"ensemble/claude-codex-opt","optimization_critic":"codex/gpt-5.4",
 "optimization_corrector":"codex/gpt-5.4"}
```

Побайтово совпадает с пресетом **Full Codex**. Продовая установка сейчас на нём.
