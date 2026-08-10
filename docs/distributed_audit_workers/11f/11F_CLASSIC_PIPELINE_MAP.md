# 11F — карта классического audit pipeline и граница worker/center

Источник истины — **код** на базовом коммите `896f1d2d`, а не документация.
Машиночитаемая версия со ссылками на файлы и строки: `11F_CLASSIC_PIPELINE_MAP.json`.

## Главный вывод: второго конвейера нет

`remote_audit_runner` не содержит ни одной стадии. Он выставляет корни данных,
сверяет снимки конфигурации и зовёт тот же `PipelineManager._dispatch_action`,
что и центр. Всё, что ниже, исполняет один и тот же `_run_ocr_pipeline`.

```
audit_worker/audit_runner.py::run_audit_job
  → python -m backend.app.pipeline.remote_audit_runner <run_spec.json>
    → PipelineManager._dispatch_action(item, job)
      → manager._run_ocr_pipeline   (manager.py:5336 — фактический порядок вызовов)
```

## Фактическая цепочка и классификация

| # | Стадия | Вызовов модели | Класс |
|---|--------|----------------|-------|
| 1 | `crop_blocks` (+ `document_graph_v2`) | 0 | DETERMINISTIC_PORTABLE |
| 2 | `block_context` (истор. имя `gemma_enrichment`) | 0 | DETERMINISTIC_PORTABLE |
| 3 | `block_grounding` | 0, флаг OFF | DETERMINISTIC_PORTABLE |
| 4 | `block_analysis` | **1 на блок** × число «ног» | **WORKER_TARGET** |
| 5 | `text_analysis` | 1 | **WORKER_TARGET** (доказан 11D.2) |
| 6 | `findings_merge` | 1 | **WORKER_TARGET** (доказан 11E.1) |
| 7 | `findings_review` (`findings_critic`+`findings_corrector`) | 0 детерминированно, **условно 1+** в «страже отсутствия» | **WORKER_TARGET** |
| 8 | `critic_v2_triage` | 0 в дефолте | DETERMINISTIC_PORTABLE |
| 9 | `optimization` | 1 | **WORKER_TARGET** |
| 10 | `optimization_review` (`optimization_critic`+`optimization_corrector`) | до 2 (корректор условен) | **WORKER_TARGET** |
| 11 | `norm_verify` | несколько + MCP | **CENTER_ONLY** |
| 12 | `debt_control` | есть | **CENTER_ONLY** |
| 13 | `decision_carryover` | есть | **CENTER_ONLY** |
| 14 | `excel` | 0 | **CENTER_ONLY** |

`NEEDS_RESEARCH` не осталось: каждая стадия классифицирована по коду.

## Почему четыре стадии — CENTER_ONLY

- **`norm_verify`** — читает центральную нормативную базу (`norms_db.json`,
  `norms_paragraphs.json`), **пишет** в глобальный реестр отсутствующих норм
  (`missing_norms_vault.json` в `APP_DATA_DIR` центра) и требует norms-MCP.
  Перенос означал бы либо копию центральной БД на воркер, либо запись воркера
  в глобальное состояние — оба запрещены заданием.
- **`debt_control`** — сквозной реестр замечаний между версиями документа.
- **`decision_carryover`** — вердикты эксперта прошлых версий и `decisions_log`
  из `knowledge_base`.
- **`excel`** — сводка по **всем** проектам центра.

## Двойной барьер против центральных стадий

1. **До прогона** — `manager._central_stage_blocked` отказывает по переменной
   `AUDIT_PIPELINE_CENTRAL_STAGES_DISABLED`; попытка запуска = отказ стадии.
2. **После прогона** — `remote_audit_runner.audit_stage_history` сверяет журнал
   и валит сборку пакета, если центральная стадия всё же исполнилась.

Граница объявлена в самом коде (`WORKER_STAGE_PLAN`, `FORBIDDEN_STAGES` в
`remote_audit_runner.py`) и совпадает с целевой схемой задания. 11F не
проектирует её заново — проверяет исполнением.

## Что пришлось починить, чтобы участок реально пошёл

Три стадии из восьми на базовом коммите **не умели** ходить через
ProviderAdapter, и это выяснилось чтением кода, а не документации:

- **`block_analysis`** — `call_claude_cli_for_block` шёл прямым
  `create_subprocess_exec` мимо моста и выдавал модели `--allowedTools Read,Write`,
  то есть свободный доступ к файловой системе воркера ради чтения одного PNG.
  Плюс production-дефолт `ensemble/gpt-codex` на воркере недостижим (Codex
  запрещён §35, OpenRouter требует платного ключа).
- **`optimization` / `optimization_review`** — провайдерской ветки не было
  вовсе: claude-ветка ждёт, что модель **сама запишет** `optimization.json`
  инструментом `Write`, а под мостом инструментов ноль.
- **`findings_review` → `absence_guard`** — прямой `subprocess claude` мимо
  моста с политикой fail-soft: на воркере CLI неавторизован → стадия **тихо**
  деградировала бы, выглядя успешной.

Новый слой меняет транспорт и распределение обязанностей, а не бизнес-логику:
промпты берутся у боевых сборщиков, схемы ответа — из боевых констант.

## Параллелизм

`_run_post_findings_parallel` (manager.py:4701) запускает три задачи разом:
`findings_review`, `norm_verify`, `optimization → optimization_review`.
На воркере `norm_verify` выключен централизацией, остаётся
`findings_review ∥ optimization → optimization_review`.

## Флаги, меняющие порядок и число вызовов

| Флаг | Дефолт | Эффект |
|------|--------|--------|
| `PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED` | ON | блоки до текста; текст читает проекцию блоков |
| `PIPELINE_VERIFIER_ENABLED` | ON | включает `findings_review` |
| `PIPELINE_NORMS_AFTER_MERGE_ENABLED` | OFF | выносит `norm_verify` из параллельной группы |
| `STAGE01_DUAL_REVIEW_ENABLED` | ON | только при `model == ensemble/gpt-codex` |
| `STAGE01_THIRD_LEG_ENABLED` | OFF | +1 вызов на блок |
| `STAGE01_PROTECTION_TABLE_CHECK_ENABLED` | OFF | +1 вызов на блок |
| `STAGE01_DUAL_GAP_SEARCH_ENABLED` | OFF | доп. вызовы |
| `CRITIC_V2_*` | OFF | доп. вызовы |

**Важно для воркера:** переменные `.env` центра до воркера **не доезжают** —
`feature_flags` только сверяется по хэшу. На воркере действуют дефолты кода.
