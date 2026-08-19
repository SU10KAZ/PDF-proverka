# Аудит текущей архитектуры перед внедрением распределённых audit-worker

**Дата:** 2026-08-07
**Статус:** аналитический отчёт, **кода нет**, рабочий код не изменялся
**Ветка на момент начала аудита:** `feature/block-vector-graphs`, HEAD `b4afae5f`
**HEAD на момент завершения:** `bdc5c87f` — параллельная сессия закоммитила 9 коммитов во время исследования (см. §0.1). Аудитор коммитов не делал.

> Все утверждения ниже подтверждены чтением реального кода с указанием пути, функции/класса и строки.
> Там, где факт не удалось подтвердить, стоит явная пометка **НЕ НАЙДЕНО / НЕИЗВЕСТНО**.
> Значения секретов (токены, ключи, cookie, файлы авторизации) не читались и не приводятся — только имена переменных.

---

## 0.1. Состояние репозитория во время аудита

На старте: ветка `feature/block-vector-graphs`, HEAD `b4afae5f8ed13ab609558457f737498660861ba1`, рабочее дерево содержало 53 незакоммиченные позиции (22 изменённых файла + 31 неотслеженный).

Во время аудита **параллельная сессия** закоммитила эти изменения: HEAD стал `bdc5c87f0a15aced0b5ef766d96d911d44b0b016` («feat(аудит): изолированный бенчмарк перепроверки отклонённых замечаний»), рабочее дерево стало чистым. Проверено: `git merge-base --is-ancestor b4afae5f bdc5c87f` → истина, между ними 9 коммитов, 29 файлов, +3531/−373 строк — линейное продолжение той же ветки.

Это важно для трактовки отчёта: часть цитат снята при HEAD `b4afae5f` (с рабочим деревом), часть — при `bdc5c87f`. Расхождений в разобранных подсистемах не обнаружено: коммиты касались бенчмарка перепроверки, витрины вектор-графов, `prefer_source_pdf` в сравнении стадий и помесячного override лимита платного API — ни один не менял `pipeline/manager.py`, `process_runner.py`, `claude_runner.py`, `ws/manager.py`.

**Аудитор не выполнял:** `git commit`, `add`, `reset`, `clean`, `checkout`, `stash`. Единственная запись на диск — этот файл и его папка.

---

## 1. Резюме текущей архитектуры

Платформа — **монолитный однопроцессный FastAPI-бэкенд**, который одновременно является веб-порталом, диспетчером очереди и исполнителем пайплайна. Точка входа: `uvicorn backend.app.main:app --host 127.0.0.1 --port 8081` ([scripts/server/start_server.sh:46](../../scripts/server/start_server.sh#L46)); наружу — nginx на `auditmanager.app` с Let's Encrypt и проксированием WebSocket ([scripts/server/nginx/auditmanager.app.conf](../../scripts/server/nginx/auditmanager.app.conf)).

Ключевые характеристики, определяющие всю работу по распределённости:

1. **Диспетчер и исполнитель — один и тот же объект в одном процессе.** `PipelineManager` — модульный синглтон, создаваемый на импорте: `pipeline_manager = PipelineManager()` ([backend/app/pipeline/manager.py:6871](../../backend/app/pipeline/manager.py#L6871)). Он же держит очередь, он же порождает asyncio-задачи, он же запускает подпроцессы `claude`/`codex`.

2. **Состояние задания живёт в памяти процесса.** `active_jobs`, `_tasks`, `_heartbeat_tasks`, `_pause_event`, `_rate_limit_deadline`, `_enqueue_lock` — всё в `PipelineManager.__init__` ([manager.py:290-319](../../backend/app/pipeline/manager.py#L290-L319)). Реестр живых подпроцессов — отдельный модульный словарь `_active_processes` ([backend/app/services/common/process_runner.py:68](../../backend/app/services/common/process_runner.py#L68)).

3. **Артефакты — обычные JSON/JSONL на локальной ФС.** Базы данных нет вообще: grep по `sqlite3|psycopg|SQLAlchemy|asyncpg|redis` в `backend/app` даёт 0 совпадений. Это и главное ограничение, и главное преимущество для выноса: пакет проекта — просто дерево каталогов.

4. **Единственный путь запуска — через очередь.** Все `start_*` только ставят элемент в очередь; исполнение идёт через `_dispatch_action` ([manager.py:5998](../../backend/app/pipeline/manager.py#L5998)). Очередь персистится атомарно в `batch_queue.json` ([manager.py:473 `_persist_queue`](../../backend/app/pipeline/manager.py#L473)).

5. **LLM исполняются как локальные CLI-подпроцессы** с ambient-авторизацией в `$HOME`: `claude -p` ([backend/app/services/llm/claude_runner.py:187 `_build_cmd`](../../backend/app/services/llm/claude_runner.py#L187)) и `codex exec` ([backend/app/services/llm/codex_runner.py:442](../../backend/app/services/llm/codex_runner.py#L442)). Ни один токен не передаётся через env — это **структурно совпадает** с требованием «секреты авторизации не передавать центру».

6. **Прогресс доставляется push-ом по WebSocket без буфера и без подтверждений** ([backend/app/ws/manager.py](../../backend/app/ws/manager.py), класс `ConnectionManager`, состояние — списки в памяти). Единственное, что переживает разрыв, — файлы на диске исполнителя: `pipeline_log.json` и `audit_log.jsonl`.

**Главный вывод резюме:** архитектура распадается на «оркестрацию» и «исполнение» гораздо чище, чем можно ожидать от 6871-строчного менеджера, потому что этапы уже развязаны через `PipelineStageContext` ([backend/app/pipeline/context.py:20](../../backend/app/pipeline/context.py#L20)), а определение точки возобновления — чистая функция от каталога проекта ([backend/app/pipeline/resume_detector.py:30](../../backend/app/pipeline/resume_detector.py#L30)). Основные препятствия — не в пайплайне, а в **разделяемом глобальном состоянии** (база знаний, база норм, учёт лимитов) и в **абсолютных путях внутри артефактов**.

---

## 2. Полная цепочка запуска одного аудита

### 2.1. Схема цепочки

```
UI (frontend/static/js/app.js)
  → POST /api/audit/{project_id}/full-audit   (audit.py:902)
  → pipeline_manager.start_audit()            (manager.py:4853)
  → _enqueue_single()                         (manager.py:6308)  ← под _enqueue_lock
  → _ensure_batch_worker()                    (manager.py:6220)  → asyncio.Task "__BATCH__"
  → _run_batch_queue()                        (manager.py:5570)
  → _batch_slot_worker()  |  _run_batch_slot_pool()   (5752 | 5685)
  → _dispatch_action()                        (manager.py:5998)  ← ЕДИНАЯ точка диспетчеризации
  → _run_ocr_pipeline()                       (manager.py:5107)  ← полный аудит
       → этапы (см. §3)
  → job.status = COMPLETED, _promote_completed_audit_v2()  (manager.py:5330-5331)
```

### 2.2. Пошаговая таблица

| # | Шаг | Файл : функция | Вход | Выход | Где состояние | Локальная ФС | Память процесса |
|---|---|---|---|---|---|---|---|
| 1 | Клик в UI | `frontend/static/js/app.js:3498 saveAndStartAudit` / `:3650 startBatchAction` | `{project_ids, action}` | HTTP-запрос | — | нет | `liveStatus`, `selectedProjects` в браузере |
| 2 | HTTP-эндпоинт | `backend/app/api/routers/audit.py:902 start_audit` | `project_id`, `version_id` | `{status, job}` | — | гейт `_check_project` (audit.py:1177) проверяет наличие локального PDF версии | — |
| 3 | Постановка в очередь | `manager.py:4853 start_audit` → `:6308 _enqueue_single` | `project_id`, `action="full"` | `AuditJob(status=QUEUED)` | `_batch_queue` + `batch_queue.json` | `BATCH_QUEUE_FILE` (config.py:295) | `_batch_queue`, `_enqueue_lock` |
| 4 | Фиксация версии | `version_service.resolve_effective_version_id` (вызов manager.py:6332) | `project_id` | `version_id` (напр. `v002`) | `BatchQueueItem.version_id` | чтение `document.json`/`current_version.txt` | — |
| 5 | Подъём воркера очереди | `manager.py:6220 _ensure_batch_worker` | queue | `asyncio.Task` в `_tasks["__BATCH__"]` | память | — | `_tasks`, `active_jobs["__BATCH__"]` |
| 6 | Цикл слотов | `manager.py:5570 _run_batch_queue` → `5752 _batch_slot_worker` | queue | захват item, `status="running"` | `batch_queue.json` (persist после каждого item, `:5954`) | да | `_enqueue_lock`, `_batch_slots_wake` |
| 7 | Диспетчеризация | `manager.py:5998 _dispatch_action` | `BatchQueueItem`, `AuditJob` | вызов `_run_*` | pipeline_log | `kill_all_processes` + `_clean_stage_files` | `active_jobs[pid]`, `_tasks[pid]` |
| 8 | Резолв путей задания | `manager.py:1459 _resolve_job_paths` | `AuditJob` | `(root_dir, version_dir, output_dir)` | — | **да**: v2-primary → `03_analysis/runs/<job_id>` ([v2_primary_wiring.py:203](../../backend/app/services/storage/v2_primary_wiring.py#L203)) | ContextVar `bind_object`/`bind_version` |
| 9 | Контекст этапа | `manager.py:1627 _make_stage_context` | `AuditJob` | `PipelineStageContext` | — | `project_dir`, `output_dir` | 14 callback-ов, замкнутых на менеджер |
| 10 | Этапы конвейера | см. §3 | артефакты предыдущего | артефакты следующего | `_output` / `runs/<job_id>` | да | `job.stage`, `job.progress_*` |
| 11 | Подпроцессы скриптов | `manager.py:1404 _run_script_for_job` → `process_runner.py:153 run_script` | argv + env `AUDIT_*` | `(exit_code, stdout, stderr)` | — | `cwd=BASE_DIR`, `sys.executable` | `_active_processes[pid]` |
| 12 | Вызовы LLM | `claude_runner.py:292 _run_cli` / `codex_runner.py:407 run_codex_exec` | task_text через stdin | `CLIResult` | audit_trail | `/tmp/sonnet_clean/run_*`, `-C ROOT_DIR` | `resource_budget` семафоры |
| 13 | Журналирование | `backend/app/services/common/audit_logger.py:143 update_pipeline_log` | stage_key, status | `pipeline_log.json` (атомарно) | **диск** | `_output/pipeline_log.json` | `_STAGE_RUN_STARTS` (длительность) |
| 14 | Лог прогона | `audit_logger.py:363 persist_log` | строка лога | `audit_log.jsonl` (append) | **диск** | да | — |
| 15 | Прогресс наружу | `audit_logger.py:414 send_progress`, `manager.py:2977 _heartbeat_loop` | current/total | WS-сообщение | **только память/сеть** | нет | `job.last_heartbeat`, `batch_durations` |
| 16 | Публикация в latest | `manager.py:1940 _promote_completed_audit_v2` | `runs/<job_id>/*` | копии в `03_analysis/latest/` | диск | да, копирование в пределах одной ФС | — |
| 17 | Отдача в UI | `audit.py:658 get_all_live_status` (поллинг 15 с) + WS | — | `{running, batches, usage, paused}` | память + скан диска | обход `iter_project_dirs()` | `active_jobs` |

### 2.3. Что критично в этой цепочке для распределённости

- **Шаги 5-7 неразделимы сегодня**: захват элемента очереди и его исполнение происходят в одной корутине. При `BATCH_MAX_PARALLEL <= 1` элемент исполняется буквально внутри корутины `__BATCH__` ([manager.py:5601](../../backend/app/pipeline/manager.py#L5601)) — от этой идентичности зависят `cancel()` ([manager.py:1353](../../backend/app/pipeline/manager.py#L1353)) и `cleanup_zombies` ([manager.py:1189](../../backend/app/pipeline/manager.py#L1189)).
- **Шаг 8 — естественная граница пакета**: всё, что ниже, работает от `(root_dir, version_dir, output_dir)` и не знает, на каком хосте эти каталоги.
- **Шаг 15 — единственный шаг без персистентности.** Прогресс и heartbeat нигде не сохраняются на диск (проверено grep-ом `heartbeat` по `backend/`: только `manager.py`, `models/audit.py`, `models/websocket.py`, `audit.py`).

---

## 3. Схема текущего пайплайна

### 3.1. Канонический порядок полного аудита (`_run_ocr_pipeline`, manager.py:5107)

```
[1]  crop_blocks         → blocks_stage02_100/index.json + block_*.png
     _seed_prepared_inputs_from_latest (2156) → _ensure_stage02_crops (2217)
[2]  document_graph v2   → document_graph.json                (manager.py:1787)
[3]  block_context       → block_context_summary.json + block_vector_graphs/*.json
     (исторический ключ "gemma_enrichment", manager.py:1961)
[4]  block_grounding     → block_grounding_summary.json       (флаг OFF по умолчанию)
     ─── развилка PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED (в проде = true) ───
[5]  block_analysis      → 01_blocks_analysis.json            (Stage 01/02, ансамбль ног)
[6]  text_analysis       → 02_text_analysis.json
[7]  findings_merge      → 03_findings.json
     + _run_verdict_rehydration (2601) — ПИШЕТ в глобальный decisions_log.json
[8]  _run_post_findings_parallel (4530) — ПАРАЛЛЕЛЬНО:
        ├── findings_verify («Верификатор») → 03_findings_review.json
        ├── norm_verify                     → norm_checks.json, 03a_norms_verified.json
        └── optimization → optimization_critic/corrector → optimization*.json
     синхронизация через asyncio.Event corrector_done (manager.py:4557)
[9]  debt_control        → migrated_findings_report.json      (читает ПРЕДЫДУЩУЮ версию)
[10] norm_verify         последовательно, если PIPELINE_NORMS_AFTER_MERGE_ENABLED
[11] decision_carryover  → decision_carryover_report.json     (ПИШЕТ в decisions_log.json)
[12] excel               → 05_export/audit_report_<pid>_<job_id>.xlsx
[13] COMPLETED → _promote_completed_audit_v2 (1940) → 03_analysis/latest/
```

### 3.2. Реестра этапов нет

Важное наблюдение для проектирования: **словаря `stage → runner` в коде не существует**. Резолв — прямыми ветвлениями `if/elif` в `_dispatch_action` ([manager.py:6089-6217](../../backend/app/pipeline/manager.py#L6089-L6217)). При этом порядок этапов продублирован в **пяти** местах:

| Список | Файл : строка | Назначение |
|---|---|---|
| `AuditStage` (Enum) | [backend/app/models/audit.py:8-27](../../backend/app/models/audit.py#L8-L27) | типы |
| фактический порядок | `manager.py:5107` (тело `_run_ocr_pipeline`) | полный аудит |
| `ocr_stages` | [manager.py:3511-3521](../../backend/app/pipeline/manager.py#L3511-L3521) | resume; **индексы зашиты числами** (`start_idx >= 4`, `== 5`, `== 6`) |
| `_PIPELINE_STAGE_ORDER_KEYS` | [audit_logger.py:79-93](../../backend/app/services/common/audit_logger.py#L79-L93) | каскадный сброс downstream |
| `_PIPELINE_STAGE_ORDER` | `backend/app/services/common/project_service.py:1832-1848` | UI |

Это — долг, который **не нужно чинить ради воркеров**, но о котором нужно знать: любая правка порядка требует синхронной правки пяти мест.

### 3.3. Артефакты этапов (что считается «сделано»)

Resume-детектор ([resume_detector.py:30](../../backend/app/pipeline/resume_detector.py#L30)) определяет готовность по двум приоритетам:
1. **`pipeline_log.json`** — первый этап со статусом `error`/`interrupted`; статус `running` намеренно исключён ([resume_detector.py:272-278](../../backend/app/pipeline/resume_detector.py#L272-L278)) как признак живого прогона;
2. **наличие файлов**: `crops_materialized()` ([block_context/contract.py:126](../../backend/app/pipeline/stages/block_context/contract.py#L126) — проверяет каждый PNG на размер ≥ 1024 байт), `02_text_analysis.json`, `01_blocks_analysis.json`, `03_findings.json`, `norm_checks.json`, `03a_norms_verified.json`.

**Это ключевое свойство для воркера:** определение точки возобновления не требует ни памяти процесса, ни сети, ни очереди — только каталог проекта.

---

## 4. Состав переносимого пакета проекта

### 4.1. Реальная раскладка версии (projects_v2, режим прода)

Прод работает в режиме `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary` и `AUDIT_STORAGE_BACKEND=projects_v2` (проверено чтением `.env`; значения этих двух переменных — режимы, не секреты). Каталог `projects/` в корне **не существует** — вся работа в `projects_v2/`.

```
projects_v2/objects/<объект>/disciplines/<ДИСЦ>/documents/<код>/versions/v00N/
  01_input/     ← НЕИЗМЕНЯЕМЫЙ исходник портала + input_manifest.json (sha256 по каждому файлу)
  02_work/      ← нормализованные копии: document.pdf, document.md, ocr.html, result.json, blocks.json
  03_analysis/
      latest/   ← актуальные артефакты + block_vector_graphs/ + blocks_stage02_100/
      runs/<job_id>/  ← прогоны: полный набор + кропы + audit_trail/ + _stage02_paid_response_cache/
  04_review/    ← вердикты эксперта (expert_review.json)
  05_export/    ← Excel
  99_service/   ← audit_log.jsonl, pipeline_log.json, block_batches.runtime.json
  version.json
```

⚠️ **Раскладка неоднородна между версиями — проверено `ls` на реальных данных.** У версии `13АВ-РД-ЭО-К3/v002` в `03_analysis/latest/` нет ни `pipeline_log.json`, ни `03a_norms_verified.json`, ни `blocks_stage02_100/` — они лежат в `99_service/`. А у версии `СТ26_01-14-АР0-АС-1-РД_V1/v002` папки `99_service/` **нет вообще**, и всё перечисленное лежит в `latest/`. Практическое следствие: **манифест пакета нельзя строить по фиксированному списку путей — только сканированием дерева версии**.

Масштаб корпуса: **477 документов, 559 версий, 199 016 файлов, 32 ГБ** в `projects_v2/objects` (`du`, `find`). Отдельно в `projects_v2/_system` лежит ещё 414 каталогов `versions` (shadow-зеркало миграции, 3,2 ГБ) — они в пакет не входят, но их надо учитывать при оценке диска. Размер одной версии: медиана **30 МБ**, p90 **125 МБ**, p95 **170 МБ**, максимум **637 МБ**. Типовой пакет — **100–300 МБ**.

### 4.2. Классификация содержимого пакета

| Категория | Что именно | Везти на воркер? | Обоснование (код) |
|---|---|---|---|
| **Обязательные исходники** | `01_input/*.pdf`, `*_document.md`\|`*_results.md`, `*_ocr.html`\|`*_results.html`, `*_result.json`\|`*_blocks.json`, `input_manifest.json` | **ДА** | суффиксы: [projects_v2_source_resolver.py:16-25](../../backend/app/services/storage/projects_v2_source_resolver.py#L16-L25); MD обязателен — `_require_project_md` ([manager.py:1575](../../backend/app/pipeline/manager.py#L1575)) бросает `RuntimeError`, fallback на `extracted_text` удалён (`prompt_builder.py:323`) |
| **Рабочие копии** | `02_work/{document.pdf, document.md, ocr.html, result.json, blocks.json}` | **ДА** (или пересоздать) | создаются `version_service._sync_v2_work_copies` (1383-1442) байтовым копированием; `document.pdf` **обязателен** — без него ре-рендер кропов невозможен |
| **Метаданные версии** | `version.json`, `project_info.json`, `document.json`, `current_version.txt` | **ДА** | `section` определяет дисциплину → промпты и профили; `_load_project_info_for_paths` ([manager.py:1521](../../backend/app/pipeline/manager.py#L1521)) |
| **Кропы (PNG)** | `blocks_stage02_100/`, `index.json` | **опционально** | восстановимы офлайн: `block_crop_store.resolve_block_image` ([block_crop_store.py:355](../../backend/app/services/common/block_crop_store.py#L355)), порядок `["local_pdf", "crop_url"]`; экономия 15–420 МБ на версию |
| **Промежуточные результаты** | `03_analysis/latest/*` (и/или `99_service/`, см. выше) | **ДА** | во-первых, `resume_detector` читает именно их; во-вторых — `_seed_prepared_inputs_from_latest` ([manager.py:2156](../../backend/app/pipeline/manager.py#L2156)) и `_seed_run_dir_from_latest` ([:2092](../../backend/app/pipeline/manager.py#L2092)) копируют кропы, индексы и `block_context_summary` из `latest` в свежий `runs/<job_id>` перед `_ensure_stage02_crops`. **Без `latest` первый прогон на воркере полезет скачивать все кропы с портала заново** |
| **Настройки аудита** | `stage_models.json`, `stage_batch_modes.json`, профиль флагов `.env` | **ДА, как часть задания** | `STAGE_MODELS_FILE` ([config.py:307](../../backend/app/core/config.py#L307)); прод целиком на codex-ногах, а дефолты в коде — на Claude Opus ([config.py:266-278](../../backend/app/core/config.py#L266-L278)) → без файла воркер прогонит **не на тех моделях** |
| **Шаблоны и промпты** | `prompts/pipeline/{ru,en}/*.md`, `prompts/disciplines/<КОД>/*` (152 файла, 1,1 МБ) | **ДА, снапшотом** | читаются по `PROMPTS_DIR` ([config.py:55](../../backend/app/core/config.py#L55), env-override `AUDIT_PROMPTS_DIR`); **редактируются из UI** (`task_builder.py:348 save_template`) → снапшот обязателен, иначе центр и воркер разойдутся |
| **Справочный каталог** | `backend/app/pipeline/stages/block_context/reference_catalog/` (4,2 МБ, 9 дисциплин) | **ДА** (едет с кодом) | `loader.py:16 CATALOG_DIR = Path(__file__).parent` — часть кода, в git |
| **Нормативная база** | `norms/` — 6,6 ГБ (venv 4,9 + `paragraphs_embeddings.npz` 1,6 + vault 89 МБ + `status_index.json`) + HF-модели `~/.cache/huggingface` 4,3 ГБ | **см. §9 — решение архитектурное** | `norms/external_provider.py:29-32`, `norms/tools/norms_api.py:31`, `_native_verify.py:29-33`; **бо́льшая часть вне git** (`.gitignore:98-103`) |
| **Предыдущая версия** | `03_findings.json` + `expert_review.json` версии N−1 | **ДА, если V2+** | `decision_carryover_service.py:191 previous_checked_version` и `:140 _load_findings`; `migrated_findings_service.py:1624` |
| **НЕЛЬЗЯ передавать** | `.env` целиком (8 имён секретов), `~/.claude/.credentials*`, `~/.codex/auth*`, `PORTAL_SESSION_SECRET`, `PORTAL_AUTH_USERS`, `OPENROUTER_API_KEY` | **НЕТ** | по условию задачи и по здравому смыслу: авторизация CLI ambient в `$HOME`, воркеру нужен только свой профиль флагов |
| **Не нужно (центральное)** | `batch_queue.json`, `objects.json`, `users.json`, `usage_data.json`, `paid_cost*.json`, `knowledge_base/decisions_log.json` (26 МБ) | **НЕТ** | это учёт и координация центра, см. §5 |
| **Пересоздаваемое** | `_stage02_paid_response_cache/`, LRU `cache/block_crops` (ключ = `sha256(realpath)`, [block_crop_lru.py:69-75](../../backend/app/services/common/block_crop_lru.py#L69-L75)), `/tmp/sonnet_clean/*` | **НЕТ** | кэш промахнётся после переезда, но не сломается |

### 4.3. Можно ли упаковать в ZIP/TAR без потери работоспособности

**Да, с тремя оговорками — все три проверены эмпирически.**

**(1) Хардлинки.** В `projects_v2/objects` **36 673 файла из 199 016 (18 %) имеют `nlink > 1`**, из них 34 932 — `block_*.png` (результат `scripts/projects_v2/dedupe_block_crops.py`). Проверено потоком:

```
$ tar -cf - blocks_gemma_100/block_X.png blocks_stage02_100/block_X.png | tar -tvf -
-rw-rw-r-- … blocks_gemma_100/block_X.png
hrw-rw-r-- … blocks_stage02_100/block_X.png link to blocks_gemma_100/block_X.png
```
→ **GNU tar 1.35 хардлинки сохраняет** (тип записи `h`); Python `tarfile` тоже (карта `self.inodes`). **ZIP не сохраняет** — формат не имеет типа записи «жёсткая ссылка», в исходнике `zipfile` слово `hardlink` не встречается. `rsync` без `-H` и `shutil.copytree` тоже рвут.
Практический эффект: версия `13АВ-РД-ЭО-К3/v002` — `du` 63 МБ, `du -l` 88 МБ → **ZIP раздувается на +40 %**. Хуже: у части версий партнёр хардлинка лежит **вне** версии (в `_system/destructive_backups/`) — там tar не поможет, экономия исчезнет в любом случае.
**Вывод: использовать TAR (GNU), а не ZIP.**

**(2) Абсолютные пути внутри артефактов.** Проверено грепом и разбором JSON:

| Артефакт | Поле | Пример |
|---|---|---|
| `01_blocks_analysis.json` | `stage01_meta.runtime_plan_path` | `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/…` |
| `block_context_summary.json` | `project_dir` | `/home/coder/projects/PDF-proverka/projects_v2/objects/…` |
| `pipeline_log.json` | `artifacts_dir` | `/home/coder/…/runs/caa2b574-…/critic_v2_assisted_round2` |
| `optimization_merge_report.json` | `output_dir` | `/home/coder/…/_optimization_ensemble/…` |
| `gemma_enrichment_summary.json` | `md_path`, `backup_path` | абсолютные |
| `version.json` / `document.json` | `legacy_folder_path`, `legacy_project_path` | указывают в **уже несуществующий** `projects/` |

**Чисто (0 вхождений `/home/coder`):** `03_findings.json`, `document_graph.json`, `blocks_*/index.json`.

Показательно: часть этих путей **уже мертва на текущем хосте** (ведёт в снесённый `projects/`), и система это переживает — значит они не критичны для чтения. Но `pipeline_log.artifacts_dir` и `optimization_merge_report.output_dir` — это адреса, **куда пишут этапы**, и их придётся либо переписывать при распаковке, либо делать относительными.

**(3) Внешние URL.** `02_work/blocks.json` содержит 129 ссылок `https://vibe.cloud-ip.cc`, `crops_evicted.json` — `crop_url` на `pub-*.r2.dev`. Живость таких ссылок в корпусе — **85 % (33/39)**, 15 % мертвы ([docs/block_crop_lifecycle.md](../block_crop_lifecycle.md)). Это подтверждает: **везти `02_work/document.pdf` обязательно**, полагаться на портал нельзя.

### 4.4. Что уже помогает переносимости

- **Все корни данных имеют env-override**: `AUDIT_ROOT_DIR`, `AUDIT_DATA_DIR`, `AUDIT_PROJECTS_DIR`, `AUDIT_PROJECTS_V2_DIR`, `AUDIT_APP_DATA_DIR`, `AUDIT_PROMPTS_DIR`, `AUDIT_OBJECTS_FILE`, `AUDIT_ACTION_LOG_DIR`, `AUDIT_BLOCK_CROP_CACHE_DIR` ([config.py:30-36, 44-55, 283-314, 1154, 1197-1201](../../backend/app/core/config.py#L30-L36)). **На проде ни один из них не задан** — всё работает на автодетекте от каталога кода. То есть механизм переносимости уже написан, просто не задействован.
- **`input_manifest.json` содержит sha256 + размер по каждому исходному файлу** — готовая проверка целостности пакета.
- **`crops_evicted.json` хранит путь к PDF ОТНОСИТЕЛЬНЫМ** (`"pdf": "02_work/document.pdf"`, [block_crop_store.py:561](../../backend/app/services/common/block_crop_store.py#L561)) — единственная корректно переносимая ссылка в хозяйстве; благодаря ей кропы восстанавливаются офлайн.
- **Атомарная запись** через tmp + `os.replace` — 102 точки в `backend/app/`, включая `pipeline_log.json` и `batch_queue.json`.
- **Удаление не деструктивно**: версии → `_trash` ([version_service.py:2230-2320](../../backend/app/services/common/version_service.py)), кропы → `.evicted/`.

---

## 5. Глобальные зависимости, которых нет внутри пакета

Это **центральная тема аудита**: пайплайн во время прогона читает и **пишет** данные вне папки проекта.

### 5.1. Разделяемое состояние, в которое пайплайн ПИШЕТ

| Ресурс | Размер | Кто пишет во время прогона | Механизм записи | Риск при N воркерах |
|---|---|---|---|---|
| `knowledge_base/decisions_log.json` | **26,5 МБ** | `decision_carryover` ([decision_carryover_service.py:682-689](../../backend/app/services/findings/decision_carryover_service.py#L682-L689)) и `verdict_rehydration` ([manager.py:2601](../../backend/app/pipeline/manager.py#L2601) → `verdict_preservation.py`) через `knowledge_base_service.save_expert_review` ([:220](../../backend/app/services/knowledge_base/knowledge_base_service.py#L220)) | read-modify-write **всего файла**. ⚠️ Защита неоднородна: `load_modify_save` ([atomic_json.py:85-92](../../backend/app/services/common/atomic_json.py#L85-L92)) берёт `fcntl.flock`, а `atomic_write_json` ([:50-58](../../backend/app/services/common/atomic_json.py#L50)) — **только `threading.Lock`, без flock**; именно им пишется `_save_json(DECISIONS_LOG_FILE, …)` ([knowledge_base_service.py:641](../../backend/app/services/knowledge_base/knowledge_base_service.py#L641)) | 🔴 **КРИТИЧНО**: flock и так локален для хоста, а часть записей не защищена даже между процессами одного хоста |
| `norms/norms_paragraphs.json` | 110 КБ | `norm_verify` → `merge_llm_norm_results` (`norms/_core.py:845`) → `merge_paragraph_checks` (`:1294`) → `save_norms_paragraphs` (def `:1131`, вызов `:1337`) | полная перезапись | 🔴 потеря проверенных цитат |
| `backend/app/data/missing_norms_vault.json` | 7,7 КБ | `norm_verify` → `missing_norms_service.accumulate_from_queue` ([:177](../../backend/app/services/knowledge_base/missing_norms_service.py#L177)) | перезапись под `threading.Lock` | 🟠 потеря очереди недостающих норм |
| `backend/app/data/paid_cost.json` + `paid_cost_events.jsonl` | 27 КБ + 136 КБ | каждый платный вызов (`llm_runner.py:1266`, `manager.py:2523`) | RMW + append, `PaidCostTracker._save` ([usage_service.py:1258-1266](../../backend/app/services/common/usage_service.py#L1258)) — tmp+`replace` **без flock** | 🔴 `PAID_API_DAILY_LIMIT_USD` перестаёт быть глобальным |
| `backend/app/data/usage_data.json` | 611 КБ | `usage_tracker.record_usage` (`manager.py:1032`) | полная перезапись | 🟠 расщепление учёта токенов |
| `logs/actions/actions-YYYY-MM-DD.jsonl` | 12 МБ | `action_log.log_pipeline_event` (хук в `audit_logger.py:242`) | append, суточная ротация | 🟢 append-only → пересылаемо |

### 5.2. Данные, которые пайплайн только ЧИТАЕТ

| Ресурс | Размер | В git? | Кто читает |
|---|---|---|---|
| `prompts/` (152 файла) | 1,1 МБ | **да**, но **пишется из UI** | все LLM-этапы; `config.py:73-88` |
| `reference_catalog/` | 4,2 МБ | да | `block_context`; `loader.py:46 load_reference_records` (`@lru_cache`) |
| `norms/tools/status_index.json` (695 норм) | 508 КБ | **нет** (`.gitignore:101`) | `norms/external_provider.py:29-32` — **единственный authoritative источник статусов** |
| `norms/vault/` (492 MD) | 89 МБ | **нет** (`.gitignore:98`) | `norms/tools/norms_api.py:31` — тексты пунктов |
| `norms/tools/paragraphs_embeddings.npz` | **1,6 ГБ** | **нет** | семантический поиск |
| `norms/tools/venv/` | **4,9 ГБ** | **нет** (`.gitignore:99`) | `_native_verify.py:97-102` подмешивает в `sys.path` бэкенда |
| HF-модели `~/.cache/huggingface` | **4,3 ГБ** | вне репо | e5-large + bge-reranker-v2-m3 |
| `stage_models.json` | 427 Б | **нет** (`.gitignore:171`) | модель КАЖДОГО этапа |
| `backend/app/data/discipline_checklists/` | — | да | `checklist_loader.py:22` |
| вендор-лист `<объект>/DOC/вендор лист.md` | — | — | `optimization` ([task_builder.py:1340](../../backend/app/pipeline/stages/prepare/task_builder.py#L1340)) — **per-object, вне версии** |
| `knowledge_base/decisions_log.json` | 26,5 МБ | **нет** (`.gitignore:15`) | `critic_v2/kb_retriever.py:19`, `decision_carryover` |

**Итого «холодная» установка воркера ≈ 11 ГБ, из которых почти всё вне git.** Пересобрать из клона нельзя: `status_index.json` строится из `vault/`, а `vault/` в gitignore.

### 5.3. Найденный дефект окружения (важен для планирования)

Разведка воспроизвела на текущем хосте: **norms-venv сломан**.
```
norms/tools/venv/pyvenv.cfg: version = 3.12.3, executable = /usr/bin/python3.12
norms/tools/venv/bin/python3 → /usr/bin/python3 → python3.14 (3.14.4)
$ norms/tools/venv/bin/python -c "import mcp"  → ModuleNotFoundError
```
То есть `mcp__norms__*` через `.mcp.json` сейчас **не поднимается**, а предохранитель `assert_norms_mcp_available()` ([codex_runner.py:114-130](../../backend/app/services/llm/codex_runner.py#L114-L130)) проверяет только существование файла-симлинка и пропускает сломанный интерпретатор. При этом Python-путь верификации норм работает, потому что бэкенд крутится на `/opt/py312` и подхватывает те же `site-packages` через `sys.path`-инжект.

Это **не блокирует** внедрение воркеров, но означает: (а) на воркере норм-venv надо ставить осознанно, (б) guard стоит усилить проверкой `import mcp`, (в) любой апгрейд системного python тихо ломает норм-MCP.

### 5.4. Абсолютные пути в конфигурации

- `.mcp.json` в корне репозитория содержит **абсолютный** путь:
  `"command": "/home/coder/projects/PDF-proverka/norms/tools/venv/bin/python"` — на воркере с другим корнем норм-MCP для Claude-этапов не поднимется. (Для codex пути строятся динамически от `ROOT_DIR`, [codex_runner.py:39-40](../../backend/app/services/llm/codex_runner.py#L39-L40).)
- `backend/app/data/objects.json` хранит **абсолютный** `projects_dir` на каждый из 5 объектов; все 5 путей указывают в несуществующий `projects/` (проверено `os.path.isdir()` = False). Без этого файла `_ensure_default_object` молча создаст новый объект с новым uuid → расхождение идентичностей при возврате пакета.
- `CLAUDE_SESSIONS_DIR = Path.home()/".claude"/"projects"` ([config.py:733](../../backend/app/core/config.py#L733)) — **без env-override**; используется для учёта лимитов подписки.
- Шаблон `prompts/pipeline/ru/norm_verify_task.md:7` ссылается на `/home/coder/projects/Norms/` — каталога **не существует**; фактически используется in-repo `norms/tools/status_index.json`. Косметический долг.


### 5.5. Ещё три контура заданий помимо PipelineManager

Критик полноты вскрыл важное: `PipelineManager` — **не единственный** владелец фоновых LLM-заданий. Для распределёнки это значит, что «занятость воркера» и «сколько слотов свободно» нельзя считать по одному реестру.

| Контур | Где | Своё состояние | Отношение к воркерам |
|---|---|---|---|
| **Сводная оптимизация раздела** | `services/section_optimization_{service,pipeline_service,agent_service,graphics_agent_service,replication_service}.py`; роутер `optimization.py:80,99,113,142,172,200,225,241` | свой in-memory реестр `_ACTIVE_TASKS` ([section_optimization_pipeline_service.py:33](../../backend/app/services/section_optimization_pipeline_service.py#L33)), запуск `asyncio.create_task` (`:409`), состояние в `APP_DATA_DIR/section_optimization*` | гоняет **Codex** (`agent_service.py:358 run_codex_json_messages`), **не берёт** `resource_budget` и не регистрируется в `_active_processes`. 🔴 **Кросс-проектный по замыслу**: `collect_section_optimization_data()` собирает ВСЕ проекты объекта и фильтрует по `section` — модель «один проект → один воркер» этот контур не обслуживает в принципе |
| **Сравнение стадий** | `services/stage_comparison/pipeline_queue.py` | свои job-файлы (`_jobs_dir:62`, `_write_job:81`), свой health-gate локальной модели (`qwen_health_gate:142`), свои стадии Qwen/Opus | хранилище — **не `projects_v2`**, а отдельный корень `comparison/` (env `COMPARISON_ROOT`, `AUDIT_STAGE_COMPARISON_ROOTS`). Состав пакета из §4 на него не распространяется |
| **Подготовка данных (prepare)** | `pipeline/stages/prepare/prepare_service.py` | свой семафор кропа, свои `pause_event`/`cancel_event` (`:62,70,76`), **свой WS-канал** (`_broadcast_queue:206`, `_ws_log:265`), свои таски (`:497`) | 🔴 **межочередной интерлок**: `_check_not_in_active_batch()` ([:152](../../backend/app/pipeline/stages/prepare/prepare_service.py#L152)) блокирует ручной prepare через `pipeline_manager.is_project_in_active_batch()`. При выносе исполнения этот инвариант становится **межхостовым** и сегодня ничем не обеспечен |

Плюс `services/findings/rejected_audit_service.py` — ещё один потребитель Codex (`run_codex_audit:5478`), не подключённый к роутерам и пишущий в `comparison/`.

**Вывод:** для первого этапа эти контуры остаются на центре целиком (они и так кросс-проектные), но их существование надо учесть в двух местах: (а) расчёт слотов воркера не должен считать их нагрузку своей, (б) интерлок prepare↔батч при удалённом исполнении требует явного решения.

### 5.6. Как проект попадает в систему сегодня — готовое определение «исходника»

Три входа, все — естественные точки сборки пакета:

1. **`POST /api/projects/upload-folder`** ([projects.py:218-275](../../backend/app/api/routers/projects.py#L218) → `save_uploaded_project_folder:2999`) — валидация от path-traversal (`:3042-3052`), белый список расширений `{.pdf,.md,.json,.html,.htm,.zip}` (`:2572`), гарды zip-бомб (`_ZIP_MAX_MEMBERS=500`, `_ZIP_MAX_TOTAL_UNCOMPRESSED=4 ГБ`). Пишет `project_info.json` + **`input_manifest.json`** с per-file sha256.
2. **`POST /api/projects/upload-folder/precheck`** ([:278-315](../../backend/app/api/routers/projects.py#L278)) — dry-run, ничего не пишет, возвращает verdict/fingerprint. **Готовый прототип «предпроверки пакета» перед отправкой на воркер.**
3. **`POST /api/projects/register-external`** ([:199-217](../../backend/app/api/routers/projects.py#L199) → `register_external_project:3275`) — копирует PDF/MD из произвольного пути, `input_manifest.json` **не пишет**.

✅ **Канон «что исходник, а что производное» уже кодифицирован в коде** — `is_source_file()` ([project_service.py:3774-3788](../../backend/app/services/common/project_service.py#L3774)), используется в `clean_project_data:3721`: `*.pdf`, `*.md`, `*_result.json`, `*_annotation.json`, `*_ocr.html|*_results.html|*_results.htm`, `project_info.json`. **Это готовое определение input-части пакета воркера — брать его, а не изобретать список заново.**

⚠️ Уточнение к §4.4: `input_manifest.json` есть **не у всех** версий — 527 файлов на 559 версий, то есть ~32 версии без манифеста (пришли через `register-external` или из миграции). Проверка целостности должна это переживать.

### 5.7. Переключатель источника кропов

`AUDIT_CROP_CACHE_SOURCE` ([crop_cache.py:50-59](../../backend/app/services/common/crop_cache.py#L50)): значение из `{download, portal, url}` → качать с портала, иначе `local_pdf` (дефолт — резать самим). Скачивание `_download_one:112-137` атомарно, гарды 64 МБ/файл и 2 ГБ/документ.

🔴 **Связка с зависимостями:** без PyMuPDF режим `local_pdf` физически невозможен — `pdf_crop._require_fitz()` бросает `PdfCropError`, и офлайн-воркер **молча становится сетевым клиентом портала**. В `01_input/*_blocks.json` хост портала захардкожен (замер: 118 значений `crop_url` с `https://vibe.cloud-ip.cc` в одном файле).

### 5.8. Инвентарь Python-зависимостей неполон — и это уже известно проекту

`requirements.txt` — 12 пакетов. Импортируются, но **отсутствуют**: `PyMuPDF/fitz` (~25 мест в `backend/app`, вся геометрия и кропы), `openpyxl` (Excel-этап, падает внутри subprocess и потому невидим бэкенду), `Pillow`, `numpy`, `PyYAML`, `httpx`.

Проект это знает: `.github/workflows/ci.yml` — **единственный** workflow, в режиме observe-first (`continue-on-error: true`, `:37`), с комментарием «корневой requirements.txt перенесён из webapp/ (2026-07-04) — список deps ниже best-effort», а шаг установки (`:34`) доставляет руками `pillow`, `passlib`, `jinja2`, `httpx`.

**Для воркера:** VPS, собранный строго по `requirements.txt`, тихо теряет локальную вырезку кропов (уходит в сеть) и роняет этап отчёта. Перед развёртыванием нужен честный `requirements-worker.txt`, собранный по фактическим импортам.

### 5.9. Абсолютные пути хоста в метаданных projects_v2

Помимо `.mcp.json` и `objects.json` (§5.4), абсолютные пути лежат **внутри самих метаданных версии**:
- `documents/<код>/document.json` → `legacy_project_path`;
- `versions/vNNN/version.json` → `legacy_folder_path`.

Оба указывают в **уже несуществующий** `projects/`. Читатели: `project_service._resolve_v2_legacy_project_path:1472-1474`, `projects_v2_dual_read.py:111-125`, `project_rename_service.py:178,242`. При переносе на воркер это либо чистить, либо игнорировать явно.

Хорошая новость: `blocks_*/index.json` **переносим** — поля `file`, `crop_px`, `render_size`, `source_result_json` относительные (проверено на реальном index.json).

### 5.10. Хардкод несуществующего deploy-worktree в рабочем коде

[pipeline_v2_runtime_root_audit.py:52-57](../../backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py#L52):
```python
MAIN_COMPARISON_ROOT   = "/home/coder/projects/PDF-proverka/comparison"
DEPLOY_COMPARISON_ROOT = "/home/coder/projects/PDF-proverka-deploy/comparison"
```
и `scripts/check_production_data_roots.sh` с `REPO_ROOT` по умолчанию `/home/coder/projects/PDF-proverka-deploy`.

Проверено: **каталога `PDF-proverka-deploy` на этом хосте нет** (`git worktree list` даёт только main + `.claude/worktrees/new-upload-format-stage1`). То есть диагностика «какой корень читает прод» на новом хосте даст ложную картину — её надо параметризовать перед раскаткой воркеров.

### 5.11. Stage 01 резолвит Claude CLI в обход автодетекта

[gemma_findings_only.py:117](../../backend/app/pipeline/stages/block_analysis/gemma_findings_only.py#L117):
```python
CLAUDE_CLI_BIN = os.environ.get("CLAUDE_CLI_BIN", str(Path.home() / ".local" / "bin" / "claude"))
```
Полноценный поиск (`shutil.which`, расширенный PATH, скан расширений VS Code, `/usr/local/bin`) живёт **только** в `config._find_claude_cli()`. На VPS, где `claude` установлен иначе, **самый тяжёлый этап отвалится**, хотя `/api/info` покажет корректный `claude_cli`. Аналогично `critic_v2/kb_gate.py:48` (там есть fallback).

---

## 6. Точки подключения удалённого исполнения

### 6.1. Главный вывод: точка врезки одна

Из-за того, что **все** пути запуска сходятся в `_dispatch_action` ([manager.py:5998](../../backend/app/pipeline/manager.py#L5998)), а исполнение элемента очереди локализовано в `_batch_slot_worker` ([manager.py:5752](../../backend/app/pipeline/manager.py#L5752)), абстракция `ExecutionBackend` вставляется **ровно в одном месте**:

```
_batch_slot_worker  →  [ExecutionBackend]  →  _dispatch_action (LocalExecutionBackend)
                                          →  RemoteWorkerExecutionBackend (новое)
```

`LocalExecutionBackend.run(item, job)` = сегодняшний вызов `await self._dispatch_action(item, job, ...)` (manager.py:5878) — байт-в-байт прежнее поведение.
`RemoteWorkerExecutionBackend.run(item, job)` = «собрать пакет → отдать воркеру → ждать событий → принять результат → распаковать в `runs/<job_id>`».

### 6.2. Минимальный список точек изменения

| # | Точка | Файл : функция | Изменение | Риск |
|---|---|---|---|---|
| 1 | Выбор бэкенда исполнения | `manager.py:5878` (внутри `_batch_slot_worker`) | `if item.worker_id: await remote.run(...) else: await self._dispatch_action(...)` | низкий — одна ветка |
| 2 | Поле назначения | [models/audit.py:111 `BatchQueueItem`](../../backend/app/models/audit.py#L111) | добавить `worker_id: Optional[str] = None` | **нулевой** — pydantic, дефолт None, старая очередь читается |
| 3 | Приём событий воркера | новый роутер `backend/app/api/routers/workers.py` | POST-эндпоинты регистрации/событий/heartbeat/артефактов | новый файл, ничего не ломает |
| 4 | Ретрансляция в UI | `ws_manager.schedule_broadcast_to_project` ([ws/manager.py:38](../../backend/app/ws/manager.py#L38)) | вызывать из обработчика событий воркера | **нулевой** — метод уже спроектирован для вызова из чужого потока |
| 5 | Запись статуса этапа | `audit_logger.update_pipeline_log` ([audit_logger.py:143](../../backend/app/services/common/audit_logger.py#L143)) | вызывать при приёме `stage_completed` от воркера | низкий — это уже единая воронка (хук ActionLog и WS внутри) |
| 6 | Живость | `cleanup_zombies` ([manager.py:1176](../../backend/app/pipeline/manager.py#L1176)) + `_protected_pids` ([:571](../../backend/app/pipeline/manager.py#L571)) | добавить «есть свежий remote-heartbeat» в защищённые | **средний** — см. §10.2, это самый опасный класс |
| 7 | Отмена | `cancel` ([manager.py:1330](../../backend/app/pipeline/manager.py#L1330)) | для remote — послать команду вместо `kill_all_processes` | средний |
| 8 | Прогресс в API | `get_all_live_status` ([audit.py:658](../../backend/app/api/routers/audit.py#L658)) | добавить поле `worker` в `running[pid]` | низкий |
| 9 | UI | `frontend/index.html` + `app.js` | см. §6.5 | низкий (аддитивно) |

**Итого: ~9 точек, из них 2 с ненулевым риском.** Полная переработка пайплайна не требуется.

### 6.3. Что НЕ нужно трогать

- Все 15 `stages/*/runner.py` — они уже принимают `PipelineStageContext` и не знают о менеджере ([context.py:22-27](../../backend/app/pipeline/context.py#L22-L27): «Не держит ссылку на PipelineManager»).
- `process_runner.py` — это и есть «исполнение», оно целиком уезжает на воркер как есть.
- `resume_detector.py` — чистая функция от каталога, работает на воркере без правок.
- `claude_runner.py` / `codex_runner.py` — локальный запуск CLI, ровно то, что должно жить на воркере.
- `audit_logger.py` — файловая часть переносима; на воркере пишет локально, события уезжают в центр.

### 6.4. Разделение обязанностей PipelineManager

**PipelineManager остаётся главным диспетчером — это правильное решение.** Он уже владеет очередью, паузой, версионностью и согласованием rate-limit; переписывать это ради воркеров незачем.

| Остаётся в PipelineManager (центр) | Выносится в новый WorkerManager / JobDispatcher |
|---|---|
| очередь `_batch_queue` + `batch_queue.json` | реестр воркеров (регистрация, токен, статус, лимиты) |
| пауза/возобновление (`pause`, `unpause`) | выбор воркера под элемент очереди (ручной / авто) |
| фиксация `version_id` и `object_id` на enqueue | сборка и отдача пакета, приём пакета обратно |
| порядок и приоритет элементов | канал событий (приём, дедуп, ретрансляция в WS) |
| `resume_interrupted_batch`, `load_persisted_queue` | remote-heartbeat и признак «воркер жив» |
| локальное исполнение через `LocalExecutionBackend` | подсчёт свободных слотов флота |
| **исключительное право записи** в `decisions_log.json`, `paid_cost.json`, `usage_data.json` | — |

### 6.5. Точки врезки во фронтенде

Живой фронт — `frontend/index.html` (9076 строк) + `frontend/static/js/app.js` (**19 462 строки**, один Vue-компонент, без бандлера); отдаётся статикой с бэкенда, `frontend/dist/` не используется ([main.py:338-346](../../backend/app/main.py#L338-L346)).

| Что добавить | Куда конкретно |
|---|---|
| пункт меню «Воркеры» | `index.html:78-104` (рядом с «Очередь»), роут — `app.js:2747 handleRoute()` |
| карточки воркеров | **готовый прототип**: `frontend/model-control.html:50-63` + `model-control.js:296 renderServerCards` — уже показывает label/URL/health/кнопку |
| колонка «Воркер» в очереди | `index.html:2002-2055` (строка `queue-item`), данные — `app.js:3811 visibleQueueItems` |
| выбор воркера при запуске | модалка `index.html:1758-1817` — через неё проходит **любой** запуск (`app.js:3498 saveAndStartAudit`, `:3620 confirmBatchAction`) |
| «где исполняется» на плитке | `app.js:2370 getProjectLiveInfo` + баннер `index.html:2366-2396` |
| лимиты по воркерам | **готовый прототип**: модалка «Расход подписки по инженерам» `index.html:3128-3190` + `app.js:11106 subSpendLoad` — калька один-в-один, строка = воркер |
| маршрутизация запросов | **готовая точка**: monkey-patch `window.fetch` в `app.js:4727-4747`, который уже добавляет `X-Object-Id` во все `/api/`-запросы — единственное место, где можно централизованно добавить `X-Worker-Id` без правки 116 вызовов `fetch` |

**Сегодня в UI нет никакого понятия «где исполняется»** — проверено grep-ом по `app.js`: `worker|воркер|исполнител|хост` даёт только `job.qwen_worker` из Stage-Comparison (внутренняя дорожка Qwen в том же процессе, не хост).

### 6.6. Что должен запускать воркер: backend или CLI-runner

**Рекомендация: полный backend в «режиме воркера», а не отдельный CLI-runner.**

Обоснование по коду:
- пайплайн опирается на `config.py` (пути, таймауты, модели), `audit_scope` ContextVar, `resource_budget`, `cpu_pool`, `version_service`, `storage_write_facade` — воспроизводить это в отдельном CLI = дублировать половину бэкенда;
- `_run_ocr_pipeline` — метод `PipelineManager`, тесно связанный с `_make_stage_context`, `_seed_*`, `_promote_*`; выдрать его в CLI без рефакторинга нельзя;
- backend уже умеет подниматься автономно (`start_server.sh`) и переживать рестарт (`load_persisted_queue`, `_recover_stale_pipelines`).

**Режим воркера** = тот же код с другим профилем:
- новые env: `AUDIT_ROLE=worker`, `AUDIT_DISPATCHER_URL`, `AUDIT_WORKER_ID`, `AUDIT_WORKER_TOKEN`;
- корни данных через уже существующие `AUDIT_DATA_DIR` / `AUDIT_PROJECTS_V2_DIR` / `AUDIT_APP_DATA_DIR`;
- при `AUDIT_ROLE=worker` **отключить**: приём портальных пользователей, авто-resume чужой очереди (`auto_resume_interrupted_batch`, [main.py:125-127](../../backend/app/main.py#L125-L127)), `_recover_stale_pipelines` по всем проектам ([manager.py:1233](../../backend/app/pipeline/manager.py#L1233) — сканирует ВСЕ проекты хоста), запись в глобальный `decisions_log.json`.

### 6.7. Отделение центрального состояния задания от локального

| Уровень | Кто владеет | Хранение | Идентификатор |
|---|---|---|---|
| **Задание** (что нужно сделать) | центр | `batch_queue.json` + новый `workers.json`/`assignments.json` | `job_id` (уже есть: `BatchQueueItem.job_id`, `AuditJob.job_id`) |
| **Исполнение** (как идёт) | воркер | `pipeline_log.json`, `audit_log.jsonl`, `runs/<job_id>/` | тот же `job_id` — **он уже равен имени run-каталога** ([v2_primary_wiring.py:203](../../backend/app/services/storage/v2_primary_wiring.py#L203)) |
| **Результат** | центр после приёма | `03_analysis/latest/` через `_promote_completed_audit_v2` | `job_id` |

Совпадение `job_id` = имя run-каталога — **счастливая случайность архитектуры**, которая даёт готовую сквозную идентичность прогона между центром и воркером. Использовать её.

⚠️ Уточнение: ветка `runs/<job_id>` активна **только** при `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary` ([manager.py:1478](../../backend/app/pipeline/manager.py#L1478)); при `legacy` выход идёт в `version_dir/_output` ([:1519](../../backend/app/pipeline/manager.py#L1519)). Комментарий в коде (`manager.py:1474-1476`) утверждает «в проде WRITE_MODE=dual_write_shadow → ветка НЕ исполняется» — **этот комментарий устарел**, прод уже на `projects_v2_primary`. Режим хранилища обязан быть частью задания и сверяться на хендшейке.

✅ **Готовый кирпич протокола, который стоит переиспользовать:** `audit_scope.as_env()` ([audit_scope.py:86-98](../../backend/app/services/common/audit_scope.py#L86)) уже отдаёт сериализуемый снимок области аудита `{output_dir, version_dir, project_id, version_id}` — именно потому, что ContextVar не наследуется дочерним процессом. Вместе с `_make_audit_env_for_job` ([manager.py:1432-1440](../../backend/app/pipeline/manager.py#L1432)) это фактически уже написанный «конверт задания»; для воркера он расширяется до `{job_id, worker_id, run_id, storage_mode, config_fingerprint}`.

---

## 7. Что можно переиспользовать без изменений

| Механизм | Файл : функция | Почему годится как есть |
|---|---|---|
| **Определение точки возобновления** | [resume_detector.py:30 `detect_resume_stage`](../../backend/app/pipeline/resume_detector.py#L30) | чистая функция от `output_dir`: `pipeline_log.json` + наличие файлов. Не трогает память менеджера, сеть, очередь. Воркер сам решает, откуда продолжить после обрыва |
| **`pipeline_log.json`** | [audit_logger.py:143 `update_pipeline_log`](../../backend/app/services/common/audit_logger.py#L143) | атомарная запись (tmp+`os.replace`), самодостаточен, едет в пакете, полностью описывает «что сделано». *Оговорка:* это главная, но **не единственная** точка записи — есть ещё два прямых и **неатомарных** писателя: `_enrich_pipeline_log` ([manager.py:1100-1101](../../backend/app/pipeline/manager.py#L1100)) и `_recover_stale_pipelines` ([manager.py:1268](../../backend/app/pipeline/manager.py#L1268)) |
| **`audit_log.jsonl`** | [audit_logger.py:363 `persist_log`](../../backend/app/services/common/audit_logger.py#L363) | append-only per-project — **естественный накопитель событий на время потери связи** |
| **Запуск подпроцессов** | [process_runner.py:153/253](../../backend/app/services/common/process_runner.py#L153) | полностью локальный; `_terminate_with_grace` (SIGTERM→10 с→SIGKILL) решает проблему полузаписанных JSON **на таймауте** (при отмене — сразу SIGKILL, см. §10.1) |
| **Claude/Codex runners** | `claude_runner.py`, `codex_runner.py` | локальная авторизация в `$HOME` — ровно то, что требует ТЗ |
| **Стадии пайплайна** | все 15 `stages/*/runner.py` | принимают `PipelineStageContext`, не знают о менеджере |
| **Контракт этапа** | [context.py:20 `PipelineStageContext`](../../backend/app/pipeline/context.py#L20) | 14 callback-ов + пути; готовый кандидат стать сериализуемым контрактом задания |
| **Атомарная запись** | 102 точки `os.replace` в `backend/app/` | ровно для локальной ФС воркера и писалось |
| **Кэш платных ответов Stage 01** | `stages/block_analysis/stage02_paid_cache.py` | per-project каталог, едет с пакетом → повторный прогон на другом хосте не платит второй раз |
| **Восстановление кропов офлайн** | [block_crop_store.py:355 `resolve_block_image`](../../backend/app/services/common/block_crop_store.py#L355) | лестница `local_pdf → crop_url`; при наличии `02_work/document.pdf` кропы можно **не везти** |
| **Ретраи и ожидание лимитов LLM** | `llm_runner.py:1162-1208`, `text_analysis/rate_limit_retry.py`, `norms/runner.py:602-641` | локальные, не зависят от диспетчера |
| **Косвенное определение остатка лимитов** | [usage_service.py:650 `GlobalUsageScanner`](../../backend/app/services/common/usage_service.py#L650) + `check_rate_limit` ([:1033](../../backend/app/services/common/usage_service.py#L1033)) | **уже делает ровно требуемое**: парсит `~/.claude/projects/*.jsonl`, считает 5-часовое и недельное окно, возвращает `{can_proceed, wait_seconds, usage_pct, resets_in_text}`. На воркере работает как есть — надо лишь отдавать наружу |
| **Парсинг времени сброса лимита** | [cli_utils.py:62 `parse_rate_limit_reset`](../../backend/app/services/common/cli_utils.py#L62) | вытаскивает «resets 11pm (Europe/Moscow)» из вывода CLI → готовый источник «даты сброса лимита». **Оговорка:** MSK зашит как UTC+3 (`cli_utils.py:88-89`) — воркер в другой TZ посчитает неверно |
| **WS-сообщения** | [models/websocket.py `WSMessage`](../../backend/app/models/websocket.py) | 12 типов (`log`, `progress`, `heartbeat`, `status`, `complete`, `error`, `finding_added`, `cli_summary`…) — почти полностью покрывают требуемый формат событий воркера (см. §11) |
| **ActionLog** | [core/action_log.py:86 `log_event`](../../backend/app/core/action_log.py#L86) | суточный append-only JSONL с retention и day-cap — формат годен для пересылки, не хватает только транспорта |
| **Ресурсные метрики** | [model_control_service.py:139 `_system_memory`](../../backend/app/services/llm/model_control_service.py#L139) (psutil RAM/swap/CPU), [main.py:278 `_disk_stats`](../../backend/app/main.py#L278) | готовый сборщик для отчёта воркера о ресурсах |
| **Пакет результатов в ZIP** | [export.py:252 `download_audit_package`](../../backend/app/api/routers/export.py#L252) | рабочий прототип упаковки (project_info + PDF + MD + артефакты конвейера + Excel); переделать на «пакет обратно» проще, чем писать с нуля |
| **Изоляция параллельных проектов** | [audit_scope.py](../../backend/app/services/common/audit_scope.py) (ContextVar вместо `os.environ`), `_ensure_clean_cwd` (tmpdir на вызов) | уже пережило инцидент «артефакты уезжали в чужой проект»; на воркере работает без правок |
| **Инфраструктура HTTPS/WSS** | nginx + Let's Encrypt на `auditmanager.app`, `client_max_body_size 200M`, `proxy_read_timeout 3600s`, WS-upgrade map | готовый транспорт для подключения воркеров |


---

## 8. Что необходимо изменить

Список отсортирован по обязательности. «Обязательно» = без этого распределённый режим не заработает или испортит данные.

### 8.1. Обязательно

| # | Что | Где | Почему |
|---|---|---|---|
| 1 | **Запретить воркеру писать в глобальную базу знаний** | `decision_carryover_service.py:682`, `verdict_preservation.py` → `knowledge_base_service.save_expert_review` ([:220](../../backend/app/services/knowledge_base/knowledge_base_service.py#L220)) | RMW 26-МБ файла под локальным `fcntl.flock` ([atomic_json.py:85-92](../../backend/app/services/common/atomic_json.py#L85-L92)) — два воркера затрут друг друга. Решение: воркер пишет дельту вердиктов в пакет результата, мержит **центр** |
| 2 | **Событийный журнал воркера с курсором доставки** | новый модуль + хук в `audit_logger` | сегодня прогресс идёт только в WS «выстрелил и забыл» ([ws/manager.py:52-57](../../backend/app/ws/manager.py#L52-L57) — при отсутствии loop событие молча теряется). Курсора доставки **НЕ НАЙДЕНО** нигде |
| 3 | **Remote-heartbeat как признак живости** | `cleanup_zombies` ([manager.py:1176](../../backend/app/pipeline/manager.py#L1176)), `_protected_pids` ([:571](../../backend/app/pipeline/manager.py#L571)) | `ZOMBIE_TIMEOUT_SEC = 600` ([manager.py:321](../../backend/app/pipeline/manager.py#L321)); 10 минут сетевого молчания — норма для WAN, а сейчас это = «зомби» → демотация очереди → resume → `_clean_stage_files` удаляет `03_findings.json` (см. §10.2) |
| 4 | **Поле `worker_id` в элементе очереди** | [models/audit.py:111 `BatchQueueItem`](../../backend/app/models/audit.py#L111) | иначе назначение негде хранить; риск нулевой (pydantic-дефолт) |
| 5 | **Реестр воркеров + токены** | новый `backend/app/api/routers/workers.py` + `workers.json` | машинной аутентификации в системе **нет вообще**: только пользовательские логины из env ([portal_auth.py](../../backend/app/core/portal_auth.py)); `external_register.py` — это реестр замечаний заказчика, не воркеров (проверено, 302 строки) |
| 6 | **Снапшот конфигурации в пакете** | `stage_models.json`, `stage_batch_modes.json`, профиль флагов | прод на `codex/gpt-5.4`, дефолты в коде — `claude-opus-5` ([config.py:266-278](../../backend/app/core/config.py#L266-L278)); файл в `.gitignore:171` → чистый клон прогонит **не на тех моделях** |
| 7 | **Снапшот `prompts/` в пакете** | `PROMPTS_DIR`, env `AUDIT_PROMPTS_DIR` | промпты редактируются из UI (`task_builder.py:348`) → без снапшота центр и воркер разъедутся |
| 8 | **Решение по нормативной базе** | см. §9 | 6,6 ГБ + 4,3 ГБ моделей; `norm_verify` **пишет** в `norms_paragraphs.json` |
| 9 | **Отпечаток версии кода/конфига** | сейчас `/api/info` отдаёт захардкоженное `"version": "1.0.0"` ([main.py:316](../../backend/app/main.py#L316)) | сверить «воркер == центр» **нечем**: ни git-хеша, ни тега (`git describe` → «No tags»), CHANGELOG протух 2026-06-17. Нужен составной отпечаток: git sha + хеш профиля флагов + хеш `stage_models.json` + хеш снапшота `prompts/` + версия норм-базы (`status_index.json:meta.indexed_at` — готовое поле) |

| 10 | **Честный список зависимостей воркера** | `requirements.txt` (12 пакетов) vs фактические импорты | без `PyMuPDF` воркер теряет локальную вырезку кропов и уходит в сеть на портал (§5.7-5.8); без `openpyxl` падает Excel-этап внутри subprocess |
| 11 | **Резолв Claude CLI в Stage 01** | [gemma_findings_only.py:117](../../backend/app/pipeline/stages/block_analysis/gemma_findings_only.py#L117) `CLAUDE_CLI_BIN` | на VPS с иным расположением `claude` самый тяжёлый этап отвалится (§5.11) |
| 12 | **Интерлок prepare ↔ батч через границу хостов** | [prepare_service.py:152](../../backend/app/pipeline/stages/prepare/prepare_service.py#L152) `_check_not_in_active_batch` | инвариант «не готовить проект, который сейчас в батче» становится межхостовым (§5.5) |

### 8.2. Желательно

| # | Что | Где | Почему |
|---|---|---|---|
| 10 | Атомарная запись `prepare_queue.json` | [prepare_service.py:218-221](../../backend/app/pipeline/stages/prepare/prepare_service.py#L213-L224) — прямой `write_text` | на воркере, который «переживает жёсткие рестарты», это гарантированный битый файл (в `batch_queue.json` атомарность уже есть) |
| 11 | Атомарная запись при recovery | [manager.py:1268](../../backend/app/pipeline/manager.py#L1268) — прямой `write_text` в `pipeline_log.json` | штатный писатель атомарен, а recovery — нет |
| 12 | Отмена не должна писать `error` | [block_analysis/runner.py:812-815](../../backend/app/pipeline/stages/block_analysis/runner.py#L812-L815) | отмена попадает в `pipeline_issues` → значок «!» на плитке (`index.html:1243`); при распределёнке ложных отмен станет больше |
| 13 | Вынести `resource_budget.snapshot()` и `cpu_pool.pool_info()` в API | функции есть, **наружу не отданы** (проверено grep-ом по роутерам). Оговорка: `snapshot()` возвращает приватное `sem._value` и `"active"` как bool при объявленном типе `dict[str, Optional[int]]` ([resource_budget.py:152-163](../../backend/app/services/common/resource_budget.py#L152)) — перед выставлением наружу привести к нормальной схеме | нужны диспетчеру для расчёта слотов |
| 14 | Убрать sync-работу из `live-status` | [audit.py:658](../../backend/app/api/routers/audit.py#L658) — `cleanup_zombies()` + обход `iter_project_dirs()` + `json.load` каждого `block_batches.json` прямо в event loop | самый частый поллинг (15 с); при N воркерах нагрузка вырастет, а watchdog убивает бэкенд по неответу `/api/info` |
| 15 | Проверять `import mcp` в guard норм | [codex_runner.py:114-130](../../backend/app/services/llm/codex_runner.py#L114-L130) | сейчас проверяется только существование файла-симлинка → сломанный venv проходит (см. §5.3) |
| 16 | TZ-независимый парсинг сброса лимита | [cli_utils.py:88-89](../../backend/app/services/common/cli_utils.py#L88-L89) — MSK зашит как UTC+3 | воркер в другой TZ посчитает время сброса неверно |

### 8.3. Обнаруженный побочный дефект (не связан с воркерами)

`backend/app/api/routers/model_control.py:86` — внутри `_schedule_backend_restart` используется `cwd=str(webapp_dir)`, но имя `webapp_dir` в модуле **не определено и не импортировано** (единственное вхождение — строка 86). Значит `POST /api/model-control/server-profiles/activate` после успешной правки `.env` упадёт с `NameError`, и рестарт не будет запланирован. Фиксирую как находку; чинить в рамках этого аудита не следует.

---

## 9. Предлагаемая граница между центральным сервером и воркером

### 9.1. Граница

```
┌─────────────────── ЦЕНТР (auditmanager.app) ────────────────────┐
│ PipelineManager: очередь, пауза, версии, приоритеты             │
│ WorkerManager:   реестр воркеров, токены, слоты, назначение     │
│ EventIngest:     приём событий, дедуп по (job_id, seq), WS      │
│ Хранилище:       projects_v2 — ЕДИНСТВЕННЫЙ источник истины     │
│ Эксклюзив:       decisions_log.json, paid_cost.json, usage_data │
│ UI:              все экраны, включая новый «Воркеры»            │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTPS/WSS, исходящее соединение ОТ воркера
┌──────────────────────────┴──────────────────────────────────────┐
│ ВОРКЕР (сторонний VPS)                                          │
│ Тот же backend, AUDIT_ROLE=worker, порт наружу НЕ открыт        │
│ Локально: claude CLI + codex CLI (ambient auth в $HOME)         │
│ Данные:  распакованный пакет в AUDIT_PROJECTS_V2_DIR            │
│ Пишет:   pipeline_log.json, audit_log.jsonl, runs/<job_id>/     │
│ Копит:   события в локальный журнал с монотонным seq            │
│ Отдаёт:  события + готовый пакет; хранит пакет 30 дней          │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2. Решение по нормативному этапу — развилка

Это **главное архитектурное решение**, которое нужно принять до реализации.

| | Вариант А: норм-этап на воркере | Вариант Б: норм-этап возвращается в центр |
|---|---|---|
| Установка | +11 ГБ на каждый воркер (venv 4,9 + npz 1,6 + vault 0,09 + HF 4,3) | ничего |
| RAM | +5,6 ГБ на сессию норм-MCP ([resource_budget.py:55-58](../../backend/app/services/common/resource_budget.py#L55-L58)) → сокращает слоты | не влияет |
| Запись в `norms_paragraphs.json` | конфликт между воркерами | безопасно |
| Задержка | нет | +1 сетевой раунд на проект |
| Сложность | развёртывание + хрупкий venv (§5.3) | нужен «частичный возврат»: воркер отдаёт `03_findings.json`, центр догоняет `norm_verify` |
| **Рекомендация** | — | **Б на первом этапе.** Норм-база — самая тяжёлая и самая «пишущая» зависимость; вынести её последней |

Оговорка: вариант Б нарушает принцип «один проект — один воркер целиком». Компромисс: воркер прогоняет весь конвейер **кроме** `norm_verify`, отдаёт пакет, центр выполняет `norm_verify` + `decision_carryover` (оба пишут в глобальные ресурсы) и публикует в `latest`. Это ровно те два этапа, которые и так требуют централизованного состояния.

### 9.3. Протокол (минимальный)

| Направление | Транспорт | Назначение |
|---|---|---|
| воркер → центр | WSS (постоянное) | регистрация, heartbeat, поток событий |
| воркер → центр | HTTPS POST | догон накопленных событий после обрыва (батч с `seq`) |
| воркер → центр | HTTPS POST multipart | готовый пакет результата (TAR) |
| центр → воркер | ответ по WSS | назначение задания, команда отмены, команда удаления пакета |
| центр → воркер | HTTPS GET | скачивание пакета проекта (воркер тянет сам) |

Ключевое: **воркер всегда инициатор**. Порт на воркере наружу не открыт; центр не «ходит» на воркер. SSH — только для установки и ремонта, вне рабочего обмена.


---

## 10. Работа при потере связи

### 10.1. Что уже работает в пользу отказоустойчивости

| Механизм | Файл : функция | Как помогает воркеру |
|---|---|---|
| Resume от файлов | [resume_detector.py:30](../../backend/app/pipeline/resume_detector.py#L30) | воркер сам продолжит с нужного этапа после любого обрыва — данных из центра не требуется |
| Атомарный `pipeline_log.json` | [audit_logger.py:231-234](../../backend/app/services/common/audit_logger.py#L231-L234) | не бьётся при kill -9 |
| `audit_log.jsonl` | [audit_logger.py:363](../../backend/app/services/common/audit_logger.py#L363) | append-only накопитель событий |
| Поблочные артефакты Stage 01 | `_stage01_findings_only_runs/<run>/block_*.json` ([gemma_findings_only.py:2444-2447](../../backend/app/pipeline/stages/block_analysis/gemma_findings_only.py)) | гранулярный прогресс переживает падение процесса |
| Восстановление очереди | [manager.py:620 `load_persisted_queue`](../../backend/app/pipeline/manager.py#L620) | `running → interrupted` при старте |
| Восстановление стадий | [manager.py:1233 `_recover_stale_pipelines`](../../backend/app/pipeline/manager.py#L1233) | `running → interrupted` в `pipeline_log.json` |
| Продолжение вместо перезапуска | `_action_override = "resume"` ([manager.py:5870-5877](../../backend/app/pipeline/manager.py#L5870-L5877)) | комментарий фиксирует инцидент 04.08.2026 — «минус полтора часа и ~$3» при перезапуске с нуля |
| Кэш платных ответов | `stage02_paid_cache.py`, ключ `sha256(model\|block_id\|prompt\|image)` | повтор блока после обрыва — бесплатно |
| Мягкое убийство **по таймауту** | `_terminate_with_grace` ([process_runner.py:109](../../backend/app/services/common/process_runner.py#L109)) | SIGTERM→10 с→SIGKILL: агент успевает дописать файл. ⚠️ **Только по таймауту**: при отмене и в `kill_all_processes` ([:133-138](../../backend/app/services/common/process_runner.py#L133)) идёт немедленный `proc.kill()` ([:243-246, 384-386, 425-427](../../backend/app/services/common/process_runner.py#L243)) — то есть сценарий «полузаписанного JSON», ради которого grace вводили, при cancel сохраняется |

### 10.2. 🔴 Главная опасность: ложный «зомби» → потеря результатов

Это **самый серьёзный риск** всей затеи, и он подтверждён кодом целиком, а не только памятью проекта.

Цепочка:
```
воркер молчит > 600 с (ZOMBIE_TIMEOUT_SEC, manager.py:321)
  → cleanup_zombies (manager.py:1176) считает job зомби
  → _cleanup(pid) снимает регистрацию
  → _reconcile_stale_queue (manager.py:6496) демотирует item → "interrupted"
  → auto_resume_interrupted_batch (manager.py:688, main.py:125) или _ensure_batch_worker
       переводит в "pending"
  → item исполняется с _action_override="resume" (manager.py:5870)
  → в resume-ветке при start_idx <= 1 вызывается _clean_stage_files
       (manager.py:3644-3649, 3665-3670) — УДАЛЯЮТСЯ 03_findings.json, 01_*, 02_*
```

**Важное уточнение по итогам факт-чека.** Сегодня эта цепочка для **локальных** job надёжно перекрыта тремя гейтами:
`cleanup_zombies` пропускает pid с живыми дочерними процессами (`_protected_pids` [manager.py:571](../../backend/app/pipeline/manager.py#L571), `has_live_processes` [:1198](../../backend/app/pipeline/manager.py#L1198)) и живым asyncio-таском ([:1206-1208](../../backend/app/pipeline/manager.py#L1206)); `_reconcile_stale_queue` дополнительно выходит досрочно при `_batch_worker_alive()` ([:6509-6510](../../backend/app/pipeline/manager.py#L6509)) и `_has_live_project_audit()` ([:6518-6519](../../backend/app/pipeline/manager.py#L6518)). Плюс удаление артефактов происходит лишь при `start_idx <= 1`, а `detect_resume_stage` при наличии артефактов вернёт более поздний этап.

🔴 **Но именно поэтому риск для remote-job максимален:** все три гейта опираются на **локальные** сигналы — живые дочерние процессы, живой asyncio-таск, живой batch-worker. У удалённого задания на диспетчере **не будет ни одного из них по определению**. То есть защита, которая сегодня спасает локальный аудит, для remote не сработает вовсе, а `cleanup_zombies` при этом вызывается каждые 15 секунд на каждый поллинг `/live-status` ([audit.py:661](../../backend/app/api/routers/audit.py#L661)). Без правки гейтов первый же удалённый аудит длиннее 10 минут будет признан зомби.
**Что обязательно сделать:**
1. Для remote-job критерий живости — **не** `has_live_processes`/`_tasks`, а свежесть remote-heartbeat. Готовый образец трёхуровневого детектора живости, который стоит расширить, — `_has_live_project_audit()` ([manager.py:550-570](../../backend/app/pipeline/manager.py#L550)).
2. Таймаут для remote должен быть **на порядок больше** локального (предложение: 30–60 минут вместо 10) и настраиваемым.
3. Пока воркер не прислал явный `job_failed`/`job_completed` **и** не истёк большой таймаут, задание **нельзя** переназначать.
4. Ре-назначение только после подтверждённого освобождения: воркер прислал финальное событие, либо оператор нажал «принудительно снять» (см. §10.6).

### 10.3. Порядок событий и идемпотентность

Требуется добавить (сегодня **НЕ НАЙДЕНО**):
- **монотонный `seq`** на каждое событие в пределах `job_id` — воркер нумерует локально;
- **курсор доставки** `last_acked_seq` на воркере (что центр подтвердил) и `last_seen_seq` на центре;
- **идемпотентный приём**: событие с `seq <= last_seen_seq` — молча отбрасывается; это даёт «повторная отправка не ломает состояние» бесплатно;
- **батчевый догон** после восстановления: воркер шлёт `[last_acked_seq+1 … now]` в порядке возрастания.

Естественный носитель — уже существующий `audit_log.jsonl`: он append-only, per-project, содержит `timestamp/level/stage/message` + произвольные `extras` ([audit_logger.py:378-385](../../backend/app/services/common/audit_logger.py#L378-L385)). Достаточно добавить в `extras` поле `seq`.

### 10.4. Сохранность готового пакета

Требование ТЗ «готовый пакет не должен потеряться» закрывается так:
- воркер после `COMPLETED` **сначала** собирает TAR локально и фиксирует его на диске, **потом** сообщает центру;
- удаление — только по явной команде центра (`delete_package`), не по факту передачи;
- авто-удаление через 30 дней по умолчанию (счётчик на воркере, продлевается командой центра);
- целостность — sha256 всего архива + `input_manifest.json` внутри (в нём уже есть sha256 по каждому исходному файлу).

### 10.5. Одно задание — один воркер

Сегодня защита от дубля держится на трёх in-process проверках: `_enqueue_single` ([manager.py:6342-6369](../../backend/app/pipeline/manager.py#L6342-L6369)), множество `busy` в `_batch_slot_worker` ([:5789-5795](../../backend/app/pipeline/manager.py#L5789-L5795)), `is_running(pid)` ([:1122](../../backend/app/pipeline/manager.py#L1122)).

**Важная деталь, найденная при разборе:** реестры `active_jobs`/`_tasks` ключуются **голым `project_id`**, хотя `job_key()` ([manager.py:1109-1120](../../backend/app/pipeline/manager.py#L1109-L1120)) для V2+ умеет строить `f"{pid}:{vid}"`. Следствие: `is_running(pid, "v002")` по живому V2-job вернёт False, и защита от дубля V2 фактически держится на статусе элемента очереди, а не на реестре.

Для распределёнки это значит: **аренда (lease) должна жить в очереди центра**, а не в реестрах памяти. Минимально: `item.worker_id` + `item.lease_until` + правило «pending-элемент с непросроченной арендой не выдаётся другому воркеру».

### 10.6. Безопасный ручной перезапуск

Оператор должен иметь возможность сказать «перезапустить», не рискуя двойным исполнением. Предлагаемое правило: кнопка «принудительно снять с воркера» доступна только когда (а) heartbeat старше большого таймаута И (б) оператор подтверждает; при этом центр помечает старую аренду отозванной, и если «потерянный» воркер всё же вернётся с результатом — центр принимает пакет, но **не публикует** его в `latest`, а показывает как конфликт для ручного решения.

---

## 11. Передача прогресса и логов

### 11.1. Что пайплайн уже умеет отдавать

| Данные | Где рождаются | Персистентность |
|---|---|---|
| текущий этап | `job.stage` ([models/audit.py:51](../../backend/app/models/audit.py#L51)) + ключи `stages` в `pipeline_log.json` | **на диске** (в pipeline_log) |
| проценты | считаются на лету: `pct = round(current/total*100, 1)` ([models/websocket.py:44](../../backend/app/models/websocket.py#L44)) | нет |
| обработано/всего блоков | `progress_current`/`progress_total` ([models/audit.py:55-56](../../backend/app/models/audit.py#L55-L56)); поток `{"type":"block_done","completed":K,"total":N}` ([gemma_findings_only.py:1734-1739](../../backend/app/pipeline/stages/block_analysis/gemma_findings_only.py)) | только per-block JSON в `run_dir` |
| stdout/stderr | `on_output` построчно ([process_runner.py:209-221](../../backend/app/services/common/process_runner.py#L209-L221)) → `ctx.log` → `persist_log` | **да**, в `audit_log.jsonl` |
| модель | `pipeline_log.stages[].model` через `_enrich_pipeline_log` ([manager.py:1038](../../backend/app/pipeline/manager.py#L1038)) | да |
| длительность | `duration_sec` из monotonic-засечки `_STAGE_RUN_STARTS` ([audit_logger.py:25,126-140](../../backend/app/services/common/audit_logger.py#L25)) | да, но **считается в памяти** → при разрыве между процессами даёт 0 |
| ошибки | `stage_info["error"]` + `pipeline_issues` | да |
| токены/стоимость | `WSMessage.cli_summary` (duration/cost/input/output/cache/model), `usage_data.json`, `paid_cost.json` | да (центрально) |
| промежуточные результаты | все артефакты этапов | да |
| ETA | `_calculate_eta` ([manager.py:3044](../../backend/app/pipeline/manager.py#L3044)) — среднее по `batch_durations` | нет |
| heartbeat | `_heartbeat_loop`, тик **15 с** ([manager.py:2989](../../backend/app/pipeline/manager.py#L2989)) | **нет — только WS** |

### 11.2. Предлагаемый единый формат события воркера

Событие = одна строка JSONL в локальном журнале воркера, она же — единица передачи.

```jsonc
{
  "seq": 1043,                       // монотонный в пределах job_id — НОВОЕ
  "job_id": "caa2b574-...",          // ЕСТЬ (AuditJob.job_id = имя run-каталога)
  "worker_id": "vps-03",             // НОВОЕ
  "project_id": "13АВ-РД-ЭМ-К4",     // ЕСТЬ
  "version_id": "v002",              // ЕСТЬ
  "ts": "2026-08-07T19:14:02.113",   // ЕСТЬ (timestamp в audit_log.jsonl)
  "type": "stage_completed",
  "payload": { }
}
```

| Тип события | Переиспользуем | Источник в коде | Что добавить |
|---|---|---|---|
| `job_started` | частично | `_dispatch_action` начало (manager.py:5998) | обёртка |
| `stage_started` | **да** | `update_pipeline_log(stage, "running")` ([audit_logger.py:143](../../backend/app/services/common/audit_logger.py#L143)) | только `seq` |
| `progress_updated` | **да** | `WSMessage.progress` + `send_progress` ([audit_logger.py:414](../../backend/app/services/common/audit_logger.py#L414)) | `seq`, персист |
| `log_line` | **да** | `persist_log` + `WSMessage.log` | `seq` |
| `stage_completed` | **да** | `update_pipeline_log(stage, "done"/"error"/"partial")` | `seq` |
| `artifact_created` | нет | — | новое: `{name, sha256, bytes}` по факту записи артефакта |
| `quota_warning` | частично | `is_rate_limited` ([cli_utils.py:45](../../backend/app/services/common/cli_utils.py#L45)), `parse_rate_limit_reset` ([:62](../../backend/app/services/common/cli_utils.py#L62)), `check_rate_limit` ([usage_service.py:1033](../../backend/app/services/common/usage_service.py#L1033)) | обёртка + отправка |
| `job_completed` | **да** | `WSMessage.complete` + `job.status = COMPLETED` (manager.py:5330) | `seq` |
| `job_failed` | **да** | `job.status = FAILED` + `error_message` | `seq` |
| `heartbeat` | **да** | `WSMessage.heartbeat` ([models/websocket.py:71](../../backend/app/models/websocket.py#L71)) | **персист** + ресурсы воркера |
| `worker_resources` | нет | `_system_memory` ([model_control_service.py:139](../../backend/app/services/llm/model_control_service.py#L139)), `_disk_stats` ([main.py:278](../../backend/app/main.py#L278)) | новое, см. §13 |

### 11.3. Ретрансляция в UI

Центр при приёме события вызывает `ws_manager.schedule_broadcast_to_project(project_id, WSMessage...)` ([ws/manager.py:38](../../backend/app/ws/manager.py#L38)). Этот метод **уже спроектирован** для вызова из чужого потока (`run_coroutine_threadsafe` на запомненный loop) — фронтенд не заметит разницы между локальным и удалённым исполнением.

Единственное, что придётся поправить на фронте для честности: `formatElapsed` (`app.js:2414-2429`) и `secondsSinceHeartbeat` (`:2461-2466`) считают от **локальных часов браузера**; при воркере в другой TZ/с другим NTP «прошло времени» поедет, а `isHeartbeatStale` (порог 60 с, `app.js:2468`) начнёт врать.


---

## 12. Claude/Codex: запуск, лимиты и параллельность

### 12.1. Все точки запуска LLM (полная карта)

| # | Что запускается | Файл : функция | Команда / транспорт |
|---|---|---|---|
| 1 | Claude CLI, основной путь | [claude_runner.py:187 `_build_cmd`](../../backend/app/services/llm/claude_runner.py#L187) + `:292 _run_cli` | `claude -p --model <M> --allowedTools <T> --output-format json [--strict-mcp-config]`, промпт через **stdin** |
| 2 | Codex CLI, агентный | [codex_runner.py:407 `run_codex_exec`](../../backend/app/services/llm/codex_runner.py#L407) | `codex exec --ephemeral --ignore-user-config --ignore-rules --skip-git-repo-check --sandbox <S> --model <M> [-c model_reasoning_effort=...] [--image ...] -C <ROOT_DIR> -o <tmpfile> -` |
| 3 | Codex CLI, JSON-only | `codex_runner.py:509 run_codex_json_messages` | тот же + `--json [--output-schema <tmp>]`, sandbox по умолчанию `read-only` |
| 4 | **Claude CLI в обход общего runner** | `gemma_findings_only.py:1418 call_claude_cli_for_block` (запуск на `:1463-1485`) | свой `asyncio.create_subprocess_exec`, свой путь `CLAUDE_CLI_BIN` (`:117`) — **без** `resource_budget`, **без** `register_process` |
| 5 | **Claude CLI, ещё один путь** | `services/stage_comparison/text_llm_provider.py:265-273` | блокирующий `subprocess.run` со своим `_find_cli()` и своим rate-limit-ретраем |
| 6 | OpenRouter (Stage 01, напрямую) | [gemma_findings_only.py:95](../../backend/app/pipeline/stages/block_analysis/gemma_findings_only.py) `OPENROUTER_URL` | httpx POST, `Authorization: Bearer $OPENROUTER_API_KEY`, **минуя** `llm_runner` |
| 7 | OpenRouter (общий) | [llm_runner.py:965 `run_llm`](../../backend/app/services/llm/llm_runner.py#L965) | `AsyncOpenAI` на `OPENROUTER_BASE_URL` |
| 8 | Gemini direct | `gemini_direct_runner.py` | Google GenAI SDK; **фактически спящий** (из KZ API отдаёт FAILED_PRECONDITION) |
| 9 | Локальные модели | `llm_runner.py:1036-1096` | **недостижимы**: `LOCAL_LLM_MODELS: set[str] = set()` ([config.py:447](../../backend/app/core/config.py#L447)) |

Список выше неполон. Факт-чек показал: **точек запуска `claude -p` в `backend/app` не менее десяти**:
`claude_runner.py:200` (единственная под бюджетом), `gemma_findings_only.py:1463`, `text_llm_provider.py:265`,
`discussions/discussion_service.py:435` и `:656`, `findings_review/critic_v2/kb_gate.py:151`,
`text_analysis/absence_guard.py:214`, `findings/decision_carryover_service.py:335`,
`findings/migrated_findings_service.py:1219`, `external_register/matcher.py:176`,
плюс `audit.py:229,265` (`claude auth status/logout`).

🔴 **Следствие, которое меняет картину лимитов:** `resource_budget` импортируется **ровно в три файла**
(`claude_runner.py:63`, `codex_runner.py:24`, `llm_runner.py:82`). При этом docstring бюджета
([resource_budget.py:9](../../backend/app/services/common/resource_budget.py#L9)) обосновывает лимит фразой
«absence_guard 4 + decision_carryover 4 → до 20 процессов» — а **оба этих потребителя слот НЕ берут**
(`absence_guard.py:214` и `decision_carryover_service.py:335` вызывают блокирующий `subprocess.run` напрямую).
То есть 8 из 20 процессов, которыми обоснован `BUDGET_CLAUDE_CLI`, этим лимитом не считаются.
`BUDGET_CLAUDE_CLI` — **не «последний рубеж»**, а лимит только для одного из путей.

Аналогично дырява и метрика живости: `register_process` вызывается только из `run_script` ([:204](../../backend/app/services/common/process_runner.py#L204)) и `run_command` ([:318](../../backend/app/services/common/process_runner.py#L318)). Не регистрируются: `run_command_stream` (у него нет параметра `project_id`), Stage 01 и **все** `subprocess.run`-пути. Значит `has_live_processes()` — не «ground-truth», а «truth для двух путей из десяти».

**Для воркера это означает:** прежде чем строить учёт нагрузки и слотов, эти точки надо свести к общему `process_runner` + `resource_budget`, иначе часть нагрузки останется невидимой и для лимитов, и для kill, и для подсчёта занятости.

### 12.2. Откуда берётся авторизация

- **Claude**: ни одного API-ключа. `HOME` намеренно сохраняется даже в «чистом» env (`_CLEAN_ENV_KEEP = {HOME, PATH, LANG, LC_ALL, USER, SHELL}`, [claude_runner.py:218](../../backend/app/services/llm/claude_runner.py#L218)) → CLI берёт подписку из своего файла в `$HOME`.
- **Codex**: аналогично, env-ключей нет; `env_overrides` только вычищает `CLAUDE*` ([codex_runner.py:462](../../backend/app/services/llm/codex_runner.py#L462)).
- Поиск бинарей: `_find_claude_cli` ([config.py:165-191](../../backend/app/core/config.py#L165-L191)) — PATH → `~/.local/bin` → расширения VSCode → `/usr/local/bin`; `find_codex_cli` ([codex_runner.py:43-76](../../backend/app/services/llm/codex_runner.py#L43-L76)) — env `AUDIT_CODEX_CLI_PATH` → PATH → `~/.local/bin` → `~/.npm-global/bin` → расширения VSCode. На текущем хосте codex живёт **внутри расширения VS Code**, то есть привязан к его версии.

**Вывод: требование «секреты авторизации не передавать центру» выполняется само собой** — их и так нет в передаваемом контуре.

✅ **Готовый health-check логина уже есть** (важная находка для диспетчера): `_claude_auth_status_sync()` ([audit.py:225-247](../../backend/app/api/routers/audit.py#L225)) запускает `claude auth status --json` и возвращает `{email, org, plan, loggedIn}`; наружу — `GET /api/audit/account` ([:249](../../backend/app/api/routers/audit.py#L249), под `asyncio.to_thread`). Есть и полный флоу смены аккаунта (`claude auth logout` + фоновый login, `_login_state` [:259](../../backend/app/api/routers/audit.py#L259)). Воркеру достаточно отдавать этот же ответ — центр получит проверяемый статус логина без единого секрета.

⚠️ **Ловушка провижининга.** `kb_gate.py:166-167` сохраняет в чистом env `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, `CLAUDE_CODE_USE_BEDROCK`, но **основной путь аудита их удалит**: `_run_cli` делает `env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}` ([claude_runner.py:336](../../backend/app/services/llm/claude_runner.py#L336)), а `_build_clean_env_overrides` ([:279-289](../../backend/app/services/llm/claude_runner.py#L279)) оставляет только 6 переменных + `XDG_*`. Значит **авторизовать воркер через `CLAUDE_CODE_OAUTH_TOKEN` в env не получится** — только полноценным интерактивным логином CLI в `$HOME`. Это надо заложить в runbook установки воркера.

### 12.3. Env, cwd и песочница

| Параметр | Claude | Codex |
|---|---|---|
| cwd | `BASE_DIR`, либо `/tmp/sonnet_clean/run_XXXX` при `clean_cwd=True` (только `block_batch`) | `str(ROOT_DIR)` + `-C ROOT_DIR` |
| env | вычищаются все `CLAUDE*`; при `clean_cwd` остаются только 6 переменных + `XDG_*` | вычищаются все `CLAUDE*`, остальное наследуется |
| песочница | нет (управляется `--allowedTools`) | `--sandbox`, env `AUDIT_CODEX_SANDBOX`, допустимо `read-only\|workspace-write\|danger-full-access`, дефолт в коде `workspace-write` |
| MCP | `--strict-mcp-config` для этапов **без** `mcp__` (флаг `AUDIT_STRICT_MCP_FOR_NON_NORM_STAGES`, дефолт true) — введён после OOM 04.08.2026 | `assert_norms_stage_wired` ([:138](../../backend/app/services/llm/codex_runner.py#L138)) не даёт норм-этапу стартовать без `mcp__norms__*` |

⚠️ **Замечание по безопасности:** в `.env` текущего прода `AUDIT_CODEX_SANDBOX=danger-full-access`, то есть Codex работает **без песочницы** с рабочим каталогом = корень репозитория. На своём VPS это осознанный выбор; на **стороннем** VPS с чужими проектами это нужно пересмотреть (см. §14).

### 12.4. Детекция лимитов и ошибок

| Что | Где | Как |
|---|---|---|
| rate limit | [cli_utils.py:45 `is_rate_limited`](../../backend/app/services/common/cli_utils.py#L45) | 9 паттернов: `rate.?limit`, `429`, `too many requests`, `overloaded`, `quota.?exceeded`, `hit your limit`, … |
| время сброса | [cli_utils.py:62 `parse_rate_limit_reset`](../../backend/app/services/common/cli_utils.py#L62) | парсит `resets 11pm (Europe/Moscow)`; **MSK зашит как UTC+3** |
| usage limit у codex | `claude_runner.py:379 _CODEX_USAGE_LIMIT_RE` | отдельный regex — общий список формулировки codex не ловит |
| ожидание сброса | [manager.py:825 `_wait_for_rate_limit`](../../backend/app/pipeline/manager.py#L825) | общий дедлайн `_rate_limit_deadline` + разбежка `stagger` (30 с × число ждущих), потолок ожидания 5 ч |
| остаток лимита | [usage_service.py:1033 `check_rate_limit`](../../backend/app/services/common/usage_service.py#L1033) | по `GlobalUsageScanner`, парсящему `~/.claude/projects/*.jsonl`; возвращает `{can_proceed, wait_seconds, usage_pct, resets_in_text}` |
| **auth error** | — | **отдельной детекции НЕТ**: невалидная авторизация попадёт в общий «код != 0» |
| **usage limit у codex → retry** | — | **ретрая НЕТ**: `scripts/monitor_codex_load.py:8-12` прямо фиксирует — исчерпание приходит как `exit != 0`, нога молча выпадает из `detectors_ok`, стадия завершается «успешно» с меньшим числом находок |

Дополнительно (найдено при факт-чеке):
- `PAID_API_ENABLED` **по умолчанию `False`** ([config.py:323](../../backend/app/core/config.py#L323)); на центральном хосте в `.env` она выставлена в `true`, а `_paid_api_enabled_runtime()` перечитывает env на каждый вызов. Свежий воркер **без явного `PAID_API_ENABLED=true` не сделает ни одного платного вызова** — GPT-нога ансамбля Stage 01 упадёт на гарде. Переменная обязана быть в профиле воркера.
- `PAID_API_DAILY_LIMIT_USD` по умолчанию `0.0`, а `0` означает **лимит ВЫКЛЮЧЕН** (`if limit <= 0.0: return None`, `paid_api_guard.py:272-273`) — не «запретить всё».
- Отдельная нерепетируемая категория отказа — `is_prompt_too_long` ([cli_utils.py:37-42](../../backend/app/services/common/cli_utils.py#L37)); это ни rate-limit, ни auth, и ретраить её бессмысленно.
- Модуль гарда лежит в `backend/app/services/llm/paid_api_guard.py` (не в `common/`).
- `STAGE01_ABORT_ON_LEG_FAILURE_ENABLED` ([config.py:511](../../backend/app/core/config.py#L511), в `.env` центра = `true`) и `STAGE01_LEG_FAILURE_THRESHOLD` — **уже заданы** в `.env` этого хоста; это готовый рычаг «останавливать прогон при выпавшей ноге», который на воркере обязателен.

**Для распределёнки это означает:** воркер обязан сам детектировать «нога выпала по лимиту» и сообщать `quota_warning`, иначе центр получит формально успешный аудит с деградированным recall. Признак есть готовый: `detectors_failed` в поблочных отчётах ([gemma_findings_only.py:1679-1680](../../backend/app/pipeline/stages/block_analysis/gemma_findings_only.py)) и флаг `STAGE01_ABORT_ON_LEG_FAILURE_ENABLED` ([config.py:511](../../backend/app/core/config.py#L511)).

### 12.5. Параллельность: сколько процессов и где ограничители

Глобальные бюджеты — [resource_budget.py](../../backend/app/services/common/resource_budget.py), `asyncio.Semaphore` **на процесс бэкенда**:

| Ресурс | env | дефолт в коде | в проде |
|---|---|---|---|
| `claude_cli` | `BUDGET_CLAUDE_CLI` | 6 | 6 |
| `codex_cli` | `BUDGET_CODEX_CLI` | 6 | **20** |
| `norms_mcp` | `BUDGET_NORMS_MCP` | 2 | **5** |
| `local_llm` | `BUDGET_LOCAL_LLM` | 1 | 1 (недостижим) |

Порядок захвата зафиксирован против hold-and-wait: **сначала дефицитный `norms_mcp`, потом обильный `*_cli`** ([claude_runner.py:344-353](../../backend/app/services/llm/claude_runner.py#L344-L353), [codex_runner.py:470-476](../../backend/app/services/llm/codex_runner.py#L470-L476)); нарушение порядка = взаимная блокировка.

Уровни выше: `BATCH_MAX_PARALLEL` (дефолт 1, потолок 8, в проде **5**, [manager.py:263-276](../../backend/app/pipeline/manager.py#L263-L276)); Stage 01 — `Semaphore(parallelism)`, дефолт 3 для GPT и **1** для codex/ensemble (`AUDIT_STAGE02_CODEX_PARALLELISM`); `absence_guard` 4 потока; `decision_carryover` 4; норм-этап `Semaphore(1)`.

Расчёт пика из `.env.example`: **5 проектов × 2 блока × 2 codex-ноги = 20** одновременных `codex exec`.

Эмпирика из кода ([resource_budget.py:51-52](../../backend/app/services/common/resource_budget.py#L51-L52)): «измерено, что подписка держит десятки одновременных сессий (эксперимент 06.08.2026), поэтому потолок здесь не про работоспособность, а про расход квоты и RAM».

**Блокировок на уровне рабочей директории нет** — ни файловых локов, ни lock-файлов на `_output/`. Изоляция достигается tmp-каталогом на каждый `claude -p` и ContextVar-скоупом путей ([audit_scope.py](../../backend/app/services/common/audit_scope.py)). Межхостовой блокировки на проект нет вообще — её вводит диспетчер (§10.5).

### 12.6. Ограничения при 5 проектах на воркере

1. **RAM — главный ограничитель.** Норм-MCP: **5,6 ГБ на сессию** после первого `semantic_search`, не выгружается до конца сессии. Норм-модели внутри процесса бэкенда — до **4,3 ГБ** (`release_models()` даёт 4063 МБ → 477 МБ).
2. **Два зафиксированных OOM-инцидента:** 01.07.2026 (норм-модели жили в uvicorn вечно, хост 11 ГБ) и 04.08.2026 (норм-MCP поднимался на этапах без `mcp__`: «text_analysis ЭО1-3 код 143», «optimization ОВ1-2.3 exit -9»).
3. **Лимиты подписки не координируются между хостами** — `_wait_for_rate_limit` согласует ожидание только внутри процесса; при нескольких воркерах на одной подписке разбежка перестаёт работать.
4. **Дневной лимит платного API не будет глобальным**: `paid_api_guard` считает по локальному `paid_cost.json` + in-process `_reservations`.

---

## 13. Требования к ресурсам VPS

### 13.1. Что контролировать (и чем это уже можно измерить)

| Метрика | Откуда | Уже есть в репо |
|---|---|---|
| `MemAvailable`, swap | `psutil.virtual_memory()/swap_memory()` | [model_control_service.py:139-166](../../backend/app/services/llm/model_control_service.py#L139) |
| свободный диск | `shutil.disk_usage(DATA_DIR)` | [main.py:278 `_disk_stats`](../../backend/app/main.py#L278) |
| ядра | `os.sched_getaffinity(0)` | [cpu_pool.py:66 `available_cores`](../../backend/app/services/common/cpu_pool.py#L66) |
| LA1/LA5 | `/proc/loadavg` | **нет — добавить** |
| живых `codex exec` | `ps -eo args`, фильтр `/codex` + ` exec` | [scripts/monitor_codex_load.py:74 `live_codex_processes`](../../scripts/monitor_codex_load.py#L74) |
| живых `claude -p` | тот же приём | **нет — добавить по аналогии** |
| занятость бюджетов | `resource_budget.snapshot()` | функция есть, **в API не выведена** |
| состояние пула | `cpu_pool.pool_info()` | функция есть, **в API не выведена** |
| GPU | `nvidia-smi` | `model_control_service.py:201` — на этом хосте честно возвращает «not found»; **конвейер GPU не использует** |
| локальные модели | — | в конвейере аудита **отключены** (`LOCAL_LLM_MODELS` пусто) |

Admission control по ресурсам в коде **отсутствует полностью** — единственный регулятор сегодня — статические env-числа.

### 13.2. Правило расчёта свободных слотов 0..5

```
slots = clamp(0, 5, min(S_ram, S_disk, S_cpu, S_la, S_proc))
```

**S_ram** = `floor((MemAvailable_ГБ − 8) / 6.5)`
— 6,5 ГБ на проект: 5,6 ГБ норм-MCP на сессию + ~0,5–1 ГБ на процессы CLI и буферы (`_STREAM_LIMIT` 64 МБ на процесс).
— резерв 8 ГБ: норм-модели в самом процессе бэкенда до 4,3 ГБ + ОС + страничный кэш.
— **жёсткий ноль при `swap_used > 1 ГБ`** — это буквально профиль обоих OOM-инцидентов.
— проверка на текущем хосте: `(44 − 8) / 6,5 = 5,5 → 5`, что совпадает с прод-настройкой `BATCH_MAX_PARALLEL=5`.

**S_disk** = `floor((free_ГБ − 20) / 0.5)`
— 0,5 ГБ на проект: p95 версии = 170 МБ, максимум 637 МБ, плюс `runs/`.
— резерв 20 ГБ: норм-багаж 6,6 + HF-кэш 4,3 + LRU кропов 1,5 + запас.
— **жёсткий ноль при `free < 2 ГБ`** — это собственный порог кода `BLOCK_CROP_CACHE_MIN_FREE_BYTES` ([config.py:1205](../../backend/app/core/config.py#L1205)); история «диск 100 % → обнулил stage_models.json» и «12,2 ГБ кропов при заполнении 98 %» показывают, что запас нужен большой.

**S_cpu** = `floor((cores − 2) / 2.5)`
— резерв 2 ядра — константа самого пула (`RESERVED_CORES`, [cpu_pool.py:50](../../backend/app/services/common/cpu_pool.py#L50)): HTTP/WS и event loop не должны голодать.
— 2,5 ядра на проект: пул делится между проектами и имеет потолок 8 воркеров.
— 16 ядер → 5; 8 ядер → 2; **4 ядра → 0** (такой VPS годится максимум под 1 слот принудительно).

**S_la** — гейт, а не делитель: `LA5/cores < 1.0` → без штрафа; `1.0…1.5` → −1 слот; `≥ 1.5` → 0 новых слотов (уже работающие не трогаем). Обоснование: при GIL-давлении замерено «85 % CPU у бэкенда и 5–22 с на блок при чистых 1–1,5 с», и на этом профиле watchdog убивает бэкенд по неответу `/api/info`.

**S_proc** — защита бюджетов: `live_codex ≥ 0.8 × BUDGET_CODEX_CLI` или `live_claude ≥ 0.8 × BUDGET_CLAUDE_CLI` → 0 новых слотов. Обоснование: у codex-пути нет retry на usage limit, и дешевле не дать слот, чем получить «успешный» аудит с урезанным recall.

### 13.3. Что воркер обязан отдавать вместе с числом слотов

Чтобы число было проверяемым: `MemAvailable`, `swap_used`, `disk_free`, `cores`, `LA1/LA5`, `live_codex`, `live_claude`, `resource_budget.snapshot()`, `cpu_pool.pool_info()`, `_has_live_project_audit()` как occupancy-признак ([manager.py:550-570](../../backend/app/pipeline/manager.py#L550)), остаток лимитов из `check_rate_limit()` и статус логина из `claude auth status --json`, **и список `project_id`, которые воркер сейчас ведёт** — потому что реестры ключуются голым `project_id`, и два элемента одного проекта на **разных** воркерах дадут ту же коллизию артефактов, от которой на одном хосте защищает проверка `busy`.


---

## 14. Риски безопасности

### 14.1. Что есть сегодня

- **Аутентификация только пользовательская**: self-contained подписанный cookie, HMAC-SHA256, пароли `pbkdf2_sha256`, настройки из env `PORTAL_AUTH_ENABLED`, `PORTAL_AUTH_USERS`, `PORTAL_SESSION_SECRET`, `PORTAL_SESSION_TTL_HOURS`, `PORTAL_COOKIE_SECURE` ([core/portal_auth.py](../../backend/app/core/portal_auth.py)). Защита `/api/*` — `PortalAuthMiddleware` (исключения: `/login`, `/api/auth/*`, `/api/info`, `/favicon.ico`); WS проверяется отдельно `_ws_authorized` ([main.py:240-246](../../backend/app/main.py#L240-L246)).
- **Машинной аутентификации (API-ключи, bearer, service accounts) НЕТ** — подтверждено grep-ом по роутерам.
- Если `PORTAL_SESSION_SECRET` не задан — эфемерный секрет на процесс ([portal_auth.py:59](../../backend/app/core/portal_auth.py#L59)): при рестарте сессии слетают, между хостами несовместимы.
- HTTPS/WSS уже есть: nginx + Let's Encrypt на `auditmanager.app`, WS-upgrade map, `client_max_body_size 200M`.
- Журнал действий: `logs/actions/*.jsonl`, 4 вида событий (`api`, `pipeline`, `app_log`, `system`), retention 180 дней, потолок суток, шум-фильтр только для успешных GET ([core/action_log.py](../../backend/app/core/action_log.py)).
- Деструктивные операции загейчены: `DestructiveWriteBlocked` ([storage_write_facade.py:104](../../backend/app/services/storage/storage_write_facade.py#L104)), удаление версий → `_trash`, эвакуация кропов → `.evicted/`.

### 14.2. Минимальные требования к распределённому контуру

| Требование | Как закрыть | Что уже есть |
|---|---|---|
| Регистрация воркера | новый эндпоинт + `workers.json`; статус `pending → approved` вручную | нет |
| Отдельный токен на VPS | длинный случайный токен на воркер, хранится хешем на центре; передача — заголовок при WSS-хендшейке | механизм HMAC-подписи есть в `portal_auth`, переиспользуем |
| HTTPS/WSS | **уже есть** (nginx + Certbot) | да |
| Целостность пакета | sha256 архива + `input_manifest.json` (sha256 по каждому исходному файлу — **уже есть**) | частично |
| Защита от повторной выдачи | аренда `item.worker_id` + `lease_until` в очереди центра (§10.5) | нет |
| Изоляция рабочих каталогов | у воркера свой `AUDIT_PROJECTS_V2_DIR`; воркер видит только выданные ему проекты | env-механизм есть |
| **Запрет произвольных команд** | центр не имеет канала «выполни shell» — только фиксированный набор действий | **сегодня такого канала и нет**, важно не создать |
| Ограниченный набор действий | `assign_job`, `cancel_job`, `delete_package`, `extend_retention`, `update_code` — закрытый enum | нет |
| Очистка документов | авто-удаление через 30 дней; принудительное — командой | нет |
| Журнал действий | ActionLog уже пишет `api`/`pipeline`, добавить `kind="worker"` | частично |
| Отзыв доступа | пометка воркера `revoked` → отказ на хендшейке + сброс WSS | нет |

### 14.3. Специфические риски этой системы

1. 🔴 **Codex в проде идёт с `AUDIT_CODEX_SANDBOX=danger-full-access`**, cwd = корень репозитория. На стороннем VPS это означает: LLM-агент имеет полный доступ к ФС воркера. Для чужой инфраструктуры нужно вернуть `workspace-write` и ограничить корень каталогом задания.
2. 🔴 **Claude-этапы работают с `WebSearch`/`WebFetch`** ([config.py:231-233](../../backend/app/core/config.py#L231-L233): `TEXT_ANALYSIS_TOOLS`, `BLOCK_ANALYSIS_TOOLS`, `FINDINGS_MERGE_TOOLS`) — модель может ходить в интернет с воркера. Для норм-этапов и оптимизации это уже запрещено (только `Read,Write,Grep,Glob` + `mcp__norms__*`).
3. 🟠 **Проектная документация — чувствительные данные заказчика.** Отправляя пакет на сторонний VPS, вы физически передаёте PDF проектной документации. Нужны: договорная рамка, шифрование канала (есть), контроль срока хранения (30 дней), возможность экстренного удаления.
4. 🟠 **Воркер = полный backend** → если на нём случайно включить портальную авторизацию и открыть порт, он станет вторым порталом с доступом к данным. В режиме воркера порт наружу открывать нельзя.
5. 🟡 **`_recover_stale_pipelines` сканирует ВСЕ проекты хоста** ([manager.py:1272](../../backend/app/pipeline/manager.py#L1272)) — на воркере это лишняя работа, на центре с зеркалами — потенциально опасная правка чужих логов.
6. 🟡 **Watchdog судит о живости по `batch_queue.json`** (`~/bin/webapp-watchdog.sh:14,29-31`) — на воркере этот контракт неверен и может перезапустить процесс посреди аудита.

---

## 15. Риски совместимости с текущим пайплайном

| # | Риск | Проявление | Смягчение |
|---|---|---|---|
| 1 | **Ложный зомби → удаление артефактов** | §10.2: 600 с молчания → resume → `_clean_stage_files` стирает `03_findings.json` | remote-heartbeat + большой таймаут + запрет авто-переназначения |
| 2 | **Расхождение режима хранилища** | `get_write_mode()`/`get_storage_mode()` читаются из env **на каждый вызов**; если на воркере режим иной — резолв уедет в legacy-ветку с `PROJECTS_DIR`, которого нет, и артефакты создадутся по фантомному пути (класс инцидента уже описан в комментарии `resolve_project_dir`) | режим — часть задания, сверять на хендшейке |
| 3 | **Разные модели этапов** | `stage_models.json` вне git; прод на codex, дефолты на opus → воркер молча прогонит другими моделями и другой ценой | снапшот конфига в пакете + отпечаток |
| 4 | **Разные промпты** | `prompts/` редактируются из UI | снапшот в пакете |
| 5 | **Конфликт записи в `latest`** | `03_analysis/latest` не append-only; воркер и центр могут перезаписать `03_findings.json`, `pipeline_log.json` | публикацию в `latest` делает **только центр** после приёма пакета |
| 6 | **Вердикты эксперта затираются** | `04_review/expert_review.json` и `discussions/` живут вне `03_analysis`; при возврате пакета их нельзя перезаписывать содержимым воркера | белый список путей при распаковке результата |
| 7 | **Выбор «свежего прогона» по mtime** | `blocks_dir` и `_fallback_run_dir` выбирают run по `st_mtime_ns`; ZIP огрубляет mtime до 2 с, `copytree`/`rsync` без `-t` теряет → после распаковки «свежим» может стать не тот прогон | TAR (сохраняет mtime) + явное указание `run_id` в задании |
| 8 | **Хардлинки рвутся** | +40 % к размеру пакета; партнёры вне версии не восстановятся | TAR вместо ZIP; кропы можно вообще не везти |
| 9 | **Абсолютные пути в артефактах** | `runtime_plan_path`, `artifacts_dir`, `output_dir`, `project_dir` | переписывать при распаковке (механика уже есть: `project_rename_service._remap_old_to_new_map`, `_replace_path_prefix`) |
| 10 | **`objects.json` отсутствует на воркере** | `_ensure_default_object` молча создаст новый объект с новым uuid → расхождение идентичностей | передавать `object_id` в задании, запретить авто-создание при `AUDIT_ROLE=worker` |
| 11 | **Резолв project_id = полный обход ФС** | `adapter.find_document` перебирает все документы с чтением каждого `document.json` | на воркере с одним проектом отработает; но форму дерева `objects/…/documents/<код>/versions/<vid>` ломать нельзя |
| 12 | **Норм-venv хрупок** | апгрейд системного python молча ломает `mcp__norms__*`, guard не ловит (§5.3) | вариант Б (§9.2) снимает риск целиком |
| 13 | **Дублирование PDF внутри версии** | `01_input/*.pdf` + `02_work/document.pdf` — байтовые копии (до 213 МБ на версию) | в пакет класть один экземпляр, второй воссоздавать `_sync_v2_work_copies` |
| 14 | **`duration_sec` обнулится** | считается по monotonic-меткам `_STAGE_RUN_STARTS` в памяти процесса | принимать длительность из событий воркера |
| 15 | **`_enqueue_single` требует локального проекта** | `resolve_project_dir` + `resolve_effective_version_id` + `get_version_entry` вызываются **до** постановки в очередь ([manager.py:6330-6338](../../backend/app/pipeline/manager.py#L6330)); `VersionNotFoundError` → `RuntimeError` | центр и так хранит все проекты (первый этап), но при «только на воркере» это отдельный барьер |
| 16 | **`cancel()` не работает для pending в прерванной очереди** | `manager.py:1362` — ветка pending выполняется только `if self._batch_queue.status == "running"`; у `interrupted`-очереди `cancel` вернёт `False` | учесть в модели отмены диспетчера |
| 17 | **Схема очереди рассинхронена с реальными значениями** | `models/audit.py:143` комментирует `running / completed / cancelled`, а код штатно пишет `interrupted` (`manager.py:658-663, 5608-5611, 5643, 6529`); у item реально ставится ещё и `skipped` (`:5827-5834`) | внешний потребитель (диспетчер, парсер пакета) получит неполный enum |
| 18 | **`kill_all_processes` теряет реестр до убийства** | `process_runner.py:135` делает `pop` **до** kill → сразу после вызова `has_live_processes(pid)` = False, хотя процессы ещё живы | ложноотрицательный сигнал занятости воркера |
| 19 | **`BLOCK_CROP_RESTORE_ALLOW_NETWORK` по умолчанию `True`** | [config.py:1187](../../backend/app/core/config.py#L1187) — при включении RESTORE поход в сеть по `crop_url` разрешён по умолчанию | на изолированном воркере выключить явно |
| 20 | **Расхождение embed-модели норм** | `norms/tools/search.py:29` объявляет `intfloat/multilingual-e5-base`, а докстринг и HF-кэш — **e5-large** | подготовка воркера по константе притащит не ту модель, чем та, на которой построен `paragraphs_embeddings.npz` |
| 21 | **`pdfplumber` не установлен на проде** | ветки `block_pdf_source.py:444` и `ar_ceiling_lighting/coords.py:222` сейчас мёртвые | если поставить на воркере — поведение профилей изменится; фиксировать состав зависимостей явно |
| 22 | **`prepare` не проваливает отсутствие MD** | `process_project.py:288 process()` при отсутствии MD делает `return False`, но `main()` результат игнорирует и `sys.exit` не вызывает → exit-код 0 → `prepare/runner.py:60` рапортует успех; реальный gate выше (`_require_project_md`) | на воркере полагаться только на `_require_project_md` |
| 23 | **Регресс-гейт зависит от окружения** | `scripts/ci_regression_gate.py` + baseline из 87 известных падений; в новом окружении baseline надо пересоздавать `--record` | учесть при настройке CI на воркере |

---

## 16. Неопределённости, которые необходимо решить до реализации

| # | Вопрос | Почему это блокирует | Варианты |
|---|---|---|---|
| 1 | **Норм-этап: на воркере или в центре?** | определяет объём установки (+11 ГБ/воркер), потребление RAM (−1..2 слота) и риск конфликта записи в `norms_paragraphs.json` | **А** — везти всё; **Б** — норм-этап в центре (рекомендация); **В** — RPC-прокси норм-API (интерфейс узкий: `resolve_norm_status`, `get_paragraph`, `semantic_search`) |
| 2 | **Кто владеет `decisions_log.json`?** | `decision_carryover` и `verdict_rehydration` **пишут** в него во время прогона | центр эксклюзивно (рекомендация) — воркер отдаёт дельту в пакете |
| 3 | **Один воркер = весь конвейер, или конвейер минус 2 этапа?** | вытекает из №1 и №2 | «минус `norm_verify` и `decision_carryover`» — компромисс первого этапа |
| 4 | **Везти ли кропы в пакете?** | 15–420 МБ на версию; восстановимы офлайн из `02_work/document.pdf` | не везти; гидрировать на воркере через `hydrate_blocks_dir` |
| 5 | **Как считать дневной лимит платного API глобально?** | `paid_api_guard` локален, `PAID_API_DAILY_LIMIT_USD` перестаёт работать при N воркерах | квота на воркер от центра при назначении, либо синхронный чек перед платным вызовом |
| 6 | **Как согласовывать rate-limit подписки между воркерами?** | если на нескольких VPS одна учётка Claude/Codex — разбежка `_wait_for_rate_limit` не работает | по одной учётке на VPS (рекомендация), либо общий дедлайн через центр |
| 7 | **Формат «даты сброса лимита», вводимой вручную** | требование ТЗ; сейчас есть только автоматический парсинг из вывода CLI и оценка `GlobalUsageScanner` | хранить на центре per-worker per-provider: `{reset_at, source: manual\|parsed\|scanner}` |
| 8 | **Что считать «скоро сгорит неиспользованный лимит»?** | правило приоритизации в авто-режиме | нужна формула: `приоритет = f(остаток_лимита, время_до_сброса)`; данные есть в `GlobalUsageScanner` |
| 9 | **Версионирование кода воркера** | сегодня отпечатка нет вообще (`/api/info` → `"1.0.0"`) | составной отпечаток (§8.1 п.9) + отказ принимать задание при несовпадении |
| 10 | **Механизм централизованного обновления кода** | требование ТЗ «в дальнейшем» | git pull + рестарт по команде? отдельный пакет? — решить позже, но заложить поле версии сейчас |
| 11 | **Что делать с «вернувшимся» воркером после переназначения** | оба закончат один job | §10.6: принимать пакет, но не публиковать, показать конфликт |
| 12 | **Судьба legacy-режима хранилища** | код держит три режима (`legacy`/`dual_write_shadow`/`projects_v2_primary`) | воркер поддерживает только `projects_v2_primary` |
| 13 | **Кто чинит сломанный norms-venv на воркерах** | §5.3 — уже сломан на центральном хосте | вытекает из №1; при варианте Б вопрос снимается |
| 14 | **Нужен ли воркеру фронтенд** | сейчас backend монтирует `/static` и отдаёт SPA | в режиме воркера не монтировать |
| 15 | **Что делать с тремя параллельными контурами заданий** | `section_optimization` (кросс-проектный по замыслу), `stage_comparison/pipeline_queue` (свой корень `comparison/`), `prepare` (свой WS и интерлок) — §5.5 | оставить целиком на центре в первом этапе; но решить, как считать их нагрузку при подсчёте слотов |
| 16 | **Источник кропов на воркере** | `AUDIT_CROP_CACHE_SOURCE`: `local_pdf` (нужен PyMuPDF) или `download` (нужен доступ к порталу) | `local_pdf` + обязательный PyMuPDF; сеть на портал с чужого VPS не открывать |

---

## 17. Рекомендуемый объём следующего этапа

Разбивка на 5 шагов, каждый — самостоятельно ценный и обратимый.

### Шаг 1. Скелет протокола (без исполнения)
- `workers.json` + роутер `/api/workers`: регистрация, heartbeat, статус, список.
- Токен на воркер, проверка на хендшейке, отзыв.
- Экран «Воркеры» в UI (по прототипу `model-control.html`).
- **Воркеры пока ничего не исполняют** — только регистрируются и отдают метрики ресурсов и лимитов.
- Проверяемый результат: в одном окне видно все подключённые VPS, их RAM/диск/слоты/остаток лимитов.

### Шаг 2. Пакет и абстракция исполнения
- `ExecutionBackend` + `LocalExecutionBackend` (рефакторинг без смены поведения: `_batch_slot_worker` вызывает бэкенд вместо прямого `_dispatch_action`).
- Сборка/распаковка TAR-пакета: исходники + метаданные + снапшот `prompts/` + `stage_models.json` + профиль флагов + (опц.) предыдущая версия.
- Переписывание абсолютных путей при распаковке.
- Проверяемый результат: регресс-гейт зелёный, локальные аудиты идут как раньше; пакет собирается и распаковывается на тестовом VPS, `resume_detector` на нём даёт правильную точку старта.

### Шаг 3. Удалённое исполнение одного проекта вручную
- `RemoteWorkerExecutionBackend` + поле `worker_id` в `BatchQueueItem`.
- Событийный журнал с `seq` и курсором, приём событий, ретрансляция в WS.
- Remote-heartbeat + правка `cleanup_zombies`/`_protected_pids` (**самая рискованная часть**).
- Приём пакета результата; публикация в `latest` — только центром.
- Норм-этап и `decision_carryover` остаются в центре (вариант Б).
- Проверяемый результат: один проект целиком отработал на стороннем VPS, лог и прогресс видны в UI в реальном времени, результат опубликован.

### Шаг 4. Отказоустойчивость
- Обрыв связи посреди аудита → воркер продолжает, события копятся, после восстановления догоняют в правильном порядке.
- Аренда заданий, запрет двойного исполнения, безопасный ручной перезапуск.
- Хранение пакета 30 дней, команды `delete_package`/`extend_retention`.
- Проверяемый результат: сценарий «выключили сеть на 30 минут посреди Stage 01» проходит без потери данных и без дублей.

### Шаг 5. Автоматика
- Подсчёт слотов 0..5 по формуле §13.2.
- Авто-назначение с приоритетом «у кого скоро сгорит неиспользованный лимит».
- Ручные даты сброса лимитов.
- До 5 параллельных проектов на воркер.

**Чего в следующем этапе делать НЕ нужно:** переписывать `PipelineManager`, чинить дублирование списков этапов, вводить брокеры сообщений/БД/объектное хранилище, распределять этапы одного проекта между VPS.

### Почему не нужны новые инфраструктурные компоненты

- **Очередь сообщений (RabbitMQ/Redis)** — избыточна: очередь уже персистится атомарно в JSON, воркеров единицы, а поток событий последовательный per-job. WSS + догон по HTTPS решают ту же задачу без нового демона.
- **База данных (PostgreSQL)** — потребовала бы переписать всё хранилище артефактов; при этом реальная потребность — реестр воркеров (десятки записей) и курсоры доставки, что укладывается в те же JSON с `flock`.
- **S3/объектное хранилище** — по условию первого этапа пакеты хранятся централизованно на основном VPS, а передача идёт по HTTPS; при 100–300 МБ на пакет и единицах воркеров прямая отдача файла проще.
- **Kubernetes** — воркеры это не эфемерные поды: у каждого своя локальная авторизация CLI и своё состояние; оркестрация контейнеров тут не решает ни одной задачи.

Если объём вырастет (десятки воркеров, сотни проектов в сутки), первым кандидатом станет БД для реестра заданий — но не раньше.


---

## 18. Таблица затрагиваемых файлов

Риск: 🟢 низкий (аддитивно / новый файл), 🟡 средний (правка живого пути), 🔴 высокий (правка механизма, где уже были инциденты).

| Путь | Назначение сегодня | Предполагаемое изменение | Риск | Обязательность |
|---|---|---|---|---|
| `backend/app/pipeline/manager.py` | синглтон-диспетчер + исполнитель (6871 стр.) | врезка `ExecutionBackend` в `_batch_slot_worker:5878`; remote-ветка в `cancel:1330`; учёт remote в `cleanup_zombies:1176` и `_protected_pids:571` | 🔴 | **обязательно** |
| `backend/app/models/audit.py` | `AuditJob`, `BatchQueueItem/Status` | + `worker_id`, `lease_until`, `assigned_at` в `BatchQueueItem` | 🟢 | **обязательно** |
| `backend/app/api/routers/workers.py` | — (нет) | **новый**: регистрация, heartbeat, события, приём пакета, ресурсы, отзыв | 🟢 | **обязательно** |
| `backend/app/services/workers/*` | — (нет) | **новый**: `WorkerManager` (реестр, слоты, назначение), `EventIngest` (дедуп по `seq`), `PackageBuilder`/`PackageReceiver` | 🟢 | **обязательно** |
| `backend/app/services/common/audit_logger.py` | единая воронка stage-статусов + лог + WS | + `seq` в `persist_log`; вызов `update_pipeline_log` при приёме remote-события | 🟡 | **обязательно** |
| `backend/app/ws/manager.py` | ретрансляция в браузер | без правок — `schedule_broadcast_to_project:38` уже годится | 🟢 | не требуется |
| `backend/app/core/config.py` | все пути и константы | + `AUDIT_ROLE`, `AUDIT_DISPATCHER_URL`, `AUDIT_WORKER_ID`, таймаут remote-зомби, отпечаток версии | 🟡 | **обязательно** |
| `backend/app/main.py` | lifespan, роутеры, WS, `/api/info` | при `AUDIT_ROLE=worker` не монтировать SPA, не запускать `auto_resume_interrupted_batch`; `/api/info` → реальный отпечаток вместо `"1.0.0"` | 🟡 | **обязательно** |
| `backend/app/api/routers/audit.py` | эндпоинты аудита и очереди | `worker_id` в теле запуска; поле `worker` в `live-status:658`; вынести sync-работу в `to_thread` | 🟡 | желательно |
| `backend/app/api/routers/export.py` | ZIP результатов (`audit-package:252`) | переиспользовать как основу «пакета обратно» | 🟢 | желательно |
| `backend/app/services/findings/decision_carryover_service.py` | **пишет** в `decisions_log.json` (`:682`) | при `AUDIT_ROLE=worker` — писать дельту в пакет, не в глобальный лог | 🔴 | **обязательно** |
| `backend/app/services/findings/verdict_preservation.py` | **пишет** в `decisions_log.json` | то же | 🔴 | **обязательно** |
| `backend/app/pipeline/stages/norms/runner.py` | норм-этап; **пишет** `norms_paragraphs.json` | при варианте Б — не исполнять на воркере | 🟡 | **обязательно** (следствие §16 №1) |
| `backend/app/services/common/resource_budget.py` | бюджеты CLI/MCP | вывести `snapshot()` наружу; пересчитать дефолты под воркер | 🟢 | желательно |
| `backend/app/services/common/cpu_pool.py` | пул процессов | вывести `pool_info()` наружу | 🟢 | желательно |
| `backend/app/services/common/usage_service.py` | `GlobalUsageScanner`, `paid_cost` | отдавать остаток лимитов в отчёт воркера; квота вместо глобального лимита | 🟡 | **обязательно** |
| `backend/app/services/common/cli_utils.py` | детект лимитов, `parse_rate_limit_reset:62` | убрать жёсткий MSK (UTC+3) | 🟢 | желательно |
| `backend/app/services/llm/codex_runner.py` | запуск Codex | `assert_norms_mcp_available:114` — проверять `import mcp`; пересмотреть sandbox для чужого VPS | 🟡 | желательно |
| `backend/app/pipeline/stages/block_analysis/gemma_findings_only.py` | Stage 01; **свой** запуск `claude -p` (`:1462`) | привести к общему `process_runner` + `resource_budget` | 🟡 | желательно |
| `backend/app/services/stage_comparison/text_llm_provider.py` | третий путь запуска `claude` (`:265`) | то же | 🟢 | опционально |
| `backend/app/pipeline/stages/prepare/prepare_service.py` | вторая очередь; `_persist_queue:218` неатомарен | атомарная запись | 🟢 | желательно |
| `backend/app/services/storage/projects_v2_adapter.py` | резолв документа, выбор run по mtime | явный `run_id` вместо mtime при распаковке пакета | 🟡 | желательно |
| `backend/app/services/common/project_service.py` | `resolve_project_dir`, `iter_project_dirs` | запретить авто-создание объекта при `AUDIT_ROLE=worker` | 🟡 | **обязательно** |
| `frontend/index.html` | SPA-шаблон (9076 стр.) | экран «Воркеры», колонка воркера в очереди, выбор при запуске, индикатор лимитов | 🟢 | **обязательно** |
| `frontend/static/js/app.js` | вся логика (19 462 стр.) | `loadWorkers()`, роут `/workers`, `worker` в live-статусе; `X-Worker-Id` через готовый хук `fetch` (`:4727`) | 🟡 | **обязательно** |
| `scripts/server/start_server.sh` | запуск uvicorn | вариант запуска в режиме воркера (без внешнего порта) | 🟢 | **обязательно** |
| `scripts/server/nginx/auditmanager.app.conf` | reverse-proxy | маршрут для WSS воркеров, лимит размера для пакетов | 🟢 | желательно |
| `~/bin/webapp-watchdog.sh` | живость по `batch_queue.json` | отдельный контракт живости для воркера | 🟡 | желательно |
| `.mcp.json` | **абсолютный** путь к norms-venv | относительный путь / генерация под хост | 🟢 | желательно |
| `requirements.txt` | 12 пакетов, неполон | новый `requirements-worker.txt` по фактическим импортам (§5.8) | 🟢 | **обязательно** |
| `backend/app/pipeline/stages/block_analysis/gemma_findings_only.py` (доп.) | `CLAUDE_CLI_BIN` в обход автодетекта (`:117`) | использовать `config.get_claude_cli()` | 🟢 | **обязательно** |
| `backend/app/pipeline/stages/prepare/prepare_service.py` (доп.) | интерлок `_check_not_in_active_batch:152` | сделать межхостовым или явно ограничить (§5.5) | 🟡 | **обязательно** |
| `backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py` | хардкод `PDF-proverka-deploy` (`:52-57`) | параметризовать корни (§5.10) | 🟢 | желательно |
| `backend/app/services/common/atomic_json.py` | `atomic_write_json` без flock (`:50-58`) | для shared-файлов использовать только `load_modify_save` | 🟡 | желательно |
| `.github/workflows/ci.yml` | observe-first, `continue-on-error: true` | добавить проверку импортов воркера | 🟢 | желательно |
| `docs/distributed_audit_workers/` | этот отчёт | + спецификация протокола, + runbook установки воркера | 🟢 | **обязательно** |
| `.env.example` | шаблон (рассинхронён ~85 %) | добавить блок `AUDIT_ROLE`/`AUDIT_DISPATCHER_URL`/`AUDIT_WORKER_*` | 🟢 | желательно |

---

## 19. Рекомендуемые автоматические тесты

Тестовая база: два корня (`tests/` 340 файлов + `backend/tests/` 70), регресс-гейт `scripts/ci_regression_gate.py` с baseline из 86 известных падений (`scripts/ci_known_failures.txt`). Существующие тесты очереди — `test_batch_queue_hardening.py`, `_parallel.py`, `_reconcile.py`, `_resilience.py`, `_selection_fixes.py`, `test_pipeline_queue_single_flight.py`, `test_resume_detector.py`, `test_pipeline_cancel_propagation.py` — их надо брать за образец и **не ломать**.

| # | Тест | Что проверяет | Тип |
|---|---|---|---|
| 1 | `test_execution_backend_local_parity` | `LocalExecutionBackend` вызывает `_dispatch_action` с теми же аргументами → поведение локального режима не изменилось | регресс |
| 2 | `test_batch_queue_item_worker_id_backcompat` | старый `batch_queue.json` **без** `worker_id` читается без ошибок; новый — сериализуется/десериализуется | совместимость |
| 3 | `test_package_roundtrip_resume_point` | собрать пакет → распаковать во временный корень → `detect_resume_stage` даёт ту же точку, что на исходном дереве | ключевой |
| 4 | `test_package_paths_portable` | после распаковки в другой корень в артефактах нет путей исходного хоста; `block_context_summary.project_dir` и `pipeline_log.artifacts_dir` переписаны | ключевой |
| 5 | `test_package_tar_preserves_hardlinks` | TAR сохраняет `nlink>1` внутри архива; ZIP — нет (защита от регресса выбора формата) | ключевой |
| 6 | `test_package_integrity_sha256` | подмена байта в архиве → приём отклонён; `input_manifest.json` сверяется | безопасность |
| 7 | `test_crops_hydrate_offline` | пакет **без** PNG + `02_work/document.pdf` → `hydrate_blocks_dir` восстанавливает все блоки без сети; `crops_materialized` = True | ключевой |
| 8 | `test_remote_heartbeat_prevents_zombie` | job с remote-heartbeat не признаётся зомби при молчании > `ZOMBIE_TIMEOUT_SEC`; `_reconcile_stale_queue` его не демотирует | 🔴 защита от §10.2 |
| 9 | `test_no_artifact_deletion_on_remote_silence` | молчание воркера не приводит к вызову `_clean_stage_files`; `03_findings.json` на месте | 🔴 защита от §10.2 |
| 10 | `test_event_seq_idempotent` | повторная доставка того же `seq` не меняет состояние; события вне порядка отбрасываются/буферизуются | ключевой |
| 11 | `test_event_replay_after_reconnect` | обрыв → воркер копит → после восстановления события приходят по возрастанию `seq`, без дыр | ключевой |
| 12 | `test_lease_prevents_double_assignment` | элемент с непросроченной арендой не выдаётся второму воркеру; после истечения — выдаётся | ключевой |
| 13 | `test_worker_cannot_write_decisions_log` | при `AUDIT_ROLE=worker` `save_expert_review` не трогает глобальный `decisions_log.json`, а пишет дельту в пакет | 🔴 защита данных |
| 14 | `test_result_merge_preserves_expert_review` | распаковка результата не затирает `04_review/expert_review.json` и `discussions/` | 🔴 защита данных |
| 15 | `test_config_fingerprint_mismatch_rejected` | воркер с другим `stage_models.json`/снапшотом промптов/git sha получает отказ в задании | ключевой |
| 16 | `test_worker_token_auth` | запрос без токена/с отозванным токеном → 401; корректный → 200 | безопасность |
| 17 | `test_worker_action_whitelist` | попытка передать воркеру произвольную команду отклоняется (закрытый enum действий) | безопасность |
| 18 | `test_slots_formula_boundaries` | формула §13.2 на граничных значениях: swap > 1 ГБ → 0; free < 2 ГБ → 0; 4 ядра → 0; 16 ядер/44 ГБ → 5 | ключевой |
| 19 | `test_package_retention_30_days` | пакет не удаляется до команды; авто-удаление по истечении срока; `extend_retention` продлевает | функциональный |
| 20 | `test_ws_relay_from_remote_event` | приём `progress_updated` от воркера → `WSMessage.progress` уходит подписчикам проекта | интеграционный |
| 21 | `test_quota_warning_on_failed_leg` | `detectors_failed` непустой → воркер шлёт `quota_warning`, центр помечает прогон деградированным | функциональный |
| 22 | `test_source_file_classification` | `is_source_file()` ([project_service.py:3774](../../backend/app/services/common/project_service.py#L3774)) остаётся единственным определением input-части пакета; список расширений не разъезжается с упаковщиком | защита от дрейфа |
| 23 | `test_worker_requirements_importable` | на чистом окружении по `requirements-worker.txt` импортируются `fitz`, `openpyxl`, `PIL`, `numpy`; `pdf_crop._require_fitz()` не падает | 🔴 иначе воркер тихо уходит в сеть |
| 24 | `test_package_without_input_manifest` | версия без `input_manifest.json` (таких ~32 из 559) упаковывается и принимается без ошибки | совместимость |
| 25 | `test_worker_mode_no_global_scans` | при `AUDIT_ROLE=worker` не запускаются `_recover_stale_pipelines` по всем проектам и `auto_resume_interrupted_batch` | защита |

Плюс: после каждого шага — `python scripts/ci_regression_gate.py` (падение только на **новых** поломках против baseline).

*В ходе аудита прогнаны существующие тесты, покрывающие описанные механизмы:* `tests/test_resume_detector.py` (3 passed), `tests/test_batch_queue_hardening.py` (7 passed), `tests/test_pipeline_cancel_propagation.py` + `tests/test_batch_queue_parallel.py` (23 passed). Они и есть тот контур, который правки по §8 не должны сломать.

---

## 20. Итоговый вердикт

**Да — распределённые audit-worker внедряются эволюционно, без переписывания пайплайна.**

Основания:

1. **Точка врезки одна.** Все пути запуска сходятся в `_dispatch_action`, а исполнение элемента локализовано в `_batch_slot_worker`. `ExecutionBackend` вставляется в одну строку ветвления; локальный режим остаётся байт-в-байт прежним.
2. **Этапы уже развязаны.** `PipelineStageContext` не держит ссылку на менеджер — все 15 stage-runner'ов переносятся на воркер без единой правки.
3. **Возобновление уже файловое.** `detect_resume_stage` — чистая функция от каталога проекта; воркер, переживший обрыв, продолжит сам, ничего не спрашивая у центра. Это половина требования «работа при потере связи», уже реализованная.
4. **Пакет проекта — просто каталог.** Базы данных нет; всё состояние — JSON/JSONL с атомарной записью. Типовой размер 100–300 МБ, TAR сохраняет хардлинки, кропы восстановимы офлайн.
5. **Авторизация CLI уже ambient.** Ни один токен Claude/Codex не проходит через центр — требование ТЗ выполняется структурно.
6. **Механизм переносимости путей уже написан** (9 env-override корней данных) — просто не задействован.
7. **Учёт лимитов уже есть.** `GlobalUsageScanner` косвенно определяет остаток по локальным логам CLI, `parse_rate_limit_reset` вытаскивает время сброса — ровно то, что требуется от воркера.

Что делает задачу нетривиальной — **не пайплайн, а три вещи**:

- 🔴 **Разделяемое глобальное состояние**: `decisions_log.json` (26 МБ, RMW под локальным flock), `norms_paragraphs.json`, `paid_cost.json`. Решается разделением прав: воркер не пишет в глобальное, отдаёт дельту, мержит центр.
- 🔴 **Механика «зомби»**. Для локальных заданий она сегодня надёжно перекрыта тремя гейтами живости, но **все три опираются на локальные сигналы** (живые дочерние процессы, живой asyncio-таск, живой batch-worker), которых у удалённого задания на диспетчере нет по определению. Требует аккуратной правки в самом чувствительном месте менеджера — это единственная часть работы, где нужна повышенная осторожность и обязательные тесты №8-9.
- 🟠 **Нормативная база**: 11 ГБ на воркер, +5,6 ГБ RAM на сессию, запись в общий файл, хрупкий venv (сломан уже сейчас). Рекомендация — оставить норм-этап в центре на первом этапе.

Отдельно стоит выделить находку факт-чека, которая портит любую модель учёта нагрузки: **точек запуска `claude -p` в коде не менее десяти, а под бюджетом `resource_budget` — только одна**. Два потребителя, которыми в комментарии обоснован сам лимит (`absence_guard` и `decision_carryover`, 8 процессов из 20), слот не берут вовсе, и в реестре `_active_processes` они тоже не появляются. Пока это не сведено к общему `process_runner`, ни «сколько CLI сейчас работает», ни «сколько слотов свободно» на воркере посчитать честно нельзя. Это не блокер запуска, но обязательное условие для **автоматического** режима (шаг 5).

Оценка: **шаги 1-3 из §17 дают работающий ручной режим** («вижу все VPS, назначаю проект, вижу прогресс и лог, получаю результат») — это и есть основной режим первого этапа по ТЗ. Шаги 4-5 добавляют отказоустойчивость и автоматику.

Отдельно отмечу два уточнения, вскрытых на этапе критики полноты и уже внесённых в отчёт: **`PipelineManager` — не единственный владелец фоновых LLM-заданий** (есть ещё `section_optimization`, `stage_comparison/pipeline_queue` и `prepare`, каждый со своим реестром задач; §5.5), и **инвентарь зависимостей неполон** — воркер, собранный строго по `requirements.txt`, тихо теряет локальную вырезку кропов и уходит в сеть на портал (§5.8). Ни то, ни другое не меняет вердикт, но обе вещи обязаны попасть в runbook установки воркера.

Формально в отчёте не закрыт один пункт задания — **готовой точки регистрации внешних воркеров в кодовой базе нет** (проверено: `external_register.py` — это реестр замечаний заказчика, а не хостов; grep по роутерам на `worker`/`node`/`agent`/`api_key`/`bearer` даёт только комментарии). Это не препятствие, а констатация: подсистему придётся создавать с нуля, но она аддитивна и ничего существующего не ломает.

---

## Приложение А. Ключевые файлы для дальнейшей работы

```
backend/app/pipeline/manager.py                          — диспетчер + исполнитель (точка врезки :5878)
backend/app/pipeline/context.py                          — контракт этапа (готов к сериализации)
backend/app/pipeline/resume_detector.py                  — файловое возобновление
backend/app/services/common/process_runner.py            — запуск подпроцессов (уезжает на воркер)
backend/app/services/common/audit_logger.py              — единая воронка статусов/логов/WS
backend/app/services/common/resource_budget.py           — бюджеты CLI/MCP
backend/app/services/common/cpu_pool.py                  — пул процессов с пиннингом
backend/app/services/common/usage_service.py             — учёт токенов + GlobalUsageScanner
backend/app/services/common/cli_utils.py                 — детект лимитов и времени сброса
backend/app/services/common/block_crop_store.py          — офлайн-восстановление кропов
backend/app/services/common/audit_scope.py               — изоляция путей параллельных проектов
backend/app/services/common/atomic_json.py               — RMW под flock (граница безопасности KB)
backend/app/services/llm/claude_runner.py                — запуск Claude CLI
backend/app/services/llm/codex_runner.py                 — запуск Codex CLI + guard норм-MCP
backend/app/services/storage/v2_primary_wiring.py        — job_id → runs/<job_id>
backend/app/services/storage/projects_v2_adapter.py      — резолв документа/версии
backend/app/ws/manager.py                                — ретрансляция в браузер
backend/app/core/config.py                               — все пути и env-override
backend/app/core/action_log.py                           — сквозной журнал действий
backend/app/api/routers/audit.py                         — API очереди и статуса
backend/app/api/routers/export.py                        — прототип упаковки результатов
frontend/index.html + frontend/static/js/app.js          — живой фронтенд
scripts/server/{start_server.sh, nginx/*}                — развёртывание
```

## Приложение Б. Сводка проверенных чисел

| Показатель | Значение | Как получено |
|---|---|---|
| Корпус `projects_v2/objects` | 32 ГБ, 477 документов, 559 версий, 199 016 файлов | `du`, `find` |
| Дополнительно в `projects_v2/_system` | ещё 414 каталогов `versions` (shadow-зеркало), 3,2 ГБ | `find`, `du` |
| Размер версии | медиана 30 МБ, p90 125 МБ, p95 170 МБ, max 637 МБ | `du` по всем версиям |
| Хардлинки | 36 673 из 199 016 файлов (18 %), из них 34 932 — PNG кропов | `find -links +1` |
| Раздувание при ZIP | +40 % (63 → 88 МБ на примере версии) | `du` vs `du -l` |
| Норм-база | 6,6 ГБ (venv 4,9 + npz 1,6 + vault 89 МБ) + HF-модели 4,3 ГБ | `du` |
| `decisions_log.json` | 26,5 МБ | `ls` |
| Переменных в `.env` | 138 (в `.env.example` — 20) | `grep -c` |
| Хост центра | 16 ядер, 62 ГБ RAM (44–45 доступно), диск 296 ГБ (179 свободно) | `nproc`, `free`, `df` |
| Прод-параллельность | `BATCH_MAX_PARALLEL=5`, `BUDGET_CODEX_CLI=20`, `BUDGET_CLAUDE_CLI=6`, `BUDGET_NORMS_MCP=5` | `.env` |
| RAM норм-MCP | 5,6 ГБ на сессию (не выгружается) | комментарий `resource_budget.py:55-58` |
| Живость `crop_url` | 85 % (33/39) | `docs/block_crop_lifecycle.md` |
| `app.js` | 19 462 строки, один Vue-компонент | `wc -l` |
| `manager.py` | 6871 строка | `wc -l` |
| Тесты | 346 + 72 файла, baseline известных падений — 86 | `ls`, `grep -c .` |
