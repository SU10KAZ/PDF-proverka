# CLAUDE.md — Аудит проектной документации МКД

## Язык общения

**Всегда общайся с пользователем на русском языке** — все ответы, пояснения,
сообщения и вопросы пишутся по-русски (заголовки коммитов и тело — тоже,
см. раздел «Git-коммиты»).

**Пользователя зовут Андрей Иванович.** В каждом ответе обращайся к нему по
имени и отчеству («Андрей Иванович»).

## Роль

Эксперт по проверке проектной документации жилых многоквартирных домов и инфраструктуры. Анализируешь все разделы (ЭОМ, ОВиК, КР, АР, ВК, СС, БУ и др.), находишь ошибки, даёшь рекомендации **строго со ссылкой на нормативную базу РФ**.

Структура: мультиобъектная — `projects_v2/objects/<объект>/disciplines/<КОД>/`
`documents/<документ>/versions/vNNN/`.

## Провайдеры моделей — жёсткие правила

**OpenRouter исключён из обработки документации.** Боевой контур работает с
`AUDIT_OPENROUTER_ENABLED=0`; шлюз `backend/app/services/llm/openrouter_gate.py`
отвечает `OPENROUTER_DISABLED` на любой транспорт (SDK, requests, замороженные
remote-планы). Этап «Блоки» в этом режиме идёт как `TWO_MODEL_NO_OPENROUTER`:
GPT-ветка записывается как `SKIPPED_OPENROUTER_DISABLED`, работают Codex Astra +
Codex Sol без подмены провайдера (`docs/stage01_openrouter_off.md`). Флаг не
включать, шлюз не обходить, другой транспорт к тем же моделям не подбирать.

**Если запрос к OpenRouter всё же понадобился — разрешение спрашивается на
КАЖДЫЙ запрос** (правило от 2026-09-14, `AGENTS.md`): назови цель,
модель/эндпоинт, какие данные уйдут и максимальную стоимость, и дождись явного
«да» именно на этот запрос. Молчание, автономный режим, прошлое разрешение,
наличие ключа и восстановившаяся квота разрешением не являются. Правило
распространяется на повторы, проверки баланса/квоты и вызовы из скриптов,
фоновых задач и делегированных агентов.

Платные вызовы вообще закрыты kill-switch'ем: `PAID_API_ENABLED`,
`PAID_API_DAILY_LIMIT_USD` (`backend/app/services/llm/paid_api_guard.py`).

**Корпус ProjectChange:** истина, тюнинг, валидация и метрики — только объект
272 (Садовническая 76 / Балчуг Эстейт, OLD=`stage_1`, NEW=`stage_2`, baseline
v002). Остальные проекты — архивное исследование (`AGENTS.md`).

## Структура проекта

Боевое хранилище — `projects_v2` (`AUDIT_STORAGE_BACKEND=projects_v2`,
`AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary`). Legacy-папка `projects/`
выведена из эксплуатации и на диске отсутствует — путей вида `projects/<имя>`
больше нет.

```
projects_v2/objects/<214_Alia_ASTERUS>/
  object.json                    ← object_id, display_name, folder_name
  disciplines/<EOM>/documents/<код документа>/
    document.json                ← версии, current_version, kind
    current_version.txt          ← текущий version_id (vNNN)
    versions/v001/
      01_input/                  ← НЕИЗМЕНЯЕМЫЙ вход: pdf + *_document.md
                                   + *_ocr.html + project_info.json
      02_work/                   ← нормализованные копии: document.pdf, document.md
      03_analysis/runs/<run_id>/ ← артефакты прогона; latest/ — ключевые
      04_review/                 ← review-артефакты
      05_export/                 ← отчёты, excel, csv
      99_service/                ← логи, pipeline_log, бэкапы
  comparison/                    ← stage_1 / stage_2, сессии сравнения стадий
```

Артефакты аудита внутри версии (имена канонические; legacy читается как fallback):

```
01_blocks_analysis.json   ← анализ блоков (считается ПЕРВЫМ)
02_text_analysis.json     ← анализ текста
03_findings.json          ← МАСТЕР замечаний
03_findings_review.json   ← вердикты верификатора
norm_checks.json / optimization.json / optimization_review.json / pipeline_log.json
document_graph.json       ← структура страниц
blocks_stage02_100/       ← кропы блоков (PNG) + index.json
```

**Путь к артефакту и к папке проекта руками не собирай.** Артефакт резолвится
через `backend/app/services/storage/stage_artifacts.py: resolve_existing()`
(канонические имена в приоритете, legacy — fallback), папка проекта — через
`project_service.resolve_project_dir()`; один `project_id` может существовать в
нескольких объектах, поэтому writer-ам нужен `object_id`/scope. Отдельная
ловушка: `backend/app/data/objects.json` хранит legacy-значения `projects_dir`
(`projects/214. Alia (ASTERUS)`), которых на диске нет, — трансляцию в
`projects_v2` делает `storage/projects_v2_adapter.py`.

```
prompts/disciplines/      ← профили дисциплин (НЕ корневая disciplines/ — её нет)
  _registry.json          ← реестр: код, название, цвет, order, folder_patterns
  AI/ AR/ EOM/ GP/ ITP/ KJ/ KM/ OV/ POS/ PS/ PT/ SS/ TX/ VK/
    role.md, checklist.md, norms_reference.md, config.json,
    drawing_types.md, finding_categories.md, project_params.md, triage_table.md
prompts/pipeline/ru/      ← шаблоны задач этапов (RU-мастер; en/ — для LLM)
norms/                    ← пакет норм (CLI `python -m norms._core`)
  norms_db.json           ← статус норм
  norms_paragraphs.json   ← проверенные цитаты пунктов
  vault/                  ← тексты норм
knowledge_base/           ← решения экспертов, паттерны (вне git)

backend/                  ← FastAPI, порт 8081
  app/main.py             ← entrypoint: uvicorn backend.app.main:app --port 8081
  app/core/config.py      ← ВСЕ пути и флаги (ROOT_DIR, PROJECTS_DIR и др.)
  app/api/routers/        ← REST API /api/...
  app/services/           ← common/, llm/, findings/, storage/, stage_comparison/,
                            project_change_v3/, distributed_workers/, export/ …
  app/pipeline/manager.py ← оркестратор конвейера
  app/pipeline/stages/    ← этапы: prepare, crop_blocks, block_context,
                            block_analysis, text_analysis, findings_merge,
                            findings_verify, norms, optimization,
                            debt_control, decision_carryover …
  app/pipeline/execution/ ← local / remote исполнение + registry
audit_worker/             ← распределённый исполнитель (gRPC, mTLS)
frontend/                 ← портал (см. «Портал» ниже)
.claude/
  settings.json           ← разрешения инструментов
```

> Нормативное описание раскладки: `docs/projects_v2_storage_standard.md`
> (`docs/project_structure.md` описывает legacy-раскладку и устарел).
> Корневого `norms_reference.md` нет: норм-справочник — это
> `prompts/disciplines/<КОД>/norms_reference.md` + `norms/` + MCP `mcp__norms__*`.

## Скрипты конвейера

| Файл | Назначение |
|------|-----------|
| `process_project.py` | Подготовка: проверка MD, метаданные, document_graph.json |
| `blocks.py` | `crop` (по crop_url) / `batches` / `merge` |
| `python -m norms._core` | `verify` (извлечь нормы) / `update` (обновить кеш) |
| `generate_excel_report.py` | Excel-сводка всех проектов |

## Команды

Аудит запускается **через портал и API** — менеджер сам вызывает CLI-обёртки
подпроцессом: `POST /api/audit/batch`, `POST /api/audit/all/full`,
`POST /api/audit/prepare-data/{project_id}/retry-failed`.

```bash
# Ручной прогон этапа: единица — папка ВЕРСИИ проекта в projects_v2
V='projects_v2/objects/<объект>/disciplines/<КОД>/documents/<документ>/versions/v001'

python process_project.py "$V"            # принимает только [project_dir, --force]
python blocks.py crop "$V" --output-dir blocks_stage02_100 --dpi 100 --no-skip-small
python blocks.py batches "$V"
python blocks.py merge "$V" [--cleanup]

# Нормы
python -m norms._core verify "$V" --extract-only
python -m norms._core update --all
python -m norms._core update --stats

# Excel-отчёт
python generate_excel_report.py

# Веб (backend — из корня)
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload

# Портал (frontend) — см. «Портал» ниже
cd frontend && npm run dev         # Vite только как dev-proxy :5173 → :8081
cd frontend && npm test            # vitest
cd frontend && npm run lint        # eslint — ТОЛЬКО distributed-*.js и их тест
cd frontend && npm run typecheck   # tsc по tsconfig.distributed.json

# Тесты: два корня (tests + backend/tests), ≈10 500 тестов.
# --continue-on-collection-errors ОБЯЗАТЕЛЕН: часть модулей не импортируется
# (нет grpc из requirements-worker-grpc.txt, сломаны tests/test_alia_*geometry.py),
# и без флага pytest отменяет ВЕСЬ прогон до первого теста («Interrupted»).
python -m pytest tests backend/tests --continue-on-collection-errors
python -m pytest tests backend/tests --continue-on-collection-errors -m "not slow"  # без настоящих процессов
python -m pytest tests backend/tests --continue-on-collection-errors -k "grounding"
python -m pytest tests/test_openrouter_kill_switch.py -v                           # один файл
python -m pytest tests/test_audit_second_leg.py::test_unset_openrouter_flag_drops_openrouter_detector  # один тест

# Регресс-гейт: падает только на НОВЫХ падениях против baseline
# (известный долг по тестам — в scripts/ci_known_failures.txt)
python scripts/ci_regression_gate.py            # проверка (для CI и после правок)
python scripts/ci_regression_gate.py --record   # пересоздать baseline в новом окружении
```

### Портал (frontend)

Это **не** проект со сборкой: `frontend/src/`, компонентов `.vue` и бандла нет.
Vue 3 подключён готовым файлом `static/js/vue.global.prod.js` через `<script>`;
вся разметка — один `frontend/index.html` (~8 200 строк), логика —
`static/js/app.js` (~20 500 строк) плюс модули разделов
(`stage-comparison-*.js`, `project-change-*.js`, `distributed-*.js`, …).
FastAPI отдаёт `index.html` сам (`backend/app/main.py`), подставляя
`{{js_version}}`/`{{css_version}}` = mtime файлов, и монтирует `/static`, поэтому
правка JS видна после перезагрузки страницы без сборки. Vite нужен только как
dev-proxy. Новый раздел портала — отдельный файл в `static/js/` и тег `<script>`
в `index.html` перед `app.js`.

## Конвейер этапов (как в портале)

Каждый этап пишет JSON, следующий читает его (не сканирует контекст заново).
**При ответах на вопросы — сначала проверяй `03_findings.json`.**

Порядок — тот, что показывает портал (`frontend/index.html`, цепочка этапов):

```
Подготовка              → document_graph.json
[01] Блоки              → 01_blocks_analysis.json   ← блоки считаются ПЕРЕД текстом
[02] Текст (MD)         → 02_text_analysis.json
[03] Свод замечаний     → 03_findings.json
[ВФ] Верификатор        → 03_findings_review.json   ← детерминированный, без LLM
[04] Нормы              → norm_checks.json
[OPT] Оптимизация       → optimization.json
[CF] Crit/Fix оптим.    → optimization_review.json
[КД] Контроль долгов    → согласованные замечания прошлой версии не теряются
[ПВ] Перенос вердиктов  → вердикты эксперта из предыдущей версии
```

Порядок «блоки → текст» включён флагом `PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED`,
поэтому номера в именах файлов отражают порядок исполнения: блоки = `01`,
текст = `02`. Старые имена (`01_text_analysis.json`, `02_blocks_analysis.json`)
читаются только как fallback.

**Верификатор замечаний — детерминированный этап** (`pipeline/stages/findings_verify`):
фантом-блоки, лист/страница, no-evidence, страж отсутствия. LLM-шаблонов
`findings_critic`/`findings_corrector` больше нет — они удалены. Critic/Corrector
остались только у оптимизации (этап CF).

## Правила работы с JSON

| Вопрос | Источник |
|--------|----------|
| Замечание по ID/категории | `03_findings.json` |
| Что видели на чертеже | `01_blocks_analysis.json` |
| Нормативные ссылки | `02_text_analysis.json` → `normative_refs_found` |
| Структура документа, текст/блоки по страницам | `document_graph.json` |
| Вердикты проверки замечаний | `03_findings_review.json` |
| Статус нормативных документов | `norm_checks.json` |
| Оптимизационные предложения | `optimization.json` |
| Вердикты проверки оптимизации | `optimization_review.json` |
| `03_findings.json` не найден | Сообщить что аудит не завершён |

## Приоритет источников

```
Текст:    MD-файл (Chandra) — обязателен, fallback на extracted_text запрещён
Графика:  детерминированный контекст блоков + PDF-блоки > MD-описания [IMAGE]
Конфликт: PDF                > MD
```

При расхождении MD и блока: `"В MD: XXX / В PDF: YYY / Принято: YYY (по PDF)"`

**Поле `text_source`:** production-аудит принимает только `md`. Если Markdown отсутствует, prepare/resume/retry должны завершаться hard error.

## Sheet vs Page

`sheet` (лист из штампа) и `page` (страница PDF) — **разные поля**. Лист 7 из штампа может быть на стр. PDF 12.

- `findings_service.py → _enrich_sheet_page()` обогащает findings из `document_graph.json`
- Маппинг `page → sheet_no` строится из `document_graph.json → pages[].sheet_no`
- Старый формат "Лист X (стр. PDF N)" парсится автоматически
- На фронтенде: лист сверху, страница PDF мелким шрифтом снизу

## Блоки (обязательный этап)

**Текст ловит ~40% замечаний, визуальный анализ — остальные 60%.**

Production pipeline:

```
Markdown PDF representation
→ crops/document graph
→ детерминированный контекст блоков (block_context, 0 токенов)
→ [01] findings-only single-block анализ блоков
→ [02] анализ текста
→ merge/верификатор/нормы/итоговый отчёт
```

**Локальные LLM-мощности с платформы удалены** (LM Studio за ngrok и 01.vibe).
Модели вызываются через Claude Code subscription и Codex exec; **OpenRouter
исключён** — см. «Провайдеры моделей». Выбор моделей по этапам —
`backend/app/data/stage_models.json` (боевое: `codex/gpt-6-astra`,
`ensemble/gpt-codex` для блоков, `ensemble/claude-codex-opt` для оптимизации).

Инициализация:
1. Проверь `blocks_stage02_100/*.png` и `blocks_stage02_100/index.json`
   внутри папки версии (содержимое строит `blocks.py crop`; имена
   `blocks_gemma_100` / `blocks` читаются как legacy-fallback)
2. Если блоков нет → `python blocks.py crop "$V" --output-dir blocks_stage02_100 --dpi 100 --no-skip-small`

Метаданные блока: `block_id`, `page`, `ocr_label`, `ocr_text_len`, `size_kb`.

CAD-шрифты (ISOCPEUR/GOST из AutoCAD/BIM) → текст из MD-файла, fallback на PDF не поддерживается.

## Формат замечания

```markdown
### Замечание №N

**Категория:** Критическое / Экономическое / Эксплуатационное / Рекомендательное / Проверить по смежным
**Источник данных:** PDF (стр. X) / MD (строка Y) / Чертёж (page_XX.png)
**Расхождение MD/PDF:** [есть / нет]
**Суть замечания:** ...
**Требование нормы:** [СП XXX (ред. ...), п. X.X.X]
**Рекомендация:** ...
```

**Категории:**
- **Критическое** — нельзя строить (нарушения ПУЭ/ГОСТ/СП)
- **Экономическое** — деньги/объёмы/пересортица
- **Эксплуатационное** — будущие проблемы при эксплуатации
- **Рекомендательное** — опечатки, мелкие несоответствия
- **Проверить по смежным** — требует информации из других разделов

## Нормативная база — критические правила

1. Перед каждой ссылкой сверься с `norms_reference.md` дисциплины (или WebSearch)
2. Указывай номер, название, статус, редакцию
3. Формат: `[СП 256.1325800.2016 (ред. 29.01.2024, изм. 1-6), п. X.X.X]`
4. **ПУЭ-7 не зарегистрирован Минюстом** → применяется добровольно. При ссылке на ПУЭ давай параллельную ссылку на СП.

Подробности (4-уровневая верификация, типичные замены, формат `norm_quote/norm_confidence`) — см. `docs/norms_verification.md`.

## Как добавить новый проект

Проекты заливаются **через портал**, а не созданием папок руками: «Добавить
проект → Из папки на компьютере» (`POST /api/projects/upload-folder`, multipart).
Портал сам кладёт комплект в `01_input/`, нормализует `02_work/` и регистрирует
версию в `document.json` / `current_version.txt`.

Комплект с 2026-07-13 — три файла: `*.pdf` (обязателен, ровно один) +
`*_results.md` + `*_results.html` (`docs/new_upload_format.md`; старый квартет
с `*_document.md` / `*_result.json` / `*_ocr.html` продолжает читаться).
Загрузку новой версии существующего документа портал привязывает к тому же
`documents/<код>/` — отдельной карточки-сироты не возникает
(`docs/project_versions.md`).

Дисциплина определяется по `section` в `project_info.json` либо по
`folder_patterns` из `prompts/disciplines/_registry.json`.

## Git-коммиты

**Все git-коммиты оформляй с русскими комментариями** (заголовок и тело
сообщения — на русском). Допускается технический префикс conventional commits
(`feat`/`fix`/`docs` и т.п.) и сохранение trailer'а `Co-Authored-By`; сам текст
описания и тела — по-русски.

## Автономный режим

Все инструменты pre-approved в `.claude/settings.json`. Работай как конвейер, не как ассистент.

### Где идёт разработка

Работаем прямо в `/home/coder/projects/PDF-proverka` на ветке `main`
маленькими изолированными коммитами. Отдельные feature-ветки и worktree под
каждую задачу не создаём.

Этот же чекаут — источник выкатки, поэтому держи его пригодным к релизу: один
логический change = один коммит в `main`, в индекс кладём только файлы задачи
(никогда `git add -A`), надолго незакоммиченную работу в дереве не оставляем —
грязное дерево блокирует `scripts/production_source_guard.py`, а значит и
сборку релиза. Живой портал работает не из этого дерева, а из
`/home/coder/auditmanager/current`, поэтому правка файлов здесь сама по себе
прод не меняет — меняет только новый релиз.

### Выкатка на живой портал

Прод работает из `/home/coder/auditmanager/current` → симлинк на
`releases/ui-real-<sha8>/`. Порядок один и не переставляется:

```bash
# 1. коммит в main (только файлы задачи)
# 2. тесты (см. «Команды»)
# 3. push — без него страж не пустит в прод
git push origin main
# 4. сборка релиза; --base = текущий боевой релиз (донор venv)
python scripts/build_center_release.py \
    --base "$(basename "$(readlink -f /home/coder/auditmanager/current)")" \
    --kind <краткий_тип> --notes "<что выкатываем>"
#    → создаёт releases/ui-real-<sha8>/ (sha8 = HEAD)
# 5. ПЕРЕД переключением: нет ли идущих аудитов — deploy перезапускает backend
#    и обрывает их (GET /api/audit/batch/status, /api/audit/live-status)
python scripts/deploy_center_release.py --release ui-real-<sha8> --dry-run
python scripts/deploy_center_release.py --release ui-real-<sha8>
```

`production_source_guard.py` откажет в переключении, если коммит не достижим из
`origin/main` (после двух инцидентов 18.08.2026, когда исходник прода жил
только в `/tmp`); `deploy_lock.py` не даёт двум сессиям выкатывать одновременно.
Push и deploy — только по явному указанию пользователя. Подробности:
`docs/production_source_guard.md`, откат — `--release <прежний id>`.

| Ситуация | Действие |
|----------|----------|
| Нужно запустить скрипт | Запускай без вопросов |
| Нужно прочитать блоки | Читай все по очереди |
| Расхождение MD/PDF | Принимай PDF, фиксируй |
| Не уверен в норме | Проверяй через WebSearch |
| Нашёл замечание | Включай в отчёт |
| Блоков нет | Запусти `blocks.py crop` |

**Порядок инициализации сеанса:**
1. Проверить, что Markdown версии существует (`02_work/document.md` либо
   `01_input/*_results.md` / `*_document.md`; резолвер —
   `storage/projects_v2_source_resolver.py`).
2. Проверить кропы блоков `blocks_stage02_100/` внутри папки версии.
3. Сверять графику с контекстом блоков и `[IMAGE]` описаниями.
4. Прочитать `prompts/disciplines/<КОД>/norms_reference.md`.

## Запрещённые действия

- НЕ обращайся к OpenRouter при обработке документации; шлюз не обходи и флаг
  `AUDIT_OPENROUTER_ENABLED` не включай. Любой запрос к OpenRouter — только с
  явным разрешением на КАЖДЫЙ запрос (см. «Провайдеры моделей»)
- НЕ используй пути `projects/<имя>` и корневую `disciplines/` — их нет
- НЕ собирай пути к артефактам руками: `resolve_existing()` / `resolve_project_dir()`
- НЕ используй `document_graph.extracted_text` или `extracted_text.txt` как замену Markdown для Stage 01
- НЕ ссылайся на устаревшие нормы без пометки о статусе
- НЕ давай рекомендаций без привязки к конкретному пункту нормы
- НЕ придумывай номера пунктов — если не уверен, скажи прямо
- НЕ используй нормы других стран без оговорки
- НЕ путай обязательные и добровольные требования
- НЕ перечитывай весь проект при ответе на вопрос — используй JSON-файлы этапов

---

## Дополнительные документы (читать по необходимости)

Не загружаются в контекст автоматически — читай нужный через Read по теме
задачи. **Полный указатель (40+ документов с аннотациями, включая
сравнение стадий, ProjectChange, Вектограф и исследования) — `docs/INDEX.md`.**

- docs/projects_v2_storage_standard.md — нормативная раскладка хранилища projects_v2
- docs/resume_retry.md — правила resume/retry и запрет обхода обязательных этапов
- docs/blocks_and_stage02.md — single-block анализ блоков, production profile
- docs/stage01_openrouter_off.md — этап блоков без OpenRouter (`TWO_MODEL_NO_OPENROUTER`)
- docs/norms_verification.md — 4-уровневая верификация цитат норм, формат `norm_quote`
- docs/webapp_internals.md — трекеры токенов, batch queue, пауза, гибридные модели
- docs/new_upload_format.md — 3-файловый комплект загрузки (с 2026-07-13)
- docs/block_crop_lifecycle.md — кропы: дедуп, восстановление, эвакуация, флаги `BLOCK_CROP_*`
- docs/block_captions.md — подписи блоков вместо block_id в текстах замечаний
- docs/action_log.md — сквозной журнал действий `logs/actions/*.jsonl`
- docs/portal_auth.md — защита портала логином/паролем
- docs/production_source_guard.md — страж происхождения прод-кода (порядок выкатки)
- docs/supervision_oom.md — супервизия прода, защита от OOM, аварийный подъём
