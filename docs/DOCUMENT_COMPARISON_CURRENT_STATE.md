# Текущее состояние функционала «Сравнение документации»

Дата аудита: 11 августа 2026 года.

## 0. Резюме и границы аудита

В интерфейсе раздел называется **«Сравнение стадий»**, хотя в постановке задачи используется название «Сравнение документации». Его основной production-путь сейчас устроен так:

1. пользователь выбирает объект в общей верхней панели и загружает ZIP-архивы `stage_1` / `stage_2`;
2. backend рекурсивно находит PDF и связывает их преимущественно по похожести имён файлов;
3. пользователь при необходимости исправляет пары PDF и выравнивание страниц;
4. сравнение выполняет Claude Opus по **заранее подготовленным enriched Markdown** двух PDF;
5. ответ модели нормализуется в список семантических `changes` и сохраняется в файловый `comparison_result.json`;
6. основной пользовательский экран «Расхождения» накладывает на этот список детерминированную классификацию инженерной значимости и экспертные решения;
7. принятые изменения можно повторно открыть и экспортировать в XLSX.

Раздел не использует БД, очередь задач, WebSocket/SSE или платформенный `version_service`. Его runtime-состояние — дерево JSON/Markdown/PNG-файлов под `comparison/`. Отдельный **Pipeline V2 β** уже умеет детерминированно нормализовать документы, сопоставлять блоки, извлекать сущности и строить entity-delta, но запускается из UI как отдельный offline dry-run и не является источником основного `comparison_result.json`.

Отчёт — результат статического трассирования текущего checkout. Значения переменных окружения конкретного production-процесса, наличие Claude CLI и фактическое содержимое внешних каталогов в коде не зафиксированы; такие места отмечены как **«Требует дополнительной проверки»**.

Ключевые точки входа:

- `frontend/index.html` → блок `currentView === 'stage-comparison'`;
- `frontend/static/js/app.js` → загрузка стадий, `scQOStartConfirmed`, `scLoadV2Changes`;
- `backend/app/api/routers/stage_comparison.py` → router с prefix `/api/stage-comparison`;
- `backend/app/main.py` → `app.include_router(stage_comparison.router)`;
- `backend/app/services/stage_comparison/store.py` → файловые сессии и пары;
- `backend/app/services/stage_comparison/enriched_comparison.py` → основной Opus-анализ;
- `backend/app/services/stage_comparison/v2_review.py` → основной пользовательский список расхождений;
- `backend/app/services/stage_comparison/pipeline_v2_dry_run.py` → независимая β-ветка.

## 1. Фактическая архитектура

```text
Пользователь
    ↓ hash-route /stage-comparison
Vue-приложение в одном HTML/JS bundle
    frontend/index.html + frontend/static/js/app.js
    ↓ REST, периодический GET статуса jobs
FastAPI router /api/stage-comparison
    backend/app/api/routers/stage_comparison.py
    ├─ objects.py / stage_upload.py
    │      → объект из общей верхней панели
    │      → ZIP-загрузка comparison_sources/<object>/stage_{1,2}
    ├─ scanner.py → PDF + соседние MD/result.json + пары PDF
    ├─ store.py/alignment.py/blocks.py
    │      → pair.json, page_alignment.json, links.json, PNG страниц/crop
    ├─ opus_only.py / unified_analysis_jobs.py
    │      ↓
    ├─ unified_analysis.py
    │      ↓ читает готовые left_enriched.md/right_enriched.md
    ├─ enriched_comparison.py
    │      ↓ subprocess `claude -p`, модель по умолчанию claude-opus-5
    │      ↓
    │   pairs/<pair_id>/enriched_comparison/comparison_result.json
    │      ↓
    ├─ unified_findings.py → page/location projection
    ├─ v2_review.py → impact filter + review overlay
    ├─ expert_review.py → решения пользователя
    │      ↓
    └─ XLSX endpoints
           ↓
Frontend: таблица «Расхождения», переход к паре/листу, вкладка «Отчёт»

Отдельная ветка:
result.json + PDF/MD → Pipeline V2 offline dry-run → pipeline_v2/*.json
                                         ↓
                              отдельная β-панель/preview, не основной result
```

Вместо блока «БД» в фактической схеме находится файловое хранилище. Сравнительных ORM-моделей или SQL-таблиц в репозитории нет. Это подтверждается тем, что все пути формирует `backend/app/services/stage_comparison/paths.py`, а запись выполняют `store.py`, `enriched_comparison.py`, `expert_review.py` и связанные файловые сервисы.

### 1.1. Процесс выполнения и фоновые задачи

Длительные операции создаются через `asyncio.create_task` внутри процесса API:

- session-wide Opus: `unified_analysis_jobs.py` → `start_unified_job_in_background`;
- legacy graphic LLM: `jobs.py` → `start_job_in_background`;
- text-only debug: `text_llm_jobs.py`;
- visual equivalence: `visual_block_equivalence_jobs.py`;
- Pipeline V2 UI-run: `pipeline_v2_run_jobs.py`.

Статус записывается в JSON, а frontend опрашивает GET endpoint примерно раз в 3 секунды (`app.js` → `scPollUnifiedJob`, `scOpusPoll`, `scPollTextLLMJob`, `scPv2RunPoll`). Внешнего broker/worker (Celery, RQ и т.п.) в этом контуре нет. После перезапуска процесса сохранённый `running` job без живой task переводится в interrupted/failed-interrupted соответствующим job-сервисом. Resume с точки прерванного LLM-вызова не реализован.

WebSocket в общем frontend есть для аудита (`/ws/audit`, `/ws/global`), но stage-comparison его не использует. В router/service этого раздела нет SSE или `text/event-stream`.

## 2. Полный пользовательский сценарий

### 2.1. Открытие раздела и выбор стадий

1. Переход по `#/stage-comparison` выбирает view `stage-comparison` и вызывает `scLoadObjects`. Каноническая сессия при входе больше не открывается.
2. UI показывает название «Сравнение стадий» и вкладки:
   - «1. Загрузка документации»;
   - «2. Связь блоков»;
   - «3. Расхождения»;
   - «4. Отчёт»;
   - «Pipeline V2 β».
3. Единственный пользовательский выбор объекта — глобальный selector в верхней панели. Локальные selector'ы объекта/стадий и кнопки «Открыть проект», «Обновить каноничную» и «Авто сопоставление листов» удалены из рабочей панели.
4. UI фиксирует пару `stage_1` / `stage_2` и показывает две ZIP-загрузки. `POST /objects/{object_id}/stages/{stage_name}/upload` проверяет архив, защищается от path traversal/символических ссылок/лимитов, требует хотя бы один PDF и только затем заменяет целевую stage-папку. Предыдущая версия переносится в `_stage_upload_backups`.
5. Если для точной пары путей есть сессия, `scTryAutoLoadSession` открывает её. Если её нет, но в обеих стадиях есть PDF, UI автоматически вызывает `POST /sessions`.

### 2.2. Сканирование и пары PDF

6. `stage_comparison.py` → `create_session` вызывает `store.create_session`.
7. `scanner.py` → `scan_stage_folder` рекурсивно находит `*.pdf`. Для каждого PDF рядом ищутся:
   - MD: `<stem>.md`, затем `document.md`, затем первый `.md` в папке;
   - result JSON: `<stem>_result.json`, `<stem>.result.json`, `result.json`, затем первый `*result*.json`, кроме annotation.
8. Примечание: docstring `_find_result_json_near` обещает ещё `<stem>.json` с `pages[].blocks[]`, но фактической ветки проверки такого файла в функции нет. Это подтверждённое расхождение документации и реализации.
9. `scanner.py` → `match_pdfs` строит жадное one-to-one сопоставление PDF по нормализованному имени:
   - lowercase, без `.pdf`, пунктуации и суффиксов «копия»;
   - удаляются шумовые слова `stage`, `rev`, `v1`…`v4`, `old/new`, `draft/final`, `ПД/РД/ИИ`, `изм`;
   - похожесть — `difflib.SequenceMatcher`;
   - `score >= 0.97` → `matched`;
   - `0.70 <= score < 0.97` → `maybe`;
   - остальные → односторонние `unmatched`.
10. Сессия сохраняет абсолютные пути стадий, время создания и массив пар. Каждая сторона пары содержит `pdf_path`, `md_path`, `result_json_path`, относительный путь, filename/stem и признаки наличия артефактов.
11. После загрузки и автосоздания сессии пользователь может:
   - подтвердить все `maybe`;
   - назначить другой правый PDF;
   - создать пару вручную из несопоставленных PDF;
   - отвязать/soft-disable или hard-delete пару;
   - изменить порядок пар.

### 2.3. Карта страниц и существующие связи блоков

12. Для новой пары начальная карта страниц позиционная: `1↔1`, `2↔2`, а лишний хвост одной стороны связывается с `null` (`store.py`/`alignment.py`).
13. Пользователь может менять карту вручную: вставлять пустую сторону, перемещать или удалять страницу одной стороны, сбрасывать/сохранять карту (`alignment.py`; соответствующие `/page-alignment/*` endpoints).
14. Автоматическое сопоставление листов, ИИ-доматчинг, ручное/IoU-связывание блоков и pair-template удалены из UI, frontend-обработчиков и публичного API перед повторной реализацией.
15. Вкладка «Связь блоков» сохраняет только `MD enrichment`, просмотр двух страниц и уже существующих связей. `blocks.py` нормализует блоки из `result.json`, а `store.py`/router рендерят страницу и crop через PyMuPDF.
16. Для новых сессий создаётся пустой `links.json`; старые сохранённые связи и шаблонные файлы на диске не удаляются, но шаблоны больше не обнаруживаются и не применяются автоматически.

### 2.4. Запуск основного сравнения

18. На вкладке загрузки пользователь выбирает пары и нажимает «Обработать»/«Обработать выбранные». Текущий диалог предлагает:
   - запустить только Opus по уже готовым enriched MD;
   - сначала сделать backup и удалить текущий `comparison_result.json`, затем запустить Opus.
19. Несмотря на старые комментарии «Qwen enrichment → Opus», текущий `unified_analysis.py` **не создаёт новые Qwen-описания**. Он требует готовые `text_enrichment/left_enriched.md` и `right_enriched.md`. Пары без них пропускаются. Старый формат enriched MD можно перестроить только из уже сохранённых descriptions (`md_image_enrichment.py`); полноценное новое распознавание этим пользовательским путём удалено/отключено.
20. `opus_only.py` проверяет пригодность пар, при необходимости создаёт `_backup_before_opus_only_<timestamp>` и формирует unified job.
21. `unified_analysis_jobs.py` последовательно обрабатывает пары. Для каждой `unified_analysis.run_pair` выполняет preflight, а затем через `asyncio.to_thread` вызывает блокирующий `enriched_comparison.run_enriched_comparison`.
22. `enriched_comparison.py` читает обе enriched MD и существующие block links, строит system/user prompt и вызывает `ClaudeCodeProvider` (`text_llm_provider.py`) через subprocess `claude -p`. Это не прямой Anthropic API. Модель по умолчанию — `claude-opus-5`.
23. Модель возвращает JSON. Backend извлекает JSON из возможной Claude-обёртки, нормализует enum, confidence и evidence, сохраняет:
   - `enriched_comparison/comparison_result.json`;
   - `prompt.md` при недоступном provider/ошибочном non-done пути (на успешном `done` prompt отдельно не записывается);
   - усечённый `raw_response.txt`;
   - `job.json`.
24. После каждой успешной пары и в конце batch перестраивается session-level `unified_findings.json` (`unified_analysis_jobs.py` → `unified_findings.rebuild_unified_findings`).
25. Frontend получает прогресс polling-запросами, затем перезагружает статусы пар и список расхождений.

### 2.5. Просмотр, экспертная оценка и отчёт

26. Обычному пользователю вкладка «Расхождения» показывает не β entity-delta, а `v2_review.py`-проекцию основного Opus `comparison_result.json` текущей PDF-пары.
27. `unified_findings.py` пытается определить страницу каждого change по evidence/heading/approx-location и карте alignment. `v2_review.py` присваивает стабильный `v2_<sha1>` id, quality label и `impact_class`, по умолчанию скрывает административные/оформительские/косметические записи.
28. Пользователь видит место, источник, стоимость, заголовок/описание, «Было», «Стало», влияние и evidence. Кнопка перехода открывает пару на alignment slot и подсвечивает строки страницы, но не точный bbox самого изменения.
29. Кнопка «Экспертная оценка» показывает «Принять/Отклонить» и поле причины. Текущая видимая форма сохраняет решения через `POST /expert-review` (`app.js` → `scSubmitExpertReview`), ключ `<pair_id>::<raw_or_v2_id>`. Отдельное canonical API `v2_review_status.json` существует и поддерживает расширенные статусы, но его controls в текущей основной таблице удалены; при чтении `v2_review.py` использует V2 status первым, `expert_review.json` — fallback.
30. Вкладка «Отчёт» показывает только принятые изменения всех пар и экспортирует grouped XLSX через `/unified-diff-flat/export.xlsx?accepted_only=true&grouped=true`. Экспорт текущей пары также доступен из вкладки расхождений.

## 3. Что именно сравнивается

### 3.1. Допустимые входы

Технически можно выбрать любые две папки `stage_*` одного найденного object-root и любые два PDF внутри них. Backend не проверяет, что папки являются хронологическими версиями, а PDF — одним логическим документом. Это предположение пользователя, поддержанное только подбором имён и возможностью ручной коррекции.

Для полноценного основного результата у обеих сторон пары должны быть:

- PDF — для страниц, preview и crop;
- соседний MD — исходное текстовое представление;
- готовый enriched MD — фактический вход Opus;
- желательно result JSON — для блоков, bbox и просмотра существующих связей.

PDF и result JSON не передаются Opus напрямую. Графический смысл попадает в основной prompt только как текстовые описания внутри enriched MD и компактный `IMAGE_DIFF_INDEX`.

### 3.2. Основной алгоритм Opus

`backend/app/services/stage_comparison/enriched_comparison.py` → `SYSTEM_PROMPT`, `build_prompts`, `run_enriched_comparison`, `_normalize_change`:

1. Входом являются `<OLD_ENRICHED_MD>` и `<NEW_ENRICHED_MD>` целиком, если суммарный размер не превышает лимит.
2. Формат `replace_image_blocks_v1` сохраняет обычные text/table blocks, а image blocks заменяет сохранённым структурированным описанием: видимый текст, оборудование, материалы, числа, узлы/связи/последовательность, uncertainty.
3. В начале enriched MD `md_image_enrichment.py` строит `IMAGE_DIFF_INDEX`: буквальные labels, ratings и connections с page/block id, confidence и `usable_for_diff`.
4. Prompt требует сначала проверять графические различия по этому индексу. `usable_for_diff=false` нельзя использовать как единственное доказательство; такой источник требует weak confirmation и human review.
5. Block links, если сохранились у старой пары, объявлены focus anchors, а не exclusive scope.
6. Модель должна искать инженерно существенные изменения: решения, материалы, оборудование, расчёты, требования, состав, таблицы, схемы, последовательность, появление/исчезновение, поток/питание/сигнал, номера линий/групп, штамп, заявленные изменения, стоимость/сроки/риски.
7. Prompt запрещает считать изменением Markdown, стиль Qwen, OCR-шум, малые сдвиги, дубли и отличия без строительного смысла. Встроено правило prompt-injection: текст документов не является инструкцией.
8. Для ГРЩ есть отдельные правила покатегорийного сравнения `GRSH_CORE_SYSTEMS` и цепей «источник → аппарат → кабель → потребитель» (`grsh_core_systems.py`, дополнительные секции в `md_image_enrichment.py`).
9. Ответ должен быть только JSON со `summary` и массивом `changes`.
10. `_normalize_change` отбрасывает пустые записи, нормализует source/type/severity/cost, ограничивает confidence диапазоном `[0,1]`, принудительно ставит human review для `present_one_side` и disputed.

Сокращённая, но структурно точная схема одного `change`:

```json
{
  "id": "chg_<slug-or-id>",
  "source": "text|image_enrichment|scheme_analysis|table|stamp|mixed",
  "type": "added|removed|changed|present_one_side|material_changed|equipment_changed|calculation_changed|requirement_changed|design_logic_changed|scheme_sequence_changed|table_changed|stamp_changed|section_changed|unknown",
  "category": "architecture|structures|engineering_systems|electrical|hvac|water_supply|fire_safety|low_voltage|technology|general|other",
  "severity": "low|medium|high",
  "title": "...",
  "summary": "...",
  "old_value": "...",
  "new_value": "...",
  "construction_impact": "...",
  "cost_impact": "none|possible|likely|unknown",
  "requires_human_review": true,
  "disputed": false,
  "confidence": 0.0,
  "evidence_left": {
    "quote": "...",
    "section": "...",
    "approx_location": "стр. N / ..."
  },
  "evidence_right": {
    "quote": "...",
    "section": "...",
    "approx_location": "стр. N / ..."
  },
  "evidence": [
    {
      "origin": "text|table|stamp|image_enrichment|scheme_analysis|image_diff_index",
      "side": "left|right",
      "page": 24,
      "block_id": "optional",
      "quote": "..."
    }
  ]
}
```

Фактический сохранённый `comparison_result.json` оборачивает это в поля `version`, `status`, `provider`, `model`, `input_stats`, `summary`, `changes`, `warnings`, `selfcheck`, `duration_sec`, `error`, `actual_cost_usd`, `analysis_profile*`, `created_at`, `updated_at`.

### 3.3. Добавление, удаление, изменение и «нет изменений»

- Добавление/удаление в основном пути — вывод Opus по наличию сущности/fact только с одной стороны; это не чистая структурная операция над PDF.
- `present_one_side` означает неоднозначный one-sided факт и всегда требует ручной проверки.
- Изменение — смысловая разница старого/нового значения с выбранным subtype.
- Отсутствие изменений не создаёт отдельный объект: `status="done"` и пустой `changes` означает «модель не нашла расхождений».
- Добавленные/удалённые страницы представлены в `page_alignment.json` как slot с одной стороной `null`; основной Opus-result не обязан породить отдельный page-added/page-removed change.
- Legacy `findings.py` умеет page-added/page-removed/page-reordered, но это отдельная старшая ветка, не источник основной пользовательской V2-таблицы.

### 3.4. Уверенность и фильтрация ложных изменений

В основном пути действуют несколько уровней:

1. prompt-фильтр Opus;
2. `usable_for_diff` для графических descriptions;
3. enum/quality normalization;
4. опциональный self-check цитат (`STAGE_COMPARISON_SELFCHECK_ENABLED`, default false), который ищет evidence в MD дословно/fuzzy/по числам и может помечать либо удалять ungrounded changes;
5. `v2_review.py` → `classify_impact`, который детерминированно скрывает `admin_only`, `documentation_only`, `cosmetic_or_noise`;
6. ручное expert review.

Основной `confidence` сообщается самой моделью и не калиброван статистически. `location_confidence` — отдельная детерминированная оценка качества привязки страницы. При default-конфигурации self-check выключен, поэтому главным антигаллюцинационным барьером до эксперта остаются prompt и качество enriched MD.

### 3.5. Ограничение размера и fallback

Default limit — 600 000 символов на обе enriched MD. При превышении обычный результат получает `status=too_large`. Опциональная ветка `evidence_first_fallback.py` (`STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED`, default false) делит материал по scope/pages, строит детерминированный fact diff, запускает Opus чанками, проверяет citations и deduplicate. Для конкретной пары UI может принудительно запустить fallback. Это более grounded путь, но не default.

### 3.6. Сопоставление листов

Автоматическое и ИИ-сопоставление удалено перед повторной реализацией. Доступна только начальная позиционная карта и её ручное редактирование.

### 3.7. Сопоставление блоков

Создание ручных и IoU-связей удалено перед повторной реализацией. Старые `links.json` могут читаться для просмотра и использоваться Opus как ориентиры, но новые сессии получают пустой список связей. Поэтому текущий production-result не является настоящим block-to-block diff.

### 3.8. PDF, OCR, изображения и векторный слой

| Источник | Основной Opus-путь | Где реально используется |
|---|---|---|
| PDF | косвенно | рендер страниц/crop, page count, UI и visual utilities |
| Обычный MD | косвенно/через enriched MD | исходный текст и fallback location |
| Enriched MD | да, основной вход | полный prompt Opus |
| OCR | только уже сохранённый upstream результат | result/MD/enrichment; сравнение само OCR не запускает |
| Изображения | не отправляются Opus напрямую | UI, old graphic debug, visual-equivalence, Pipeline V2 |
| Графические блоки | текстовые descriptions + optional anchors | `IMAGE_DIFF_INDEX`, links, β pipeline |
| Векторный PDF-слой | нет в основном пути | отдельные platform grounding/vector services и часть upstream pipeline |
| Объекты внутри листа | не сопоставляются в основном пути | Pipeline V2 entities делает это отдельно |

## 4. Pipeline V2 β: что он делает и чего не делает

Pipeline V2 нельзя смешивать с `v2_review.py`: это разные подсистемы с похожим названием.

### 4.1. UI-run

`pipeline_v2_run_jobs.py` → `run_pipeline_v2_dry_run` запускает для одной пары подтверждённую offline-операцию. При rerun сначала создаётся `pipeline_v2_backup_before_ui_run_<TS>`. Из UI runner передаёт `llm_runner=None` и `vision_runner=None`, а manifest фиксирует, что модели не тронуты.

### 4.2. Фактические стадии

1. `pipeline_v2_prepared_ingest.py` → `build_normalized_document_model` требует result JSON обеих сторон и нормализует pages, blocks, bbox, OCR text, stamps, semantic types и quality flags. PDF/MD — дополнительные источники.
2. `pipeline_v2_block_matching.py` сопоставляет страницы по exact sheet name, stamp sheet number, fuzzy name, document code и слабому page-number fallback. Блоки сопоставляются one-to-one с учётом semantic type, текста и геометрии. Результат делится на strong/medium/weak/unmatched.
3. `pipeline_v2_graphic_block_descriptor.py` строит descriptors графических блоков; `pipeline_v2_visual_equivalence_gate.py` может детерминированно сравнивать рендеры через OpenCV.
4. `pipeline_v2_entity_extraction.py` извлекает stamp fields, requirements, normative references, equipment, cables, power, table rows/change history, scheme components и connection hints.
5. `pipeline_v2_entity_diff.py` матчит entities по exact key, identity и допустимому fuzzy и выдаёт `changed/added/removed/uncertain` с old/new, entity/block/page IDs, evidence, match score/method, confidence и quality flags. Также считает matched unchanged entities.
6. `pipeline_v2_grounded_evidence.py` связывает entity-delta с графическими evidence.
7. `pipeline_v2_delta_explanation.py` допускает LLM explanation, но в UI-run runner отсутствует, поэтому шаг `skipped_no_runner`.
8. Exclusion/link-validation/controlled-enforce/skip-readiness существуют как preview/observe/optional gates. UI-run не применяет controlled enforce к основному comparison result.

### 4.3. Результат β-ветки

Артефакты сохраняются в `pairs/<pair_id>/pipeline_v2/`. UI показывает summary, документы, блоки, entities, deltas, graphic grounding, link preview и mapping. Bbox overlays действительно подсвечивают сопоставленные/несопоставленные блоки. Manual entity mapping/exclusion overrides хранятся отдельно и являются mark-only.

Главное ограничение интеграции: Pipeline V2 **не записывает** `enriched_comparison/comparison_result.json`, не становится входом `v2_review.py` и не попадает автоматически в обычный экспертный отчёт.

## 5. Структура результата и единица изменения

### 5.1. Иерархия

Фактическая иерархия основного пути:

```text
session (две папки стадий)
└── PDF pair
    └── semantic change / finding
        ├── optional left/right page + alignment slot
        ├── text/graphic-derived evidence
        └── expert decision overlay
```

Единица найденного изменения — **семантическое замечание/change по PDF-паре**, сформированное LLM. Это не документ целиком, не строго лист, не bbox и не гарантированно один инженерный объект. Page/slot вычисляются после модели эвристически, bbox для основного change не хранится.

Pipeline V2 имеет более строгую альтернативную единицу — entity delta с entity/block/page IDs, но она остаётся в β-артефактах.

### 5.2. Проекция для UI

`unified_findings.py` добавляет к raw change:

```json
{
  "id": "chg_...",
  "pair_id": "pair_...",
  "pair_label": "old.pdf ↔ new.pdf",
  "left_pdf_name": "old.pdf",
  "right_pdf_name": "new.pdf",
  "sheet": "Лист ...",
  "page": 12,
  "left_page": 12,
  "right_page": 13,
  "alignment_slot": 14,
  "location_method": "quote|heading|approx_location|not_found",
  "location_confidence": 0.7,
  "source_layer": "mixed",
  "type": "equipment_changed",
  "category": "electrical",
  "change_direction": "complication|simplification|neutral|unknown",
  "cost_direction": "increase|decrease|unknown",
  "severity": "high",
  "title": "...",
  "summary": "...",
  "old_value": "...",
  "new_value": "...",
  "construction_impact": "...",
  "cost_impact": "likely",
  "requires_human_review": false,
  "confidence": 0.87,
  "evidence_left": {},
  "evidence_right": {},
  "status": "new"
}
```

`text_location.py` → `resolve_text_change_location` использует по приоритету:

1. evidence quote в исходном MD и ближайший маркер `## СТРАНИЦА N`;
2. ближайший heading;
3. page из `approx_location` модели;
4. `not_found`.

Затем page переводится в alignment slot. Уверенность этого resolution — отдельная эвристика (наиболее сильное точное попадание до 1.0, слабые fallback ниже).

### 5.3. Хранение и связь с документами

По умолчанию корень: `<ROOT_DIR>/comparison` (`paths.py` → `comparison_root_path`), с override `COMPARISON_ROOT`.

```text
comparison/
├── index.json
└── sessions/<session_id>/
    ├── session.json
    ├── unified_findings.json
    ├── expert_review.json
    ├── jobs/*.json
    └── pairs/<pair_id>/
        ├── pair.json
        ├── page_alignment.json
        ├── links.json
        ├── graphic_diffs.json
        ├── pages/{left,right}/*.png
        ├── crops/{left,right}/*.png
        ├── text_enrichment/
        │   ├── left_enriched.md
        │   ├── right_enriched.md
        │   └── *_image_descriptions.json
        ├── enriched_comparison/
        │   ├── comparison_result.json
        │   ├── prompt.md
        │   ├── raw_response.txt
        │   └── job.json
        ├── v2_review_status.json
        ├── v2_excluded_changes.json
        └── pipeline_v2/*.json
```

Старое `backend/app/data/stage_comparison_sessions/<id>.json` читается как legacy fallback, но новые сессии туда не пишутся (`store.py`, `paths.py`).

Связь с документами — абсолютные файловые пути в pair/session. Связи с project id/version id/БД нет. Старую сессию можно открыть через список сессий или автопоиск по двум путям. Из неё можно определить сравнивавшиеся папки и конкретные PDF; логические V1/V2 платформы определить надёжно нельзя, если это не следует из имени/пути.

### 5.4. История и повторный запуск

- Все session directories могут остаться на диске и доступны `GET /sessions`.
- Обычный UI автоматически ищет сессию для фиксированных `stage_1` / `stage_2`; кнопка ручного открытия канонической сессии удалена.
- `saved_config.py` хранит одну текущую saved/canonical конфигурацию и перезаписывает предыдущую; versioned history конфигураций нет.
- Forced Opus-run обычно перезаписывает `comparison_result.json`. Режим clear делает backup.
- `comparison_merge.py` реализует non-destructive merge старых/новых findings и сохранение id, но поиск импортов показывает, что production run его не вызывает; он покрыт отдельными unit tests. Поэтому UI-поля `is_new/change_origin` могут присутствовать только в результатах, которые были обработаны иным/старым внешним путём. Это подтверждённая неинтегрированная реализация.
- Clear-analysis создаёт `_backup_before_clear_analysis_<TS>` и может удалить comparison/enrichment/review artifacts по выбранным параметрам.
- Автоматического UI восстановления из этих backup folders нет. Их использование **требует дополнительной проверки/ручной операции**.

## 6. Версии документации

### 6.1. Что считается версией в этом разделе

Версия здесь — выбранная папка `stage_*`, а не объект `Version` платформы. `objects.py` возвращает label/path стадии; session хранит `stage_a_path`/`stage_b_path`. Хронология не валидируется.

Следствия:

- V1 → V2: можно, если соответствующие папки представлены как две `stage_*`;
- V2 → V3: можно аналогично;
- V1 → V3: можно аналогично;
- направление «старое → новое» задаёт сам порядок A/B;
- система не строит цепочку V1–V2–V3 и не переносит comparison history между такими запусками автоматически.

### 6.2. Как определяется «тот же документ»

Только похожестью filename с порогами и ручной коррекцией. Content hash, document code, stable document id, состав листов или platform version manifest для выбора PDF-пары не используются.

При переименовании:

- шумовое переименование (`V1`→`V2`, `ПД`→`РД`, `old`→`new`, punctuation) обычно переживается нормализацией;
- существенное новое имя может попасть в `maybe` или `unmatched`;
- пользователь может назначить правый PDF вручную.

### 6.3. Добавление/удаление листов и изменение PDF

- Разное количество страниц отражается в односторонних alignment slots.
- Перестановка требует stamp auto-match или ручной карты; позиционный initial alignment сам её не понимает.
- Изменение структуры PDF не мешает, если PDF открывается PyMuPDF и page map исправлена.
- Новый/удалённый лист не гарантирует отдельного semantic finding в основном Opus-result.
- Block links после изменения alignment могут стать stale/cross-page.

### 6.4. Отдельный платформенный механизм версий

`backend/app/services/common/version_service.py` уже поддерживает:

- legacy `project_versions.json`;
- container `version_group.json`;
- синтетический V1 для legacy;
- `v001/v002/...`, latest version, version directories;
- создание/удаление/загрузку файлов и разрешение активного output-dir.

API находится в `backend/app/api/routers/projects.py` → `list_project_versions`, `create_project_version`, `get_project(..., version_id=...)` и др.

Ни router, ни services stage-comparison не импортируют `version_service`; обратный поиск связей это подтверждает. Следовательно, история проекта сейчас **не используется** для выбора стадий, identity PDF или хранения сравнения.

## 7. Интерфейс

### 7.1. Вкладка «Загрузка документации»

- объект из общей верхней панели и две ZIP-загрузки `stage_1` / `stage_2`;
- автоматическое открытие/создание подходящей сессии;
- counts `matched/maybe/unmatched` и warnings;
- таблица PDF-пар с drag-order, inline rematch, block readiness, Opus duration/status, expert completeness;
- checkbox batch selection, «Обработать», «Открыть», Pipeline V2 run;
- обработка пар без enriched MD с явным skip;
- `too_large` badge позволяет per-pair fallback.

### 7.2. «Связь блоков»

- страницы A/B рядом;
- синхронная навигация по alignment slots и zoom;
- overlay блоков по bbox и просмотр существующих связей;
- ручное редактирование page alignment;
- enriched MD side-by-side;
- кнопки связывания, auto-match, Pipeline V2 link/entity preview и pair-template отсутствуют.

### 7.3. «Расхождения»

Обычный режим — текущая PDF-пара и `v2_review.py`. Колонки:

- источник и направление стоимости;
- место: sheet/page/pair;
- изменение: title, summary, type/category/evidence;
- «Было»;
- «Стало»;
- влияние;
- при включённой оценке — решение и причина;
- переход к месту.

По умолчанию скрыты формальные изменения; есть кнопка «Показать формальные». Полноценной панели фильтров/поиска в текущем основном виде нет — комментарий frontend прямо говорит, что summary cards, filters и bulk actions удалены. Dev-mode открывает классический unified, text-only и graphic debug views.

Переход к месту открывает обе версии рядом и подсвечивает alignment slot. Точная coordinate region конкретного Opus-change не подсвечивается, потому что change не хранит bbox.

Можно подтвердить/отклонить и оставить текстовую причину. Назначение ответственного отсутствует. Поля assignee/responsible и соответствующих действий в stage-comparison UI/API не найдены.

### 7.4. «Отчёт»

- read-only accordion по PDF-парам;
- только accepted findings;
- счётчики по парам;
- grouped XLSX всех принятых изменений;
- отдельный сервис `reports.py` умеет markdown/html/json/pdf/docx и manifest, но router не содержит `/sessions/{id}/reports/*` endpoints, несмотря на URL в самом сервисе. Значит, этот генератор сейчас не экспонирован текущим router и не является фактическим UI-путём.

### 7.5. «Pipeline V2 β»

Панель читает готовый composite payload и показывает health/summary/artifacts. Заголовок всё ещё говорит read-only, но в upload-таблице есть отдельная подтверждаемая кнопка запуска offline dry-run; modal честно предупреждает о записи артефактов. Это UX-несогласованность текста, а не предположение.

## 8. Конфигурация и модели

`.env.example` и loaders задают основные параметры:

| Переменная | Default | Роль |
|---|---:|---|
| `COMPARISON_ROOT` | `<ROOT_DIR>/comparison` | runtime-хранилище |
| `AUDIT_STAGE_COMPARISON_ROOTS` | platform-specific | roots объектов/stage folders |
| `STAGE_COMPARISON_TEXT_LLM_ENABLED` | `false` | legacy text-only debug |
| `STAGE_COMPARISON_TEXT_LLM_PROVIDER` | `claude_code` | provider debug-ветки |
| `STAGE_COMPARISON_TEXT_LLM_MODEL` | `claude-sonnet-5` | модель debug-ветки |
| `STAGE_COMPARISON_TEXT_LLM_MAX_CHARS` | `350000` | размер debug text input |
| `STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED` | `false` | основной Opus gate |
| `STAGE_COMPARISON_ENRICHED_COMPARE_PROVIDER` | `claude_code` | основной provider |
| `STAGE_COMPARISON_ENRICHED_COMPARE_MODEL` | `claude-opus-5` | основная модель |
| `STAGE_COMPARISON_ENRICHED_COMPARE_TIMEOUT_SEC` | `900` | timeout |
| `STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS` | `600000` | input limit |
| `STAGE_COMPARISON_SELFCHECK_ENABLED` | `false` | post-grounding evidence |
| `STAGE_COMPARISON_LLM_CONCURRENCY` | `1` | legacy graphic jobs |

Default false означает default кода/example, а не обязательно production. **Требует дополнительной проверки:** значения env активного deployment, точная установленная версия Claude CLI и доступность алиасов `claude-opus-5`/`claude-sonnet-5`/Haiku.

Legacy graphic debug (`jobs.py`) отправляет две PNG-картинки через общий `backend/app/services/llm/llm_runner.py` в OpenRouter, требует `run_paid=true` и `confirm_paid=true`, проходит `paid_api_guard`; request может переопределить модель, а default batch-модуля — `google/gemini-3.1-pro-preview`. Это не основной Opus-путь.

## 9. Связи с остальной платформой и кандидаты на переиспользование

| Существующий механизм | Что делает сейчас | Можно ли использовать в новом сравнении |
|---|---|---|
| `common/version_service.py` | stable project versions, manifests, version dirs, history order | Да. Это готовая основа identity V1/V2/V3; сейчас не подключена. |
| `projects.py` version API | list/create/delete versions, files, version context | Да, для выбора версий вместо произвольных stage paths. |
| `stage_comparison/scanner.py` | находит PDF/MD/result и делает filename pairing | Частично. Сканирование полезно, identity по filename недостаточно. |
| `alignment.py` | явная двусторонняя page map с null slots | Да. Простая, редактируемая и проверяемая модель. |
| `blocks.py`/`block_pdf_source.py` | bbox normalization, crops, существующие link anchors, источники block PDF | Да, базовый слой координат/preview. |
| upstream PDF pipeline/result JSON | распознанные pages/blocks, OCR, Markdown | Да. Stage comparison уже потребляет эти артефакты. |
| `pipeline/stages/crop_blocks/`, `block_markdown.py` | нарезка/описание блоков | Да. Следует устранить дубли parsing/normalization, но в этом аудите код не менялся. |
| `pipeline/stages/block_grounding/block_source_router.py` → `vector_text_block_index`, `vector_path_graph.py`, `services/findings/grounding_service.py` | буквальный/vector grounding, координатные индексы и граф путей | Да, сильная основа точного evidence и bbox. Основной compare их не использует. |
| Pipeline V2 prepared ingest | единая normalized document model | Да, вероятный каркас новой детерминированной обработки. |
| Pipeline V2 block matching | semantic/type/text/geometry page+block matches | Да; существенно сильнее legacy IoU-only links. |
| Pipeline V2 entity extraction/diff | инженерные entities и grounded old/new delta | Да; уже решает object-level единицу изменения в β. |
| Pipeline V2 visual equivalence/grounding | deterministic image gate и evidence | Да, для подавления визуального шума и подтверждения change. |
| Pipeline V2 mapping/exclusion overrides | mark-only ручные решения с history | Да, модель неразрушающих overrides удачна. |
| `expert_review.py` / `v2_review.py` | stable per-pair review overlay, accept/reject/comment | Да, после унификации двух хранилищ/ID. |
| `unified_grouping.py` | deterministic theme/discipline/direction/cost grouping | Да, как presentation/aggregation после детекта. |
| `migrated_findings_service.py` | перенос accepted findings, duplicate/still relevant/possibly resolved | Да, готовый механизм «было ли замечание раньше / исправлено ли». Сейчас отдельный. |
| `decision_carryover_service.py` | перенос вердикта между версиями: shortlist + Sonnet + threshold/manual | Да, для review continuity. Сейчас отдельный. |
| debt-control services | незакрытые/перенесённые замечания между проверенными версиями | Да, для межверсионной истории долгов. |
| platform discipline registry/service | справочник и определение дисциплин | Да, вместо свободного category только от LLM. |
| `findings/rejected_audit_service.py` | уже переиспользует `stage_comparison.block_pdf_source` | Косвенное подтверждение, что block-source utility достаточно универсален. |

## 10. Подтверждённые ограничения

### 10.1. Архитектурные

1. Frontend — крупный monolith `index.html` + `app.js`, а не изолированные компоненты раздела. Состояние legacy, current V2-review и Pipeline V2 сосуществует в одном namespace `sc*`.
2. Основной результат, reviews, jobs и конфигурация распределены по JSON-файлам без транзакции между ними. Атомарная запись применяется к отдельным файлам, но не ко всему session-state.
3. Background tasks живут в процессе API; restart обрывает работу.
4. Одновременно существуют три понятия «V2»: platform project versions, V2 review overlay и Pipeline V2 β. Это подтверждённая терминологическая коллизия.
5. `comparison_merge.py` и `reports.py` реализованы и протестированы, но не подключены к фактическому основному run/router соответственно.
6. Есть дублирующие пути: difflib, Sonnet text-only, graphic LLM, unified Opus, V2 review и Pipeline V2. Обычный UI скрывает часть, но backend/API сохраняет их.

### 10.2. Алгоритмические

1. Identity PDF основана на filename similarity, а не стабильном document ID/content/stamp ensemble.
2. Основной детектор — один LLM-проход по больших enriched MD; нет гарантированного entity-level matching.
3. Основные additions/removals не вычисляются структурно; это интерпретация Opus.
4. Block links — anchors, не границы change. Bbox change отсутствует.
5. Default self-check выключен, confidence — self-reported моделью.
6. «Нет изменений» означает пустой LLM-массив и не доказывает детерминированное равенство.
7. Page location после модели строится эвристически по цитате/heading/approx-location и может остаться `not_found`.
8. Автоматический matcher листов/блоков отсутствует до повторной реализации.

### 10.3. Производительность и устойчивость

1. Opus получает оба enriched MD целиком до 600k символов; сверх лимита нужен отдельно включённый fallback.
2. Batch пар выполняется последовательно внутри in-process job.
3. UI polling создаёт регулярные GET-запросы; push-progress нет.
4. Рендер страниц/crops кешируется — это положительная часть, но смена upstream PDF при том же пути не имеет явного content-version key во всех артефактах.
5. Текущий UI не создаёт новое image enrichment, поэтому подготовка входов зависит от внешнего/предыдущего pipeline.

### 10.4. UX

1. Раздел называется «Сравнение стадий», а не «Сравнение документации»; термины V2 неоднозначны.
2. Переход ведёт к странице/slot, но не к точной области изменения.
3. В основном списке нет ответственного и нет полноценной истории rerun.
4. Фильтры основной V2-таблицы удалены; при большом числе changes остаётся линейный список.
5. Pipeline V2 tab подписан read-only, хотя запуск dry-run доступен в соседней upload-таблице.
6. Обычная форма review пишет `expert_review.json`, при том что backend называет `v2_review_status.json` canonical. Два слоя повышают сложность объяснения статуса.

### 10.5. Работа с версиями

1. Нет связи с platform project/version IDs.
2. Нет цепочки V1→V2→V3 и сравнения с общим logical document identity.
3. Каноническая конфигурация одна и перезаписывается.
4. История сессий физически есть, но не оформлена как версия сравнения с lineage.
5. Переименование файла поддерживается лишь эвристически.

### 10.6. Работа с графикой

1. Основной Opus видит текстовые descriptions, а не pixels/vector geometry.
2. Качество зависит от сохранённого enrichment; новые descriptions текущий UI не создаёт.
3. Low-confidence graphics ограничиваются prompt-правилом, а не жёстким доказательным validator по умолчанию.
4. Сильные visual/entity механизмы Pipeline V2 не интегрированы в основной result/review/report.

### 10.7. Работа с ИИ

1. Основной compare зависит от локальной доступности Claude CLI и model alias.
2. Default `enabled=false`; runtime activation внешняя.
3. Prompt содержит хорошую инженерную детализацию, но model output остаётся недетерминированным.
4. Force rerun перезаписывает result; неинтегрированный merge не гарантирует сохранение IDs/вердиктов.
5. Legacy graphic path использует отдельный OpenRouter/paid guard, то есть в разделе два разных механизма provider/billing.

### 10.8. Хранение истории

1. Нет БД-истории, immutable run entity или run lineage.
2. Backup folders существуют, но не индексируются как пользовательская история.
3. Raw excerpt помогает аудиту, но сохраняется усечённо; `prompt.md` пишется только на отдельных error/unavailable путях, а не на каждом успешном run.
4. Review overlays могут стать orphan при изменении raw IDs; есть pruning/transfer helpers, но это дополнительная reconciliation-логика.

## 11. Карта API

Основные группы `backend/app/api/routers/stage_comparison.py`:

| Группа | Endpoints/handlers | Назначение |
|---|---|---|
| Объекты/загрузка | `/objects`, `/objects/{object_id}/stages/{stage_name}/upload`; legacy `/saved-config`, `/canonical-config` | привязка глобального объекта и ZIP-замена `stage_1` / `stage_2` |
| Сессии/пары | `/sessions`, `/unmatched`, pair create/update/delete/order | scan и ручная коррекция PDF identity |
| Страницы/блоки | `page-image`, `block-image`, `auto-link`, `links`, templates | side-by-side UI и anchors |
| Alignment | `page-alignment`, suggest/stamp/auto-match/insert/move/delete | сопоставление листов |
| Main analysis | `unified-analysis`, `unified-analysis-jobs`, `opus-only`, `clear-analysis` | preflight/run/status/cancel/backup |
| Results | `unified-diff-flat`, grouped, XLSX export | projection и export |
| Review | `expert-review`, `/pairs/{pid}/v2/*`, transfer/prune | overlay решений |
| Legacy/debug | `text-diff`, `text-llm-*`, `graphic-summary`, `graphic-diff*` | отдельные старые методы |
| Pipeline V2 | `/pipeline-v2/{sid}/ui-payload`, pair run/status, previews, overrides | β artifacts и offline run |

## 12. Карта основных файлов

| Файл | Основные классы/функции | Роль в сравнении |
|---|---|---|
| `frontend/index.html` | stage-comparison template | все пять вкладок, таблицы, modal, overlays |
| `frontend/static/js/app.js` | `scUploadStageArchive`, `scTryAutoLoadSession`, `scOpenPair`, `scQOStartConfirmed`, `scLoadV2Changes`, pollers | frontend state и REST orchestration |
| `frontend/static/css/styles.css` | `.sc-*` | layout/overlays/status/review styling |
| `backend/app/main.py` | `include_router` | регистрация API |
| `backend/app/api/routers/stage_comparison.py` | FastAPI handlers | весь публичный REST-контур |
| `stage_comparison/paths.py` | `comparison_root_path`, path builders | файловая схема runtime |
| `stage_comparison/objects.py` | `list_objects` | allowlisted objects и `stage_*` |
| `stage_comparison/stage_upload.py` | `replace_stage_from_zip` | валидация ZIP, backup и замена `stage_1` / `stage_2` |
| `stage_comparison/scanner.py` | `scan_stage_folder`, `match_pdfs` | поиск артефактов и filename pairing |
| `stage_comparison/store.py` | `create_session`, `get_session`, pair/link/alignment/render helpers | основное файловое состояние |
| `stage_comparison/saved_config.py` | load/save canonical config | один saved/canonical context |
| `stage_comparison/alignment.py` | initial/validate/insert/move/delete | двусторонняя карта страниц |
| `stage_comparison/blocks.py` | `normalize_blocks_from_result_json` | bbox normalization для просмотра |
| `stage_comparison/block_pdf_source.py` | `resolve_block_pdf_source/package` | надёжный источник crop/text блока |
| `stage_comparison/md_image_enrichment.py` | format detection/rebuild, `IMAGE_DIFF_INDEX` | сохранённые graphic descriptions в MD |
| `stage_comparison/analysis_profile.py` | profile flags/classification | default/rich_grsh metadata и downgrade guard |
| `stage_comparison/graphic_profiles.py` | profile config | graphic extraction profiles |
| `stage_comparison/grsh_core_systems.py` | GRSH extraction/render helpers | специальная модель ядра ГРЩ |
| `stage_comparison/unified_analysis.py` | `preflight_pair`, `run_pair` | orchestration ready-enriched-MD → Opus |
| `stage_comparison/unified_analysis_jobs.py` | create/run/start/get/cancel | persistent in-process batch jobs |
| `stage_comparison/opus_only.py` | prepare/backup eligible pairs | текущий основной UI-run |
| `stage_comparison/enriched_comparison.py` | `SYSTEM_PROMPT`, `build_prompts`, `run_enriched_comparison` | главный семантический compare |
| `stage_comparison/text_llm_provider.py` | `ClaudeCodeProvider` | subprocess `claude -p` |
| `stage_comparison/evidence_first_fallback.py` | fallback config/run | chunked grounded path для too-large |
| `stage_comparison/text_norm.py` | grounding normalization | единая text normalization |
| `stage_comparison/text_location.py` | `resolve_text_change_location` | evidence → page/alignment slot |
| `stage_comparison/unified_findings.py` | `build_unified_flat`, `rebuild_unified_findings` | session/pair projection результата |
| `stage_comparison/unified_grouping.py` | group/classify/direction helpers | deterministic aggregation/directions |
| `stage_comparison/v2_review.py` | `make_v2_id`, `classify_impact`, `build_pair_v2_changes` | основной pair-scoped пользовательский список |
| `stage_comparison/expert_review.py` | load/apply/summary/prune | видимые accept/reject/reason решения |
| `stage_comparison/review_transfer.py` | exact/Claude transfer | reconciliation старых/новых review IDs |
| `stage_comparison/clear_analysis.py` | backup/remove selected artifacts | безопасная очистка анализа |
| `stage_comparison/comparison_merge.py` | `merge_changes`, `apply_merge` | реализованный, но не подключённый rerun merge |
| `stage_comparison/diff_text.py` | difflib line diff | legacy debug |
| `stage_comparison/text_llm_input.py` | strip image blocks | вход legacy text-only |
| `stage_comparison/text_llm.py` | Sonnet prompt/run | legacy semantic text debug |
| `stage_comparison/text_llm_jobs.py`, `text_llm_preflight.py`, `text_llm_flat.py` | job/preflight/projection | batch debug-контур |
| `stage_comparison/jobs.py` | graphic LLM jobs | legacy paid image compare |
| `stage_comparison/findings.py` | rebuild legacy findings | old text/graphic/page finding merge |
| `stage_comparison/visual_block_equivalence.py` | deterministic image comparison | false-diff precheck/diagnostics |
| `stage_comparison/visual_block_equivalence_jobs.py` | visual jobs | batch execution |
| `stage_comparison/block_equivalence_precheck.py`, `text_block_equivalence.py` | equivalence gates | suppression candidates до/вокруг enrichment |
| `stage_comparison/block_exclusion_preview.py` | preview exclusions | mark-only preview |
| `stage_comparison/reports.py` | generate report formats | implemented service, router path absent |
| `stage_comparison/production_root_health.py`, `pipeline_v2_runtime_root_audit.py` | health/audit | read-only runtime diagnostics |
| `stage_comparison/pipeline_v2_prepared_ingest.py` | normalized document | β canonical input model |
| `stage_comparison/pipeline_v2_block_matching.py` | page/block matching | β precision-first matching |
| `stage_comparison/pipeline_v2_graphic_block_descriptor.py` | graphic descriptors | β graphic structure |
| `stage_comparison/pipeline_v2_visual_equivalence_gate.py` | deterministic pixel gate | β visual equivalence |
| `stage_comparison/pipeline_v2_entity_extraction.py` | extract entities | β object model |
| `stage_comparison/pipeline_v2_entity_diff.py` | match/diff entities | β changed/added/removed/uncertain |
| `stage_comparison/pipeline_v2_grounded_evidence.py`, `pipeline_v2_grounding_detail.py` | grounded evidence/detail | β traceability |
| `stage_comparison/pipeline_v2_delta_explanation.py` | injectable LLM explanation | skipped in normal offline UI-run |
| `stage_comparison/pipeline_v2_graphic_vision_enrichment.py`, `pipeline_v2_graphic_vision_grounding.py` | optional vision + validation | no runner in offline UI-run |
| `stage_comparison/pipeline_v2_block_link_preview.py` | preview payload | bbox side-by-side links |
| `stage_comparison/pipeline_v2_entity_alignment_preview.py`, `pipeline_v2_entity_alignment_detail.py` | mapping preview/detail | same/rename/reorg/mismatch diagnostics |
| `stage_comparison/pipeline_v2_link_validation.py`, `pipeline_v2_link_validation_detail.py` | validate proposed links | β mark-only validation |
| `stage_comparison/pipeline_v2_entity_mapping_overrides.py` | manual override history | separate non-destructive decisions |
| `stage_comparison/pipeline_v2_exclusion_preview.py`, `pipeline_v2_exclusion_review_overrides.py` | exclusions/overrides | β engineering relevance review |
| `stage_comparison/pipeline_v2_skip_readiness.py` | readiness | optional skip gate |
| `stage_comparison/pipeline_v2_controlled_enforce*.py` | config/preflight/dry-run/executor/state/observe | optional guarded enforcement, not main UI result |
| `stage_comparison/pipeline_v2_dry_run.py` | `run_pipeline_v2_dry_run` | full β orchestration |
| `stage_comparison/pipeline_v2_run_jobs.py` | UI job + backup | actual offline UI launch |
| `stage_comparison/pipeline_v2_ui_payload.py`, `pipeline_v2_payload_service.py` | composite response | β panel data |
| `backend/app/services/common/version_service.py` | version manifests/context | existing platform versions, not integrated |
| `backend/app/api/routers/projects.py` | version endpoints | existing project/version UI/API |
| `backend/app/services/findings/migrated_findings_service.py` | cross-version finding recheck | reusable finding history, not integrated |
| `backend/app/services/findings/decision_carryover_service.py` | verdict carryover | reusable review history, not integrated |
| `.env.example` | stage comparison env block | documented default gates/models/limits |

### 12.1. Тестовое покрытие

В `tests/` находится более 70 `test_stage_comparison_*.py`, а во frontend — `pipeline_v2_panel.test.js`, `pipeline_v2_run_button.test.js`, `pipeline_v2_block_link_preview.test.js`. Покрыты, среди прочего:

- pairing/alignment/stamp/multipart/LLM match;
- block/text/visual equivalence;
- Opus-only, clear-analysis, self-check, too-large fallback;
- expert/V2 review, stable decisions, grouping/export;
- почти каждая стадия Pipeline V2 и её endpoints;
- UI payload/run/preview.

Это подтверждает большое число локально проверяемых механизмов, но наличие unit tests у `comparison_merge.py` не означает production-интеграцию: импортов этого модуля из run-path нет.

## 13. Пять коротких ответов

1. **Как сейчас работает «Сравнение документации»?** Выбирает две `stage_*` папки, сопоставляет PDF по именам, использует подготовленные enriched MD, запускает Claude Opus, сохраняет semantic changes в JSON и показывает их с impact-фильтром и экспертной оценкой.
2. **Что именно оно реально умеет сравнивать?** Текст, таблицы и текстовые описания графики двух PDF-пар; страницы/блоки помогают навигации и prompt anchors. Прямого production pixel/vector/entity comparison в основном результате нет.
3. **Какая самая слабая часть текущей реализации?** Identity версий/документов и единица изменения: нет связи с platform versions/stable document IDs, а change остаётся недетерминированным LLM-finding без обязательного object/bbox grounding.
4. **Какие сильные механизмы уже есть и их не нужно писать заново?** Project version service, page/stamp alignment, bbox/crop utilities, expert overlays, history/carryover findings, а особенно Pipeline V2 normalized model, semantic block matching, entity extraction/diff и visual grounding.
5. **Какие части архитектуры лучше сохранить при полной переработке?** Неизменяемые prepared artifacts, явную page map, отдельные review overrides, атомарные/трассируемые артефакты и prompt/evidence audit trail; Pipeline V2 стоит рассматривать как более строгий вычислительный фундамент, не смешивая его с presentation/review слоями.

## 14. Что требует дополнительной проверки

- фактические env-флаги и model aliases production deployment;
- кто и каким внешним процессом сейчас создаёт новые enriched MD/image descriptions;
- есть ли эксплуатационный скрипт вне просканированного router, который вызывает `comparison_merge.apply_merge` или `reports.py`;
- политика очистки старых session/backup directories на production;
- насколько полны реальные result JSON/OCR/vector artifacts разных дисциплин;
- фактическая стоимость/latency/ошибки Claude runs по production logs — статический код этого не доказывает.
