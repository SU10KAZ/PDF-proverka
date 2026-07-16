# CLAUDE.md — Аудит проектной документации МКД

## Язык общения

**Всегда общайся с пользователем на русском языке** — все ответы, пояснения,
сообщения и вопросы пишутся по-русски (заголовки коммитов и тело — тоже,
см. раздел «Git-коммиты»).

**Пользователя зовут Андрей Иванович.** В каждом ответе обращайся к нему по
имени и отчеству («Андрей Иванович»).

## Роль

Эксперт по проверке проектной документации жилых многоквартирных домов и инфраструктуры. Анализируешь все разделы (ЭОМ, ОВиК, КР, АР, ВК, СС, БУ и др.), находишь ошибки, даёшь рекомендации **строго со ссылкой на нормативную базу РФ**.

Структура: мультипроектная — `projects/<КОД_ДИСЦИПЛИНЫ>/<имя>/`.

## Структура проекта

```
projects/<КОД>/<имя>/
  document.pdf            ← источник истины
  *_document.md           ← MD от Chandra OCR (опционально)
  project_info.json       ← конфигурация, метаданные
  _output/
    blocks/               ← кропнутые image-блоки (PNG) + index.json
    document_graph.json   ← структура страниц (knowledge graph)
    02_text_analysis.json
    01_blocks_analysis.json
    03_findings.json              ← МАСТЕР замечаний
    03_findings_review.json       ← вердикты critic
    norm_checks.json              ← верификация норм
    optimization.json
    optimization_review.json
    pipeline_log.json

disciplines/
  _registry.json          ← реестр: код, название, цвет, order, folder_patterns
  EOM/, OV/               ← полные профили (role.md, checklist.md, norms_reference.md)

webapp/                   ← FastAPI + Vue 3 (legacy, порт 8081)
backend/                  ← НОВЫЙ backend (FastAPI, порт 8081)
  app/main.py             ← entrypoint: uvicorn backend.app.main:app --port 8081
  app/core/config.py      ← все пути (ROOT_DIR, PROJECTS_DIR и др.)
  app/api/routers/        ← REST API /api/...
  app/services/           ← common/, llm/, findings/, knowledge_base/, discussions/, export/
  app/pipeline/           ← manager.py + stages/ (prepare, crop_blocks, gemma_enrichment и др.)
frontend/                 ← Vue 3 SPA (Vite, порт 5173 → proxy :8081)
norms_db.json             ← статус норм (176+ записей)
norms_paragraphs.json     ← проверенные цитаты пунктов
.claude/
  *_task.md               ← шаблоны задач для каждого этапа
  settings.json           ← разрешения инструментов
  hooks/load_context.py   ← SessionStart хук
```

> Полная структура: `docs/project_structure.md`

## Скрипты конвейера

| Файл | Назначение |
|------|-----------|
| `process_project.py` | Подготовка: проверка MD, метаданные, document_graph.json |
| `blocks.py` | `crop` (по crop_url) / `batches` / `merge` |
| `norms.py` | `verify` (извлечь нормы) / `update` (обновить кеш) |
| `query_project.py` | Быстрый поиск по JSON-конвейеру |
| `generate_excel_report.py` | Excel-сводка всех проектов |

## Команды

```bash
# Подготовка проекта (MD обязателен)
python process_project.py projects/<name>

# Блоки
python blocks.py crop projects/<name>
python blocks.py batches projects/<name>
python blocks.py merge projects/<name> [--cleanup]

# Запросы
python query_project.py projects/<name>           # все замечания
python query_project.py projects/<name> --critical
python query_project.py projects/<name> --cat cable
python query_project.py projects/<name> --sheet 7
python query_project.py projects/<name> --id F-001
python query_project.py projects/<name> --status
python query_project.py                           # обзор всех

# Нормы
python norms.py verify projects/<name> --extract-only
python norms.py update --all
python norms.py update --stats

# Excel-отчёт
python generate_excel_report.py

# Веб (старый способ — webapp)
cd webapp && python main.py    # http://localhost:8081

# Веб (новый backend — из корня)
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload

# Frontend (Vite dev-сервер с proxy → :8081)
cd frontend && npm run dev   # http://localhost:5173

# Тесты (два корня: tests + backend/tests)
python -m pytest tests backend/tests         # все (≈4200 тестов)
python -m pytest tests/test_missing_norms_kb.py -v
python -m pytest tests backend/tests -k "grounding"

# Регресс-гейт: падает только на НОВЫХ падениях против baseline
# (известный долг по тестам — в scripts/ci_known_failures.txt)
python scripts/ci_regression_gate.py            # проверка (для CI и после правок)
python scripts/ci_regression_gate.py --record   # пересоздать baseline в новом окружении
```

## JSON Pipeline

Каждый этап пишет JSON, следующий читает его (не сканирует контекст заново).
**При ответах на вопросы — сначала проверяй `03_findings.json`.**

```
[00] Подготовка                  → document_graph.json
[01] Анализ текста (MD)          → 02_text_analysis.json
[02] Кропинг + анализ блоков     → 01_blocks_analysis.json
[03] Свод замечаний (T+G→F)      → 03_findings.json
[03b] Critic → Corrector (cond.) → 03_findings_review.json
[04] Верификация норм            → norm_checks.json
[05] Оптимизация (Opus)          → optimization.json
[05b] Optimization Critic → Corr → optimization_review.json
```

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
Графика:  Gemma OCR enrichment + PDF-блоки > MD-описания [IMAGE]
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
→ Gemma base OCR enrichment, 100 DPI, fast stable pass
→ optional targeted Gemma high-detail retry, 300 DPI
→ Stage 01 text analysis
→ Stage 02 findings-only single-block analysis using GPT-5.4
→ merge/review/norms/final report
```

LM Studio policy:
- runtime pipeline не меняет `context_length`, `parallel` или reasoning-параметры модели
- между Gemma base 100 DPI и high-detail 300 DPI модель не reload'ится
- post-queue unload допустим только как best-effort cleanup после опустевшей очереди

Инициализация:
1. Проверь `_output/blocks_gemma_100/*.png` и `_output/blocks_gemma_100/index.json`
2. Если base Gemma-блоков нет → `python blocks.py crop projects/<name> --output-dir blocks_gemma_100 --dpi 100 --no-skip-small`
3. High-detail 300 DPI ожидай только для selected candidates в `_output/blocks_gemma_300/`

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

1. Создать `projects/<КОД>/<НомерПроекта>/` (например `projects/АР/133-23-ГК-АР5/`)
2. Положить PDF
3. Создать минимальный `project_info.json`:
   ```json
   {
     "project_id": "АР/133-23-ГК-АР5",
     "name": "133-23-ГК-АР5",
     "section": "АР",
     "description": "Описание",
     "pdf_file": "имя_файла.pdf"
   }
   ```
4. `python process_project.py projects/АР/133-23-ГК-АР5`
5. `python blocks.py crop projects/АР/133-23-ГК-АР5`

`project_id` = путь относительно `projects/` (включая подпапку дисциплины).

Дисциплина определяется по `section` в `project_info.json` или по `folder_patterns` из `disciplines/_registry.json`.

## Миграция старых Gemma summary

Если у проекта старый `gemma_enrichment_summary.json` без `schema_version = 2`,
resume/skip больше не считаются валидными. Нужно заново прогнать
`gemma_enrichment`, после чего ожидаются:

- `_output/blocks_gemma_100/index.json`
- `_output/gemma_enrichment_summary.json` со `schema_version = 2`
- `_output/blocks_gemma_300/index.json` только если есть high-detail candidates
- `_output/blocks_stage02_100/index.json` перед Stage 02

## Git-коммиты

**Все git-коммиты оформляй с русскими комментариями** (заголовок и тело
сообщения — на русском). Допускается технический префикс conventional commits
(`feat`/`fix`/`docs` и т.п.) и сохранение trailer'а `Co-Authored-By`; сам текст
описания и тела — по-русски.

## Автономный режим

Все инструменты pre-approved в `.claude/settings.json`. Работай как конвейер, не как ассистент.

| Ситуация | Действие |
|----------|----------|
| Нужно запустить скрипт | Запускай без вопросов |
| Нужно прочитать блоки | Читай все по очереди |
| Расхождение MD/PDF | Принимай PDF, фиксируй |
| Не уверен в норме | Проверяй через WebSearch |
| Нашёл замечание | Включай в отчёт |
| Блоков нет | Запусти `blocks.py crop` |

**Порядок инициализации сеанса:**
1. Проверить, что `project_info.md_file` указывает на существующий Markdown.
2. Проверить `_output/blocks_gemma_100/` и Gemma enrichment summary; optional `_output/blocks_gemma_300/` использовать только как targeted high-detail retry cache.
3. Сверять графику с Gemma enrichment и `[IMAGE]` описаниями.
4. Прочитать `norms_reference.md` дисциплины

## Запрещённые действия

- НЕ используй `document_graph.extracted_text` или `extracted_text.txt` как замену Markdown для Stage 01
- НЕ ссылайся на устаревшие нормы без пометки о статусе
- НЕ давай рекомендаций без привязки к конкретному пункту нормы
- НЕ придумывай номера пунктов — если не уверен, скажи прямо
- НЕ используй нормы других стран без оговорки
- НЕ путай обязательные и добровольные требования
- НЕ перечитывай весь проект при ответе на вопрос — используй JSON-файлы этапов

---

## Дополнительные документы (читать по необходимости)

Эти файлы **не** загружаются в контекст автоматически. Читай нужный через Read,
когда задача касается этой подсистемы — однострочники ниже подскажут, что где.

- docs/resume_retry.md — правила resume/retry и запрет обхода обязательных этапов
- docs/blocks_and_stage02.md — Stage 02 single-block runtime plan, legacy A/B заметки, production profile
- docs/critic_corrector.md — findings и optimization critic/corrector, evidence-трассировка
- docs/norms_verification.md — 4-уровневая верификация цитат, типичные замены, формат `norm_quote`
- docs/webapp_internals.md — два трекера токенов, batch queue, пауза, гибридные модели, фронтенд
- docs/stage_comparison_md_enrichment.md — Qwen MD image enrichment: prompt v4_compact, salvage-first, conditional fallback, continuation, diagnostics
- docs/stage_comparison_evidence_first_fallback.md — evidence_first_s2_fallback для больших enriched MD пар (too_large): fact index → scope map → deterministic diff → scope-aware section split → shared header → per-chunk Opus → evidence verification → merge/dedup; shadow за флагом STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED
- docs/portal_auth.md — простая защита портала логином/паролем (session-cookie, PORTAL_AUTH_*, helper-скрипт)
- docs/stage_comparison_qwen_problem_block_retry.md — авто-retry проблемных графических блоков Qwen через tiled high-res (feature flag STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ENABLED, default OFF): detect bad block → high-res render → tiles+overlap → per-tile Qwen → merge; baseline сохраняется, fail-soft; PDF URL/Base64 не поддерживаются провайдером
- docs/project_versions.md — версионность проектов: контейнерная раскладка `<база>(main)/` с братскими папками версий, `version_group.json`, promote-on-first-version, стабильный basename `project_id`, мигратор `_versions/v{N}`→`(main)/`
- docs/new_upload_format.md — новый 3-файловый комплект портала с 2026-07-13 (pdf + `*_results.md` + `*_results.html`, БЕЗ result.json): чем отличается от старого квартета, этапы интеграции (приём → конвертер MD → псевдо-result.json+кэш кропов → парсер html), план деприкации приёма старого метода после ~2026-08-14 (грепать `2026-08-14`; чтение старых суффиксов не удалять никогда)
- docs/stage_comparison_main_path_selfcheck.md — r6 self-check на ОСНОВНОМ пути сравнения (не только too_large): каждый change от Opus сверяется с исходным MD через `verify_change_evidence` + числовой re-cite (`_salient_numbers`, практичная замена r3 для CAD-чертежей без векторного текст-слоя); негрунтованные → `requires_human_review` (мягкий режим) либо drop; флаги STAGE_COMPARISON_SELFCHECK_ENABLED / _DROP_UNGROUNDED, оба default OFF; fail-soft
- docs/stage_comparison_diff_contract_alignment.md — r5/r4/r1 против «не распознал = убрал»: r5 контракт Opus (тип `present_one_side`, цитировать ОБА значения, `disputed`→questionable; always-on prompt); r4 выравнивание отходящих линий по ИМЕНИ потребителя + словарь `consumer_synonyms.json` (тег `<CONSUMER_SYNONYMS>`, env override; always-on); r1 фиксированная доменная схема Qwen `domain_fields` с «не указано» для отсутствующих (флаг STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED default OFF, версия v6 → re-enrichment)
- docs/stage_comparison_block_pdf_source.md — Graphic Structured Extraction: универсальный слой профилей (`graphic_profiles.py`) для image-блоков, GRSH = первый профиль `electrical_singleline/grsh` (не костыль); классификатор block_type→profile, 8 профилей-схем (electrical/hvac/water/low_voltage/structural/architectural/table/stamp), `field_state` (present/not_extracted/visual_unverified/ocr_only/…, NON_REMOVAL_STATES); универсальный helper `block_pdf_source.py` (crop_url block-PDF + `pdfplumber_text` text-layer как OCR-словарь, resolve/extract/render/validate); контур A `STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED` (block-PDF render+vocab вместо page-crop); контур B `grsh_feeder_extraction.py` — tiled пофидерное извлечение ГРЩ/ВРУ (concurrency=1!), merge+recall+anti-hallucination; флаг `STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED` (GRSH-флаг = backward-compat alias), все default OFF; live-валидация p9692b6b5: recall OLD 0.947 / NEW 1.0, 0 искусственных рядов, 19 changes vs baseline 10; page-crop = fallback
- docs/stage_comparison_stamp_sheet_matching.md — сопоставление листов по штампу (page-alignment): `stamp_matching.py` глобально матчит листы старой/новой стадии по `**Наименование листа:**` из MD (находит листы, уехавшие далеко — схема ВРУ стр.51↔32), фолбэк на текст-слой result.json; forward-fill продолжений, IDF-взвешенный косинус токенов + margin-гейт (неоднозначные имена не предлагаются, precision>recall); `store.suggest_alignment_by_stamp` + `POST .../page-alignment/suggest-by-stamp` (ничего не применяет); UI-кнопка «🏷 Сопоставить по штампам» в «Связь блоков» → панель предложений → «Применить» ставит листы напротив (обычный PUT page-alignment); always-on, офлайн (без Qwen/сети), env-тюнинг `STAGE_COMPARISON_STAMP_MATCH_*`
- docs/stage_comparison_block_equivalence_precheck.md — pre-Qwen block equivalence gate (Stage 1: **observe only**, default OFF): `block_equivalence_precheck.py` сравнивает блоки result.json OLD↔NEW (pairing по `coords_norm`/IoU + page_alignment, split/merge→uncertain, added/deleted), строгое canonical-равенство текста + визуал через `cv2.findTransformECC` (MOTION_EUCLIDEAN, total/colored-diff, diff_bbox); decision→qwen_action (`qwen_skip_candidate` только при уверенной идентичности, иначе `qwen_required`); НИЧЕГО не пропускает (skip — Stage 2); cv2 опционален (без него visual→qwen_required, не ложный skip); артефакт `block_equivalence/block_equivalence_report.json` + debug PNG; хук в `run_md_enrichment_job` (observe, `asyncio.to_thread`, fail-soft) + surface в pipeline_queue per-pair; флаг `STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED`
- docs/vectograf.md — «Вектограф» (vectograf): детерминированное построение графа однолинейной схемы (ВРУ/ГРЩ/РП) из вектор-слоя PDF по геометрии координат, без нейросети/OCR; связка `singleline_structurer.py` (разбор текста-формул) + `singleline_graph_geometry.py` (топология по координатам) + рендер Markdown; точка входа — `/blocks/llm-text` (blocks.py секции 6/7), панель «🔌 Граф схемы» в txt; ~1,5с/блок (в осн. открытие PDF), 0 токенов, офлайн; работает ТОЛЬКО по вектор-слою (сканы → Qwen); grep `Вектограф`/`vectograf`
- docs/block_captions.md — гуманизация ссылок на блоки в текстах замечаний: block_id («6L97-3VTH-XTC») в problem/description/solution/risk → подписи «Название» (лист N, стр. PDF M); запрет ID в промптах merge/01/02/opt + детерминированный пост-проход block_captions.py в findings_merge (флаг FINDINGS_BLOCK_CAPTIONS_ENABLED default ON); найденные в тексте ID переносятся в related_block_ids; backfill старых данных — отдельный шаг по команде
- docs/stable_finding_id.md — спека (дизайн для AuditManager rewrite, кода нет): почему `F-NNN` сбивается между прогонами (позиционная перенумерация в `findings_merge`: `merge_similar_findings` + `phase0_dedup`) и метод стабильной идентичности — отделить `ordinal` (косметика) от вечного `uid`; tracking по фингерпринту (version_id+sheet+norm+category+severity+`_normalize_problem_pattern`+`_salient_numbers`) с append-only реестром `finding_identity.json` (exact→fuzzy→mint, uid не переиспользуется); decisions/expert_review/KB ключуются на `uid` (он кодирует версию → хайдрейтинг читает нужную версию, а не latest, чинит пустые строки БЗ); кросс-версия = отдельная ось (`origin_finding_id`/`_stable_migrated_id`); миграция 447 орфанов version-aware-проходом; Python-шим — отдельный трек
