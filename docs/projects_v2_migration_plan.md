# projects_v2 — план миграции

**Дата:** 2026-06-15
**Статус:** ЭТАП 1 выполнен (инструменты + тестовая миграция одного проекта).

Стандарт раскладки: `docs/projects_v2_storage_standard.md`.

## Принципы

- **Аддитивность.** Создаётся только параллельная `projects_v2/`. `projects/` и
  `comparison/` не изменяются и не удаляются.
- **Lossless.** Полный `_output` копируется verbatim в
  `03_analysis/runs/<run_id>/`. Классификация по buckets — поверх копии.
- **Проверяемость.** Каждый скопированный файл фиксируется в `old_to_new_map.json`
  с sha256; `validate_migration.py` сверяет копию и неизменность legacy.
- **Без подключения backend.** UI/pipeline работают на старой структуре до
  отдельного решения о cutover.

## Этапы

### Этап 1 — подготовка (этот PR) ✅

1. Документация (`storage_standard`, `migration_plan`).
2. `inventory_legacy_projects.py` — read-only инвентарь всей `projects/`.
3. `migrate_one_project_to_v2.py` — миграция одного проекта/контейнера.
4. `validate_migration.py` — проверка миграции и неизменности legacy.
5. Тестовая миграция **одного небольшого проекта**.
6. Тесты (`tests/test_projects_v2.py`).

### Этап 1.5 — readiness report (выполнено) ✅

`scripts/projects_v2/report_migration_readiness.py` — read-only отчёт готовности
по всем legacy-проектам (на базе legacy + `migration_inventory.json`). Пишет
`projects_v2/_system/migration_readiness_report.{json,csv}`. Делит проекты на:

| Группа | Смысл |
|---|---|
| `AUTO_SAFE` | полный комплект + `project_info`, plain/валидный контейнер, без конфликтов и `.pdf`-папок — мигрировать автоматически |
| `CAN_MIGRATE_WITH_WARNINGS` | мигрируется, но есть мелкие warning (`.pdf` в имени папки версии при наличии `version_group`, нет анализа у V2, нет `_ocr.html`, грязные legacy-артефакты, уже мигрирован, объект не в реестре) |
| `MANUAL_REVIEW_REQUIRED` | блокеры: неполный комплект, несколько PDF/MD/JSON в папке, нет `project_info.json`, конфликт `document_code`, контейнер без `version_group.json` |
| `SKIP_EMPTY_OR_INVALID` | пусто/мусор/не проект |

Чистая логика классификации — в `scripts/projects_v2/readiness.py`
(`classify_readiness`), покрыта `tests/test_projects_v2_readiness.py`.

### Этап 2 — пакетная миграция (план)

- По `migration_readiness_report.json` мигрировать сначала `AUTO_SAFE`, затем
  `CAN_MIGRATE_WITH_WARNINGS`; `MANUAL_REVIEW_REQUIRED` — отдельным разбором;
  `SKIP_EMPTY_OR_INVALID` — пропуск.
- Проблемные (`.pdf` в имени, отсутствующий `version_group`, missing quad,
  несколько PDF/MD/JSON) — ручной разбор.
- После каждого батча — `validate_migration.py`.
- Связать сравнения: заполнить `comparisons/<vA>_vs_<vB>/comparison_link.json`
  ссылками на `comparison/sessions/...` (без копирования сессий).
- Решить судьбу `comparison/` (вероятно остаётся отдельным деревом, только
  линкуется из документов).

### Этап 2.1 — batch migration + пилот AUTO_SAFE (выполнено) ✅

`scripts/projects_v2/batch_migrate_projects_v2.py` — пакетная миграция по
`migration_readiness_report.json`. Чистые функции `validate_request` /
`select_candidates` покрыты `tests/test_projects_v2_batch_migration.py`.

**Safety-инварианты (по умолчанию всё запрещено):**

- без `--execute` — **DRY-RUN**, ничего не копируется (план в отчёте);
- `--class` обязателен; неизвестный класс → ошибка;
- `MANUAL_REVIEW_REQUIRED` и `SKIP_EMPTY_OR_INVALID` — миграция запрещена всегда;
- класс ≠ `AUTO_SAFE` требует явного `--allow-warnings`;
- `--skip-already-migrated` пропускает уже мигрированные (проверка по факту —
  наличие `document.json` в projects_v2, не по отчёту);
- перед копированием проверяется существование legacy-path;
- перезапись существующего документа запрещена → `--force` (намеренно **НЕ
  реализован**, всегда ошибка);
- fail-soft: ошибка одного проекта не валит весь батч (status=error в отчёте).

**Dry-run → pilot:**

```bash
# 1) dry-run (ничего не копирует):
python scripts/projects_v2/batch_migrate_projects_v2.py \
    --dry-run --class AUTO_SAFE --limit 5 --skip-already-migrated \
    --legacy-root .../projects --v2-root .../projects_v2

# 2) реальный пилот (после проверки dry-run):
python scripts/projects_v2/batch_migrate_projects_v2.py \
    --execute --class AUTO_SAFE --limit 5 --skip-already-migrated \
    --legacy-root .../projects --v2-root .../projects_v2

# 3) проверка:
python scripts/projects_v2/validate_migration.py --v2-root .../projects_v2
```

**Почему сначала только `AUTO_SAFE`:** это проекты с полным входным комплектом,
`project_info.json`, без конфликтов `document_code` и без `.pdf`-папок версий —
миграция детерминирована и не требует решений. `CAN_MIGRATE_WITH_WARNINGS`
мигрируется отдельно (осознанно, с `--allow-warnings`), `MANUAL_REVIEW_REQUIRED`
— только после ручного разбора.

**Почему `--limit 5`:** контролируемый пилот. Малая партия проверяется validate +
ручным осмотром дерева до масштабирования на все 42 `AUTO_SAFE`. Партиями легче
ловить регрессии и считать дисковый объём.

**Как читать `batch_migration_report.{json,csv}`** (в `projects_v2/_system/`):

- `summary`: `mode` (dry_run|execute), `class`, `selected`,
  `skipped_already_migrated`, `migrated`, `planned`, `errors`,
  `copied_files_total`, `checksum_checked_total`;
- `projects[]`: per-project `status` (`planned`|`migrated`|`error`), `old_path`,
  `new_path`, `object_id`, `discipline`, `document_code`, `version_count`,
  `copied_files_count`, `checksum_checked_count`, `error_message`;
- `skipped_already_migrated[]`: что пропущено как уже мигрированное.

После пилота: `validate_migration.py` должен дать PASS по всем новым документам;
повторный `--execute --skip-already-migrated` обязан давать `selected=0`.

### Этап 2.2 — разбор CAN_MIGRATE_WITH_WARNINGS + ALREADY_MIGRATED (выполнено) ✅

После миграции всех `AUTO_SAFE` **нельзя сразу гнать все warnings одной партией**:
группа `CAN_MIGRATE_WITH_WARNINGS` неоднородна — часть проектов безопасна для
батча, часть требует решения, часть фактически заблокирована. Поэтому warnings
разбираются по типам предупреждений отдельной политикой.

**Новая группа `ALREADY_MIGRATED`.** Проект считается мигрированным, только если
выполнены ОБА условия: есть `document.json` в `projects_v2` И запись в
`old_to_new_map.json` (`is_already_migrated`). Такие проекты выводятся в
отдельную группу и больше **не загрязняют** статистику `CAN_MIGRATE_WITH_WARNINGS`
и не считаются кандидатами на повторную миграцию. Если v2-папка есть, а записи в
карте нет — это несогласованность (`v2_present_not_in_map`, warning), проект
остаётся в warnings для разбора, не в `ALREADY_MIGRATED`.

Свежая сводка readiness теперь печатает 5 групп + три явных числа:
`remaining_candidates` (AUTO_SAFE), `already_migrated_count`,
`not_migrated_warning_count`.

**Warning-policy report** (`migration_warning_policy_report.{json,csv}`) делит
НЕ-мигрированные, НЕ-AUTO_SAFE проекты на три подгруппы
(`classify_warning_policy`, приоритет blocker > need_policy > auto_candidate):

| Подгруппа | recommendation | Критерии |
|---|---|---|
| `WARNINGS_AUTO_CANDIDATE` | `can_batch_migrate` | только безопасные warnings: `messy_legacy_artifacts`, `no_analysis` (полный комплект → пустой `03_analysis`), `pdf_in_version_folder_name` **при наличии `version_group.json`**; их сочетания без blocker |
| `WARNINGS_NEED_POLICY` | `needs_policy` | `missing_ocr_html` (есть PDF+MD+result, нет `_ocr.html`); `.pdf` в имени папки версии **без** `version_group`; объект не в реестре; `v2_present_not_in_map`; любой неизвестный warning |
| `WARNINGS_BLOCKED` | `manual_only` | blocker-сигналы: `multiple_pdf/md/result`, `incomplete_input_quad`, нет `project_info.json`, конфликт `document_code`, контейнер без `version_group.json` — backend fallback может выбрать не тот файл |

**Как принимать решение по следующему пилоту warning-проектов:**

1. Брать только `WARNINGS_AUTO_CANDIDATE` (`recommendation=can_batch_migrate`) —
   эти warnings не меняют детерминизма миграции (полный комплект, корректный
   `version_group`, отсутствие анализа переносится честно пустым `03_analysis`).
2. Запускать батчами по 10 с `--allow-warnings`, как и `AUTO_SAFE`, с validate
   после каждой партии.
3. `WARNINGS_NEED_POLICY` — не трогать до отдельного решения по каждому типу
   (например: достраивать ли `_ocr.html`; как трактовать plain-папку с `.pdf` в
   имени без `version_group`).
4. `WARNINGS_BLOCKED` / `MANUAL_REVIEW_REQUIRED` — только ручной разбор.

Чистая логика — `scripts/projects_v2/readiness.py`
(`classify_readiness`, `classify_warning_policy`, `is_already_migrated`),
покрыта `tests/test_projects_v2_warning_policy.py`.

### Этап 2.3 — миграция warning-проектов (WARNINGS_AUTO_CANDIDATE) ✅

**Почему нельзя мигрировать весь `CAN_MIGRATE_WITH_WARNINGS` одной партией.**
Группа неоднородна (см. этап 2.2): `WARNINGS_AUTO_CANDIDATE` безопасна для
батча, `WARNINGS_NEED_POLICY` требует решения по каждому типу warning,
`WARNINGS_BLOCKED` фактически заблокирована. Гнать их вместе = протащить
неоднозначные/битые проекты без разбора.

**Почему нужен `--warning-policy`.** Это явный «предохранитель»: чтобы
мигрировать warnings, оператор обязан указать КОНКРЕТНУЮ безопасную подгруппу.
`batch_migrate_projects_v2.py` тогда читает
`migration_warning_policy_report.json` и берёт только проекты с
`policy_group == WARNINGS_AUTO_CANDIDATE` И `recommendation == can_batch_migrate`.

**Запуск пилота `WARNINGS_AUTO_CANDIDATE`:**

```bash
# dry-run:
python scripts/projects_v2/batch_migrate_projects_v2.py \
    --dry-run --class CAN_MIGRATE_WITH_WARNINGS \
    --warning-policy WARNINGS_AUTO_CANDIDATE --allow-warnings \
    --limit 10 --skip-already-migrated \
    --legacy-root .../projects --v2-root .../projects_v2

# execute (после проверки dry-run) — те же флаги с --execute, затем:
python scripts/projects_v2/validate_migration.py --v2-root .../projects_v2
```

**Запрещённые/защищённые случаи (`validate_request`):**

| Запрос | Результат |
|---|---|
| `--class CAN_MIGRATE_WITH_WARNINGS` без `--warning-policy` | ошибка |
| `--warning-policy WARNINGS_AUTO_CANDIDATE` без `--allow-warnings` | ошибка |
| `--warning-policy WARNINGS_NEED_POLICY` | ошибка (нужен отдельный флаг, пока не реализован) |
| `--warning-policy WARNINGS_BLOCKED` | ошибка (никогда) |
| `--warning-policy` с не-warnings классом | ошибка |
| `MANUAL_REVIEW_REQUIRED` / `ALREADY_MIGRATED` как `--class` | ошибка |
| `--force` | ошибка (не реализован) |
| target exists без force | per-project status=error |

Все остальные safety-инварианты этапа 2.1 сохранены (dry-run по умолчанию,
skip-already-migrated по факту наличия `document.json`, fail-soft).

`WARNINGS_NEED_POLICY` и `WARNINGS_BLOCKED` в этом этапе **не мигрируются**.

### Этап 2.4 — человекочитаемые папки объектов ✅

Папки объектов переведены с технических `obj_<hash>` на читаемые имена
(`make_object_folder_name`): `obj_0b540226 → 213_Mosfilmovskaya_31A_KingSons`,
`obj_73a0e59a → 214_Alia_ASTERUS`. Правила именования — см.
[projects_v2_storage_standard.md](projects_v2_storage_standard.md) («Имена папок
объектов»). `object_id` сохраняется только в `object.json`; `obj_<hash>` как имя
папки запрещён (кроме legacy/runtime до переименования).

- Новые миграции сразу создают читаемую папку (`allocate_object_folder`,
  конфликт → суффикс `_<object_id>`), `object.json` несёт `display_name` +
  `folder_name`.
- Уже созданные папки переименованы скриптом
  `scripts/projects_v2/rename_object_folders.py` (dry-run по умолчанию;
  `--execute` для реального переименования; конфликт → остановка). Скрипт
  переименовывает папку, обновляет `object.json`, `old_to_new_map.json` и
  generated reports/архивы в `_system/`; legacy `projects/` и `comparison/` не
  трогает.
- `resolve_object_folder` находит путь по читаемому имени → по `object_id` из
  `object.json` → по legacy `obj_<id>`, поэтому downstream работает и до, и после
  переименования.

```bash
python scripts/projects_v2/rename_object_folders.py --dry-run  --v2-root .../projects_v2
python scripts/projects_v2/rename_object_folders.py --execute  --v2-root .../projects_v2
```

### Этап 2.5 — legacy drift и безопасный refresh snapshot ✅

**Что такое legacy drift.** Миграция в projects_v2 — это снимок legacy на момент
копирования (sha записаны в `old_to_new_map.json`). Если legacy-файл изменится
ПОСЛЕ миграции, `validate_migration.py` помечает его `LEGACY CHANGED`.

**Почему возникает.** Backend-аудит работает на живом сервере и продолжает
писать в legacy `_output/` (`pipeline_log.json`, `audit_log.jsonl`,
`optimization_pre_review.json`) и иногда в входные файлы (`_document.md` при
ре-OCR) уже после того, как документ мигрирован. Это видно по `mtime` файлов.

**Почему это НЕ ошибка миграции.** Копии в projects_v2 целы (у validate нет
ошибок `checksum drift (new copy)`), запись в карте консистентна — расходится
только живой источник. То есть дрейфует legacy, а не наш снимок.

**Как безопасно делать refresh одного мигрированного snapshot.**
`scripts/projects_v2/refresh_migrated_snapshot.py` пере-снимает ОДИН уже
мигрированный документ/версию из `old_to_new_map.json`:

```bash
python scripts/projects_v2/refresh_migrated_snapshot.py --dry-run \
    --document "13АВ-РД-АР3-К6" --version v002 \
    --v2-root .../projects_v2 --stable-seconds 120
# затем --execute, потом validate_migration.py
```

Гарантии:
- **stability-check**: два снимка legacy с паузой `--stable-seconds`; если файлы
  меняются между снимками (аудит ещё идёт) — refresh **прерывается**, ничего не
  трогая. Обновляем только стабильный legacy;
- работает строго по записи в `old_to_new_map.json` (не создаёт новую миграцию,
  не делает массовый обход);
- архивирует старую v2-копию в `_system/refresh_archive/<doc>/<version>/<ts>/`
  перед перезаписью; обновляет sha/size в карте; пишет
  `_system/refresh_report.{json,csv}`;
- legacy `projects/` и `comparison/` не трогаются (legacy только читается).

**Почему запрещён общий `--force`.** Массовый/безусловный refresh «затихни и
перезапиши всё» опасен: он может затереть снимок документом, который прямо сейчас
активно пишется живым аудитом (гонка), и скрыть реальные расхождения. Поэтому
refresh — только по одному документу и только при доказанной стабильности
legacy; глобального `--force` нет by design.

> Замечание: legacy может дрейфовать по РАЗНЫМ документам по мере работы
> аудита. Refresh целевого документа устраняет его drift; если в окне операции
> «уехал» другой документ — это новый отдельный drift того же класса, лечится
> тем же инструментом по этому документу, когда его аудит устаканится.

### Этап 2.6 — глобальный drift scan → refresh_safe → validate PASS ✅

Вместо ручной гонки за одним документом — сначала **глобальная диагностика**
дрейфа по всем уже мигрированным документам, потом точечный refresh только
стабильных.

**drift scan** (`scripts/projects_v2/scan_migrated_drift.py`, READ-ONLY) идёт по
`old_to_new_map.json`, сравнивает по каждому tracked-файлу recorded sha ↔ текущий
legacy sha ↔ текущий v2 sha и классифицирует:

| drift_type | смысл | recommendation |
|---|---|---|
| `legacy_changed_v2_old` | legacy ушёл вперёд, v2 = recorded (живой-аудит drift) | `refresh_safe` если legacy стабилен, иначе `wait_backend` |
| `v2_changed` | изменилась копия в projects_v2 | `manual_review` |
| `missing_legacy` | legacy-файл пропал | `manual_review` |
| `missing_v2` | копия в projects_v2 пропала | `manual_review` |

Для каждого drift-документа выполняется stability-check (два снимка legacy с
паузой `--stable-seconds`): меняется → `unstable`/`wait_backend`, не меняется →
`stable`. Отчёты: `_system/migrated_drift_scan_report.{json,csv}`. Скан ничего
не копирует и не меняет.

**Отличие ошибки миграции от живого изменения legacy.** Ошибка миграции = копия
в projects_v2 неверна (`v2_changed` / `checksum drift (new copy)` у validate).
Живой drift = legacy ушёл вперёд после снимка (`legacy_changed_v2_old`), а копия
цела. Первое — баг, второе — нормальная гонка с работающим backend-аудитом.

**Почему нельзя refresh-ить нестабильный документ.** Если legacy всё ещё
пишется (аудит идёт), refresh снимет промежуточное состояние и может тут же
снова разойтись (или поймать полузаписанный файл). Поэтому refresh — только при
`stable=True` (`refresh_safe`); `wait_backend` оставляем до завершения аудита.

**Порядок: scan → refresh_safe → validate.**

```bash
# 1) глобальный скан
python scripts/projects_v2/scan_migrated_drift.py --v2-root .../projects_v2 --stable-seconds 120
# 2) для КАЖДОГО документа с recommendation=refresh_safe:
python scripts/projects_v2/refresh_migrated_snapshot.py --execute \
    --document "<code>" --version "<vid>" --v2-root .../projects_v2 --stable-seconds 120
# 3) полный validate -> PASS
python scripts/projects_v2/validate_migration.py --v2-root .../projects_v2
```

Массовый refresh без списка из scan не делается; `unstable`/`v2_changed`/`missing_*`
требуют ручного разбора.

**Почему перед миграцией `WARNINGS_NEED_POLICY` нужен полный validate PASS.**
`NEED_POLICY` — следующий рискованный класс. Прежде чем его трогать, состояние
projects_v2 должно быть доказуемо чистым (validate PASS): иначе нельзя отличить
новый эффект миграции от уже накопленного drift, и регрессии будет не видно.
Чистый validate — это базовая линия, относительно которой оценивается следующий
этап.

### Этап 2.7 — новый класс drift: `legacy_new_file_not_in_map` ✅

**Что это.** legacy получил НОВЫЕ файлы после миграции, которых нет ни в snapshot,
ни в `old_to_new_map.json`. Частный случай — V2-документ был мигрирован без
анализа (`analysis_status=none`), а затем живой backend-аудит дописал в legacy
`_output/` `01_text_analysis.json` / `02_blocks_analysis.json` / `03_findings.json`
и др. Тогда validate падает на `CRITICAL artifact lost` (файл есть в legacy, нет
в v2).

**Почему возникает.** Backend работает на живом сервере и продолжает аудит
документов уже после их миграции — это нормальная гонка, а не ошибка миграции
(копии в v2 целы).

**Почему ловится отдельно.** Старый `scan_migrated_drift.py` сравнивал только
файлы из карты, поэтому НОВЫЕ файлы не видел. Теперь scan дополнительно ищет
whitelist-файлы в legacy, которых нет в карте → `legacy_new_file_not_in_map`.

**Whitelist новых файлов** (только они добавляются автоматически):
`01_text_analysis.json`, `02_blocks_analysis.json`, `03_findings.json`,
`03_findings_review.json`, `norm_checks.json`, `03a_norms_verified.json`,
`optimization.json`, `optimization_review.json`, `pipeline_log.json`,
`audit_log.jsonl` (все в `_output/`). НЕ добавляются: backup-папки
(`_bench_backup_*`), `cache/raw/prompts`, debug-файлы, любое вне whitelist.
Это защита от затягивания мусора в snapshot.

**Как добавлять безопасно** (`refresh_migrated_snapshot.py --include-new-files`):
по одному document/version, после stability-check; нестабильный legacy →
прерывание; legacy только читается.

```bash
python scripts/projects_v2/scan_migrated_drift.py --v2-root .../projects_v2 --stable-seconds 120
# для каждого refresh_safe документа с legacy_new_file_not_in_map:
python scripts/projects_v2/refresh_migrated_snapshot.py --execute --include-new-files \
    --document "<code>" --version "<vid>" --v2-root .../projects_v2 --stable-seconds 120
python scripts/projects_v2/validate_migration.py --v2-root .../projects_v2   # -> PASS
```

**Куда кладутся новые файлы.** Новые analysis-артефакты — это live-дописанный
анализ, его нельзя смешивать со старым snapshot. Поэтому создаётся отдельный
`03_analysis/runs/run_refresh_<timestamp>/` (verbatim-копии), а критичные/основные
analysis-файлы дублируются в `03_analysis/latest/`. Отдельный run, а не запись в
существующий — чтобы старый и новый (live) анализ не перемешались.

**`analysis_status` в `version.json`** (выставляется при refresh):
`complete` (есть 01+02+03), `partial` (часть), `none` (нет). Плюс
`analysis_refreshed_at` и `analysis_refresh_reason=legacy_new_analysis_artifacts`.

**Почему это НЕ новая миграция проекта.** Документ уже мигрирован и есть в
`old_to_new_map`; refresh лишь до-снимает добавленные legacy-файлы в его же
версию (по whitelist, со stability-check, с архивом). Новый проект/версия не
создаётся, массового обхода нет.

**Пауза backend.** Существует штатный механизм (`POST /api/audit/pause`
`finish_current` + `POST /api/audit/resume`, in-memory, обратимый). Использовать
его опционально: если backend активно пишет — можно поставить на паузу и
обязательно затем возобновить; если backend idle — достаточно stability-check
самого refresh. Убивать процесс/делать restart нельзя.

### Этап 2.8 — разбор WARNINGS_NEED_POLICY + политика миграции ✅ (анализ, без миграции)

`scripts/projects_v2/analyze_need_policy_projects.py` (READ-ONLY) разбирает
проекты `policy_group=WARNINGS_NEED_POLICY / recommendation=needs_policy` и
делит их на подгруппы (`classify_need_policy`, приоритет worst-first). Отчёты:
`_system/need_policy_analysis_report.{json,csv}`. Миграция НЕ выполняется.

| Подгруппа | Когда | next_class | auto после политики |
|---|---|---|---|
| `POLICY_RECHECK_AS_BLOCKED` | blocker (multiple_pdf/md/result, нет project_info, конфликт document_code, неполный комплект) | WARNINGS_BLOCKED | нет |
| `POLICY_READY_LEGACY_KB_PRESERVE` | legacy-объект (King&Sons) + KB-связь/старый анализ, новый pipeline неполный | WARNINGS_AUTO_CANDIDATE | да (`analysis_status=legacy_partial`) |
| `POLICY_READY_SINGLE_PDF_NAMED_FOLDER` | `... V1.pdf/` без `version_group` и без sibling-версий | WARNINGS_AUTO_CANDIDATE | да (как `v001`, старое имя в metadata) |
| `POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN` | `... V1/V2/V3.pdf/` без `(main)`, однозначно один logical document | WARNINGS_AUTO_CANDIDATE | да (объединить в `versions/v001..v00N`) |
| `POLICY_NEEDS_MANUAL_VERSION_GROUPING` | похожие V1/V2/V3, но связь неоднозначна | MANUAL_REVIEW_REQUIRED | нет |
| `POLICY_READY_MISSING_OCR_HTML` | есть `.pdf+_document.md+_result.json+project_info`, нет `_ocr.html` | WARNINGS_AUTO_CANDIDATE | да (`missing_optional_files:["ocr_html"]`) |
| `POLICY_READY_NO_ANALYSIS` | комплект полный, анализа нет | WARNINGS_AUTO_CANDIDATE | да (`analysis_status=none`) |
| `POLICY_READY_PARTIAL_ANALYSIS` | часть analysis-файлов | WARNINGS_AUTO_CANDIDATE | да (`analysis_status=partial` + `missing_analysis_files`) |

**Политики (утверждены пользователем):**

- **`_ocr.html` — optional.** Если есть `.pdf+_document.md+_result.json`,
  отсутствие `_ocr.html` не блокирует. При миграции: фейковый `ocr.html` не
  создаётся, в `input_manifest.json` пишется `missing_optional_files:["ocr_html"]`.
- **Все документы в projects_v2 — контейнеры версий** (`documents/<code>/versions/vNNN`),
  даже если в legacy не было `(main)`.
- **`... V1.pdf/`/`... V2.pdf/` без `(main)`:** одиночная → один document с `v001`
  (старое имя в metadata); несколько с однозначной нумерацией → объединить в один
  document с `versions/v001..v00N`; неоднозначно → manual.
- **Анализ:** нет → `analysis_status=none`; неполный → `partial` +
  `missing_analysis_files`; всё переносить, ничего не терять.
- **Legacy King&Sons (старые алгоритмы):** отсутствие файлов нового pipeline НЕ
  блокирует. Главное — сохранить найденное (`03_findings.json`/`01`/`02`/
  `pipeline_log` если есть, legacy batch/block-файлы, связь с
  `knowledge_base/decisions_log.json`, экспертные решения). Такие проекты —
  `POLICY_READY_LEGACY_KB_PRESERVE`, в `version.json`:
  `analysis_status=legacy_partial`, `analysis_generation=legacy`,
  `preserve_reason=legacy_algorithm_with_kb_findings`. KB-связь определяется по
  `source_project` в `decisions_log.json` (по logical base документа).

**Результат разбора (44 NEED_POLICY, на текущих данных):**
`POLICY_READY_SINGLE_PDF_NAMED_FOLDER` 28, `POLICY_READY_MISSING_OCR_HTML` 15,
`POLICY_READY_LEGACY_KB_PRESERVE` 1 (King&Sons `133_23-ГК-СОТ V1`, kb_linked),
остальные подгруппы 0. **Все 44 — кандидаты на следующий пилот после
утверждения политики; 0 manual; 0 в blocked.** Чистые функции — в
`analyze_need_policy_projects.py`, тесты —
`tests/test_projects_v2_need_policy_analysis.py`.

> ⚠️ Перед фактической миграцией NEED_POLICY нужен полный `validate PASS`. Если
> живой backend-аудит дрейфит ALREADY_MIGRATED-документы, baseline временно
> RED — это не блокирует READ-ONLY анализ, но блокирует саму миграцию (сначала
> drift scan → refresh refresh_safe → validate PASS).

### Этап 2.9 — пилотная миграция approved WARNINGS_NEED_POLICY

Инструмент готов (`batch_migrate_projects_v2.py --need-policy-approved`),
протестирован hermetic-тестами и dry-run; **фактический execute выполняется
только при чистом baseline** (validate PASS).

**Селектор `--need-policy-approved`** читает `need_policy_analysis_report.json` и
берёт только проекты с `can_migrate_auto_after_policy=true` И subgroup из
одобренного списка: `POLICY_READY_SINGLE_PDF_NAMED_FOLDER`,
`POLICY_READY_MISSING_OCR_HTML`, `POLICY_READY_LEGACY_KB_PRESERVE`,
`POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN`. Подгруппы
`POLICY_NEEDS_MANUAL_VERSION_GROUPING` / `POLICY_RECHECK_AS_BLOCKED`, а также
`WARNINGS_BLOCKED` / `MANUAL_REVIEW_REQUIRED` — **не выбираются**.

Safety: без `--need-policy-approved` проекты NEED_POLICY не мигрируются;
`--need-policy-approved` несовместим с `--warning-policy` и требует
`--class CAN_MIGRATE_WITH_WARNINGS` (но не требует `--allow-warnings` — это
собственный явный gate одобрения); already-migrated пропускаются; target exists
без force → ошибка; общего `--force` нет.

**Правила миграции по policy-группам** (применяются `migrate_version`):
- `MISSING_OCR_HTML` — переносится без `_ocr.html`; фейковый `ocr.html` не
  создаётся; `input_manifest.json.missing_optional_files=["ocr_html"]`.
- `SINGLE_PDF_NAMED_FOLDER` — `... V1.pdf/` → `versions/v001`, старое имя в
  `version.json.legacy_folder_name` и `input_manifest.json`.
- `LEGACY_KB_PRESERVE` — legacy-снимок: `version.json` →
  `analysis_status=legacy_partial`, `analysis_generation=legacy`,
  `preserve_reason=legacy_algorithm_with_kb_findings`.
- Дополнительно `migrate_version` теперь ВСЕГДА пишет `analysis_status`
  (none/partial/complete) и `missing_analysis_files`, а `input_manifest.json` —
  `missing_optional_files` (generic, для всех миграций).

**Почему сначала пилот 10.** Контролируемая партия проверяется validate +
ручным осмотром до масштабирования на все 44; партиями легче ловить регрессии.

**Почему нужен validate PASS перед миграцией.** Если baseline RED (живой
backend-аудит дрейфит ALREADY_MIGRATED-документы), нельзя отличить эффект новой
миграции от накопленного drift. Поэтому Stage A: `scan → refresh refresh_safe →
validate PASS`, и только потом execute. Если на Stage A документ `unstable`
(backend пишет прямо сейчас) — refresh прерывается, execute не выполняется.

### Этап 2.10 — manual/blocked King&Sons: legacy-findings-preserve (анализ, без миграции)

Оставшиеся 4 проекта `WARNINGS_BLOCKED` / `MANUAL_REVIEW_REQUIRED` — все объекта
`213. Мосфильмовская 31А "King&Sons"`, анализированы старыми алгоритмами:
`EOM/133_23-ГК-ЭМ2`, `EOM/Фасадное освещение`, `ITP/133_23-ГК-ИТП.ТМ`,
`SS/133_23-ГК-АК`. Для них блокеры (multiple PDF/MD/result, incomplete quad) НЕ
должны блокировать перенос — главное сохранить найденные замечания и связь с KB.

**Политика `POLICY_READY_LEGACY_FINDINGS_PRESERVE`** (не угадывать «основной» PDF,
переносить как legacy-снимок, ничего не теряя). Read-only анализ:
`scripts/projects_v2/analyze_blocked_manual_projects.py` →
`_system/blocked_manual_analysis_report.{json,csv}`. **Миграция этих 4 проектов
выполняется только после отдельного подтверждения.**

Целевая структура (для будущей миграции):

```text
versions/v001/
  01_input/legacy_bundle/     # ВСЕ найденные pdf/md/ocr/result как есть (без выбора primary)
  03_analysis/latest/         # 03_findings.json / 01 / 02 / pipeline_log если есть
  99_service/legacy_output/   # полная копия legacy _output/ (контекст)
```

`version.json`:
```json
{
  "analysis_status": "legacy_partial",
  "analysis_generation": "legacy",
  "preserve_reason": "king_sons_legacy_findings_preserve",
  "source_files_strategy": "legacy_bundle",
  "primary_goal": "preserve_findings_and_kb_links"
}
```

**Результат анализа (4/4 → LEGACY_FINDINGS_PRESERVE):**

| Документ | blockers | 03_findings | KB-записи | source-файлов |
|---|---|---|---|---|
| EOM/133_23-ГК-ЭМ2 | multiple pdf/md/result | ✅ (+01/02/pipeline_log) | 0 | 15 |
| EOM/Фасадное освещение | multiple pdf/md/result | — | 0 | 9 |
| ITP/133_23-ГК-ИТП.ТМ | incomplete_input_quad | — | 0 | 3 (только PDF) |
| SS/133_23-ГК-АК | multiple pdf/md/result | ✅ (+01/02/pipeline_log) | **4** | 17 |

Все source-файлы (включая неоднозначные дубли ролей) сохраняются в
`legacy_bundle` без выбора primary; `_output` целиком — в `legacy_output`;
найденные `03_findings`/анализ — в `03_analysis/latest`; KB-связь (по
`source_project` в `decisions_log.json`) фиксируется в отчёте. `unclassified`
файлы тоже попадают в `legacy_bundle` (ничего не теряем). Чистые функции —
`analyze_blocked_manual_projects.py`, тесты —
`tests/test_projects_v2_blocked_manual_analysis.py`.

### Этап 2.11 — migrate King&Sons legacy snapshot (ВЫПОЛНЕНО, по подтверждению)

После отдельного подтверждения 4 проекта мигрированы как legacy snapshot
инструментом `scripts/projects_v2/migrate_legacy_findings_preserve.py`
(обязательный флаг `--legacy-findings-preserve`, без `--execute` — dry-run;
отказывается мигрировать не-King&Sons). Раскладка `v001`:

```text
versions/v001/
  01_input/legacy_bundle/<rel>            # все исходники как есть (структура версий сохранена)
  03_analysis/latest/<name>               # 03_findings/01/02/pipeline_log/norm_checks/optimization — если есть
  03_analysis/runs/run_legacy_preserve_<ts>/<rel>  # значимые .json/.jsonl из каждого _output
  04_review/kb_decisions_link.json        # только если есть связь с KB (база НЕ меняется)
  99_service/legacy_output/<rel>          # ПОЛНАЯ копия каждого legacy _output/ (вкл. бэкапы/png)
```

`version.json`: `legacy_partial` (ЭМ2, АК — есть findings) или `source_only`
(Фасадное, ИТП.ТМ — только исходники, без fake-файлов:
`preserve_reason=king_sons_source_only_legacy_bundle`).

| Документ | analysis_status | files | checksum | KB-link |
|---|---|---|---|---|
| EOM/133_23-ГК-ЭМ2 | legacy_partial | 156 | 93 | — |
| EOM/Фасадное освещение | source_only | 9 | 7 | — |
| ITP/133_23-ГК-ИТП.ТМ | source_only | 3 | 2 | — |
| SS/133_23-ГК-АК | legacy_partial | 389 | 142 | `04_review/kb_decisions_link.json` (4 записи) |

Итог: **557 файлов, 244 checksum-сверки, 0 ошибок**; `old_to_new_map.json` +
4 записи (`migration_kind=legacy_findings_preserve`, `v001`). `validate_migration.py`
для таких записей пропускает строгий quad (source-only допустим), checksum и
критичные артефакты проверяются как обычно. После миграции:
**ALREADY_MIGRATED = 184, MANUAL_REVIEW_REQUIRED = 0**, validate `[PASS]`.
Тесты — `tests/test_projects_v2_legacy_findings_preserve.py`.

### Этап 2.12 — финальная приёмка + metadata-only нормализация (ВЫПОЛНЕНО)

**Приёмка** (`scripts/projects_v2/generate_final_acceptance_report.py`, read-only):
184 документа, validate `[PASS]` (842 ok), drift 0, ALREADY_MIGRATED 184,
MANUAL_REVIEW_REQUIRED 0, WARNINGS_BLOCKED 0, 0 папок `obj_*`. Отчёт —
`projects_v2/_system/final_migration_acceptance_report.{json,md}`. Проверено
адверсариально (7/7 claims confirmed). Тесты —
`tests/test_projects_v2_final_acceptance.py`.

**Остаточный риск приёмки:** 160 `version.json` ранней схемы без `analysis_status`.

**Нормализация** (`scripts/projects_v2/normalize_version_metadata.py`,
metadata-only): классифицирует `analysis_status` строго по файлам в
`03_analysis/latest`, в `--execute` пишет ТОЛЬКО `version.json` (copy/delete нет,
legacy `projects/` не читается). Результат: проверено 218, без `analysis_status`
было 160, заполнено 160, у 8 добавлено только `missing_analysis_files`, 49
без изменений, 1 расхождение (`133_23-ГК-СОТ V1`: findings в KB, но без файлов в
`latest` → осознанно остаётся `legacy_partial`, не перезаписан). Распределение
после: complete 135, partial 33, none 45, legacy_partial 3, source_only 2 (= 218,
0 без статуса). validate `[PASS]` (842 ok), legacy `projects/` не изменена
(12744 файла), идемпотентно. Отчёт —
`projects_v2/_system/version_metadata_normalization_report.{json,csv}`. Тесты —
`tests/test_projects_v2_version_metadata_normalization.py`.

### Этап 3 — read-only backend adapter + parity (ВЫПОЛНЕНО, подготовка; НЕ подключён)

Подготовлен read-only слой чтения `projects_v2` без переключения production:

- [backend/app/services/storage/projects_v2_adapter.py](../backend/app/services/storage/projects_v2_adapter.py)
  — `ProjectsV2Adapter`: объекты/дисциплины/документы/версии, `document.json`/
  `version.json`, `01_input`, `03_analysis/latest` (01/02/03), findings с тем же
  приоритетом, что и legacy (`03a_norms_verified > 03_findings > pre_merge`),
  `pipeline_log` из latest/99_service/runs. **Только чтение**: ни записи, ни
  `mkdir`, ни fallback в legacy. Feature flag `AUDIT_STORAGE_BACKEND`
  (default `legacy`) — читается в самом модуле, в `core/config.py` не вносится;
  ни один production read-path его пока не проверяет → поведение не меняется.
- [scripts/projects_v2/check_backend_parity.py](../scripts/projects_v2/check_backend_parity.py)
  — сравнивает legacy ↔ v2 (v2 через adapter, legacy read-only) по выборке всех
  типов; главный инвариант — `findings_no_loss`; симметричный подсчёт findings.
  Отчёт `projects_v2/_system/backend_parity_report.{json,md,csv}`.

**Прогон parity:** проверено 16 документов (complete/partial/none/source_only/
legacy_partial/versioned/King&Sons), **16/16 passed**, findings v2/legacy
**607/607 (потери нет)**, `parity_ok=true`. Расхождение версий King&Sons
ЭМ2 (v2=1 / legacy-контейнер=2) помечено `expected_difference` (snapshot).

Тесты — `tests/test_projects_v2_backend_adapter.py` (включая
`test_adapter_writes_nothing`), `tests/test_projects_v2_backend_parity.py`
(включая обнаружение потери findings). Доп. файл `backend/app/services/storage/__init__.py`
(package marker, как у соседних сервис-пакетов).

### Этап 3.5 — read-only shadow API (ВЫПОЛНЕНО, выключено по умолчанию)

Adapter подключён к изолированному **read-only shadow API** для проверки чтения
`projects_v2` через backend, БЕЗ переключения основного UI/API:

- [backend/app/api/routers/projects_v2_shadow.py](../backend/app/api/routers/projects_v2_shadow.py)
  — endpoints `/api/projects-v2-shadow/{health,objects,documents,documents/{code},
  …/versions,…/snapshot,parity/sample}`, все только через `ProjectsV2Adapter`
  (read-only, без fallback). Gated флагом `AUDIT_PROJECTS_V2_SHADOW_API_ENABLED`
  (default `false` → каждый endpoint 404). Роутер всегда include-ится, но при
  выключенном флаге инертен → production/UI не меняется.
- Регистрация: один include в [backend/app/main.py](../backend/app/main.py)
  (import + `app.include_router(projects_v2_shadow.router)` с пояснением). Префикс
  `/api/projects-v2-shadow` не конфликтует с catch-all `/api/projects/{id:path}`
  (дефис, не слеш). `project_service` и прочие read-path НЕ изменены.
- [scripts/projects_v2/check_shadow_api.py](../scripts/projects_v2/check_shadow_api.py)
  — проверка без UI: HTTP против `--base-url` или `--in-process` (TestClient в
  отдельном процессе, без живого сервера и без рестарта). Отчёт
  `projects_v2/_system/shadow_api_check_report.{json,md}`.

**Прогон shadow check (in-process, реальный projects_v2):** 10/10 checks,
**12/12 документов**, ok=true, покрыты все типы (complete/partial/none/
source_only/legacy_partial/versioned/King&Sons preserve). Тесты —
`tests/test_projects_v2_shadow_api.py` (router disabled by default → 404; при
флаге работает; snapshot содержит analysis_status; source_only/legacy_partial не
падают; adapter read-only через API; production endpoint не зависит от флага).

Доп. изменён `backend/app/main.py` (регистрация роутера, gated) и `.gitignore`
(исключение для `check_shadow_api.py` — правило `check_*.py` ловит scratch).

**Controlled HTTP smoke (реальный сокет).**
[scripts/projects_v2/http_smoke_shadow_api.py](../scripts/projects_v2/http_smoke_shadow_api.py)
поднимает минимальный app (shadow + один legacy router) на эфемерном порту через
uvicorn — БЕЗ `backend.app.main` и без lifespan, поэтому не трогает общий state и
production (:8081). Прогон: **15/15 checks, 8/8 документов, ok=true** —
404 без флага, все shadow endpoints 200 с флагом, legacy `/api/objects` 200,
backend default legacy, read-only (54503 файла `objects/` без изменений), 404
после выключения. Production не перезапускался (тот же PID, `/api/info`→200).
Отчёт `projects_v2/_system/shadow_api_http_smoke_report.{json,md}`. Тесты —
`tests/test_projects_v2_shadow_http_smoke.py` (вкл. реальный uvicorn-сокет).

### Этап 3.6 — UI/API contract parity (ВЫПОЛНЕНО, read-only)

Read-only сверка контракта UI/API между legacy и `projects_v2`, БЕЗ переключения
backend:

- [scripts/projects_v2/check_ui_contract_parity.py](../scripts/projects_v2/check_ui_contract_parity.py)
  — по выборке всех типов сравнивает UI-поля (object/discipline/code,
  current_version, version_count, analysis_status, наличие 01/02/03,
  findings_count, severity, pipeline_log, blocks analysis, legacy/source_only
  флаги, KB-link для King&Sons). Классификация
  `MATCH/EXPECTED_DIFFERENCE/MISMATCH/MISSING_IN_V2/MISSING_IN_LEGACY`; подсчёт
  findings симметричный. Переиспользует helpers из `check_backend_parity.py`.
  Отчёт `projects_v2/_system/ui_contract_parity_report.{json,md,csv}`.
- Gated read-only endpoint `GET /api/projects-v2-shadow/ui-contract/sample`
  (default 404) — v2-сторона контракта по выборке, без legacy и без записи
  отчётов (добавлено в `backend/app/api/routers/projects_v2_shadow.py`).

**Прогон (реальный projects_v2):** 16 документов — **MATCH 12,
EXPECTED_DIFFERENCE 4, MISMATCH 0, MISSING 0**; field-level MATCH 216 / EXPECTED
11 / MISMATCH 0. **Потерь findings нет** (174/174, 67/67, 62/62, 21/21…),
**потерь версий нет**. EXPECTED_DIFFERENCE — это King&Sons preserve (snapshot
collapse, legacy_partial/source_only) и СОТ V1 (findings в KB). Тесты —
`tests/test_projects_v2_ui_contract_parity.py` (вкл. обнаружение потери
findings → MISMATCH).

Доп. изменён `backend/app/api/routers/projects_v2_shadow.py` (новый gated
endpoint) и `.gitignore` (исключение для `check_ui_contract_parity.py` — правило
`check_*.py`).

### Этап 3.7 — dual-read shadow + storage facade + cutover readiness (ВЫПОЛНЕНО, подготовка)

Объединённый подготовительный этап к будущему read-only cutover. Backend/UI не
переключаются, `AUDIT_STORAGE_BACKEND` остаётся `legacy`.

**Preflight gate (с refresh дрейфа):** drift scan нашёл 3 дрейфнувших документа
объекта 214 (`13АВ-РД-АР4.1-К6` v002, `13АВ-РД-АР1.2-К6` v001,
`13АВ-РД-СОУЭ-ПА V1` v001) — production live-переаудитил их после миграции. Все
`stable + refresh_safe` → выполнен `refresh_migrated_snapshot.py --execute
--include-new-files` для каждого (по 4 файла, со стабилизацией 120с, старые
версии в `_system/refresh_archive`). После refresh: **validate PASS (842 ok),
drift 0**.

**Реализовано:**
- [projects_v2_dual_read.py](../backend/app/services/storage/projects_v2_dual_read.py)
  — `DualReadService` (per-document legacy↔v2 сравнение, статусы
  match/expected_difference/mismatch/missing_legacy/missing_v2, read-only) +
  `cutover_readiness()` (рекомендация not_ready / ready_for_shadow_prod /
  ready_for_read_only_canary);
- [storage_read_facade.py](../backend/app/services/storage/storage_read_facade.py)
  — `StorageReadFacade` (legacy/projects_v2/dual_read_shadow, default legacy, не
  подключён к production; `production_uses_v2()`==False);
- [projects_v2_shadow.py](../backend/app/api/routers/projects_v2_shadow.py) —
  gated endpoints `/dual-read/sample`, `/dual-read/document/{code}`,
  `/cutover-readiness` (default 404);
- [check_cutover_readiness.py](../scripts/projects_v2/check_cutover_readiness.py)
  — CLI (validate subprocess + dual-read + reports);
- [http_smoke_shadow_api.py](../scripts/projects_v2/http_smoke_shadow_api.py)
  `--cutover` — ephemeral HTTP smoke dual-read/cutover endpoints.

**Прогон (реальный projects_v2):** cutover readiness = **ready_for_shadow_prod**
(validate PASS, drift 0, backend parity ok, UI contract ok 16 checked, dual-read
sample match 12 / expected 4, **0 mismatches, 0 потерь findings/versions**, 184
docs). HTTP smoke (ephemeral, без lifespan, не трогая prod :8081): **13/13
checks**, dual-read 404 без флага / 200 с флагом, backend default legacy,
read-only (54503 файла без изменений). Тесты —
`tests/test_projects_v2_dual_read.py`,
`tests/test_projects_v2_cutover_readiness.py`,
`tests/test_projects_v2_storage_read_facade.py` (полный projects_v2 suite: 242 passed).

**Порядок будущего cutover** (deploy flags off → enable shadow API → monitor
dual-read → read-only canary → rollback flag → full cutover) — см.
[docs/projects_v2_storage_standard.md](projects_v2_storage_standard.md).

Доп. изменён `backend/app/api/routers/projects_v2_shadow.py` (canary endpoints),
`scripts/projects_v2/http_smoke_shadow_api.py` (cutover smoke), `.gitignore`
(исключение для `check_cutover_readiness.py`).

### Этап 3.8 — production shadow rollout + full-corpus parity (parity ВЫПОЛНЕНА; deploy ЗАБЛОКИРОВАН)

Подготовка к production shadow rollout. **Cutover НЕ выполнялся**, backend/UI не
переключались, `AUDIT_STORAGE_BACKEND` остаётся `legacy`.

**Production deploy ЗАБЛОКИРОВАН (deploy НЕ выполнен, restart НЕ выполнен):**
production backend (PID 1537521, cwd `PDF-proverka-deploy`, ветка `deploy/main-live`
@ `b860f15`) НЕ содержит shadow-кода. Деплой не выполнен по двум блокерам:
1. **дивергенция веток** — `deploy/main-live` +117 коммитов, feature +40
   (merge-base `bfa0a2c`): нет чистого ff/merge;
2. **некоммиченные production hotfix'ы** в deploy worktree (9 modified +
   untracked, вкл. `start_server_deploy.sh`, `rate_limit_retry.py`,
   `md_resolver.py`) — git-deploy риск затереть их (правило «НЕ трогать
   некоммиченные hotfix'ы»).
→ Требуется командный deploy (коммит/согласование hotfix'ов, приведение веток
main→deploy/main-live), а не автономная операция.

**Подготовка к production-совместимости (выполнено в коде):**
`projects_v2_adapter._default_v2_root` теперь берёт `config.DATA_DIR/projects_v2`
(а не code-relative путь) — в production код в `-deploy`, данные через
`AUDIT_DATA_DIR=/…/PDF-proverka`, поэтому code-relative путь был бы неверным.

**Full-corpus parity (ВЕСЬ корпус 184 документа, read-only, в WIP):**
`check_ui_contract_parity.py --all` → отчёт `full_corpus_parity_report.{json,md,csv}`.
Результат: **doc_status MATCH 179 / EXPECTED_DIFFERENCE 5 / MISMATCH 0 / MISSING 0**;
field-level MISMATCH 0; **findings_loss 0, version_loss 0**, contract_ok=True. 5
EXPECTED_DIFFERENCE — только разрешённые: 4 King&Sons preserve (ЭМ2/АК/Фасадное/
ИТП.ТМ) + 1 legacy_partial (СОТ V1).

**Cutover readiness:** теперь читает full-corpus отчёт → **`ready_for_read_only_canary`**
(validate PASS 842 ok, drift 0, backend parity ok, ui_contract 184 checked,
dual-read ok, 0 mismatches).

**Shadow endpoints** проверены на ephemeral uvicorn (production не трогали):
base smoke 15/15, cutover/dual-read smoke 13/13, default→404, backend default legacy.

Тесты: полный projects_v2 suite **246 passed**. Отчёты —
`production_shadow_rollout_report.{json,md}`, `full_corpus_parity_report.{json,md,csv}`,
`cutover_readiness_report.{json,md}`.

### Этап 3.9 — robust legacy matcher в parity (tooling-fix, 2026-06-16) ✅

`check_ui_contract_parity.py` давал **ложные** `MISSING_IN_LEGACY` (а на
versioned-доках — `MISMATCH`) из-за хрупкого сопоставления legacy↔v2:

- `projects_root` брался из `v2lib.legacy_projects_root()` = `repo_root()/projects`
  (code-relative). В worktree-деплое это НЕ совпадает с реальными данными
  (`AUDIT_DATA_DIR/projects`), поэтому `Path(legacy_project_path).relative_to(projects_root)`
  падал → `legacy_object/discipline` = null → весь корпус как `MISSING_IN_LEGACY`;
- `document_code` сравнивался с именем legacy-папки as-is: `13АВ-РД-ВК1-К2.pdf`
  (хвостовой `.pdf`) и `133_23-ГК-АР2 V2` (версионный суффикс) не нормализовались
  → `MISMATCH`, хотя `old_to_new_map` авторитетно связывает их по `document_code`.

**Исправлено:**

1. `projects_root` по умолчанию = **sibling** `--v2-root/../projects`; добавлен
   флаг `--legacy-root` (override). Больше не зависит от code-relative пути.
2. Robust-резолвер `_resolve_legacy` — приоритет источников:
   `old_to_new_map` (object_name/discipline/legacy_folder_name/legacy_folder_path,
   document_code — авторитетная связь) → `document.json.legacy_project_path` →
   вывод object/discipline из пути → скан `projects_root/<obj>/<disc>` по
   нормализованному имени. Существование папки проверяется на диске.
3. Новая категория **`EXPECTED_NAMING_DIFFERENCE`** (не блокирует cutover, как
   `EXPECTED_DIFFERENCE`): legacy-папка существует и связана с v2, но её имя
   отличается от чистого `document_code` лишь `.pdf` / `(main)` / ` V{N}`.

**Результат full-corpus (184 дока):** `MATCH 117 / EXPECTED_DIFFERENCE 2 /
EXPECTED_NAMING_DIFFERENCE 65 / MISSING_IN_LEGACY 0 / MISSING_IN_V2 0 / MISMATCH 0`;
`findings_loss 0`, `version_loss 0`, `missing_in_legacy_real []`, `contract_ok=True`
(184 ложных доко-флага устранены). Реальные потери ловятся по-прежнему:
`findings_loss` (v2 < legacy), `version_loss` (version_count MISMATCH вне King&Sons),
genuinely missing legacy → `MISSING_IN_LEGACY` + `missing_in_legacy_real`.

#### Known naming artifacts (почему `.pdf` в legacy project_id — НЕ потеря данных)

Часть legacy-папок исторически названа с артефактами, которых нет в чистом v2
`document_code`:

| Артефакт | Пример legacy-папки | v2 document_code | Почему норма |
|---|---|---|---|
| хвостовой `.pdf` | `13АВ-РД-ВК1-К2.pdf` | `13АВ-РД-ВК1-К2` | имя папки оканчивается на `.pdf` (legacy-конвенция); содержимое то же |
| контейнер `(main)` | `133_23-ГК-ЭМ1(main)` | `133_23-ГК-ЭМ1` | версионный контейнер; `document_code_for` берёт `logical_project_id` |
| версионный суффикс | `133_23-ГК-АР2 V2` | `133_23-ГК-АР2` | legacy хранит версии sibling-папками `<base> V{N}`; v2 — под `versions/` |

Это артефакты ИМЕНОВАНИЯ, а не данных: `old_to_new_map` связывает каждую такую
папку с её v2 `document_code`, findings/versions сохранены (0 loss), а UI/adapter
резолвят документ по basename. Parity помечает их `EXPECTED_NAMING_DIFFERENCE` и
НЕ блокирует cutover. Полное устранение рассинхрона имён — отдельная необязательная
нормализация (не потеря).

**Как parity matcher должен сопоставлять legacy ↔ v2 (контракт):**
по `old_to_new_map` (приоритет, авторитетная связь по `document_code`), затем по
`legacy_project_path`/`legacy_folder_name`, затем по нормализованному basename
(`document_code_for` снимает `.pdf`/`(main)`; ` V{N}` снимается дополнительно), с
проверкой существования папки. MATCH/EXPECTED при совпадении кода и сохранности
findings/versions; `EXPECTED_NAMING_DIFFERENCE` для имени-артефакта; `MISMATCH`/
`MISSING_IN_LEGACY` — только при реальной потере или отсутствии.

Тесты: `tests/test_projects_v2_ui_contract_parity.py` (+8 кейсов: `.pdf`/`(main)`/
` V{N}` → не false-positive; `old_to_new_map` priority; legacy_folder_name matching;
реальные missing/findings_loss/version_loss всё ещё ловятся).

### Этап 4 — подключение adapter за флагом к основным read-path (план, НЕ в этом PR)

- Подключить adapter к реальным read-path (project list / findings / pipeline
  summary / versions) за `AUDIT_STORAGE_BACKEND`, двойное чтение (v2 +
  логирование расхождений) на shadow-объекте.
- Прогон тестов backend на shadow-объекте до включения флага в prod.
- Backend restart / deploy — **только** после явного подтверждения (см. правила
  проекта).

### Этап 5 — cutover (план)

- Включить `STORAGE_BACKEND=v2` для одного объекта, наблюдать.
- Постепенно расширить на все объекты.
- legacy `projects/` сохранить как архив до полной верификации.

## Открытые вопросы (решить до этапа 3)

1. **Запись результатов.** Куда новый pipeline пишет результаты — в
   `03_analysis/runs/<new_run_id>/` с обновлением `latest/`? Нужна политика
   run_id и ретеншена прогонов.
2. **02_work и PDF.** Нужен ли backend нормализованный `document.pdf` в `02_work`
   (сейчас копируется) или достаточно `01_input`?
3. **Version-awareness decisions_log / expert_review.** Сейчас не version-aware
   (см. память проекта). При v2 ключевать по `document_code + version_id`.
4. **Связь с comparison/.** Линк или перенос. Рекомендация — линк.
5. **Классификация 04/05/99.** Уточнить правила (сейчас эвристика; runs/ —
   источник истины).
6. **Имена с спецсимволами.** `document_code` с кириллицей/кавычками/скобками —
   подтвердить переносимость (linux ext4 — ок; для FAT/архивов нужна транслитерация).
7. **Дисковый объём.** verbatim run-копия + классиф-копии дублируют JSON; блоки
   копируются один раз (только в runs). Оценить итоговый объём перед массовой
   миграцией; рассмотреть hardlink/симлинки для крупных блоков.

## Откат

Удалить папку `projects_v2/` (она целиком производна). legacy не затрагивается,
поэтому откат тривиален и безопасен.

## Команды

```bash
# Инвентарь (read-only):
python scripts/projects_v2/inventory_legacy_projects.py

# Readiness report (read-only, после inventory):
python scripts/projects_v2/report_migration_readiness.py

# Миграция одного проекта или (main)-контейнера:
python scripts/projects_v2/migrate_one_project_to_v2.py "projects/<obj>/<disc>/<project>"

# Проверка:
python scripts/projects_v2/validate_migration.py [--document <document_code>]

# Тесты:
python -m pytest tests/test_projects_v2.py tests/test_projects_v2_readiness.py -q
```
