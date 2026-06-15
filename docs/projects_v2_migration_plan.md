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

### Этап 3 — storage adapter + feature flag (план, НЕ в этом PR)

- Тонкий `StorageAdapter` в backend, отдающий пути `projects_v2` для версии.
- Feature flag `STORAGE_BACKEND=legacy|v2` (default `legacy`).
- Переходный режим: чтение из v2 с fallback на legacy.
- Прогон тестов backend на shadow-объекте до включения флага в prod.
- Backend restart / deploy — **только** после явного подтверждения (см. правила
  проекта).

### Этап 4 — cutover (план)

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
