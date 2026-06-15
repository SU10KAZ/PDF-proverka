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
