# Версионность проектов — контейнерная раскладка `(main)/`

**Дата:** 2026-05-30

Один логический проект может иметь несколько версий документации (V1, V2, V3…).
Версии хранятся **братскими папками внутри папки-контейнера** `<база>(main)/`.

```
projects/<объект>/<дисциплина>/
    133_23-ГК-ЭМ1(main)/            ← контейнер (НЕ проект)
        version_group.json          ← манифест: порядок, метки, какая версия primary/latest
        133_23-ГК-ЭМ1/            ← V1 (basename сохранён → project_id не меняется)
            project_info.json, *.pdf, *.md, _output/
        133_23-ГК-ЭМ1 V2/        ← V2 (своя папка, свой _output)
        133_23-ГК-ЭМ1 V3/        ← V3 …
```

## Ключевой инвариант: стабильный `project_id`

`project_id` = **basename** папки версии (не полный путь). У V1 это исходное имя
проекта; при промоуте в контейнер папка V1 перемещается под тем же именем,
поэтому `project_id` **не меняется**. Все персистентные хранилища ключуются по
basename + `object_id`-хэшу, поэтому при версионизации **ничего не
переписывается**: замечания (`decisions_log`), обсуждения, реестр заказчика,
учёт стоимости остаются валидными.

## Логический слой `version_id` сохранён

Физическая раскладка изменилась, но абстракция версий — нет. Весь pipeline / API
/ UI работают через `version_id` (`v1`, `v2`…), `versions_summary`, инъекцию
`?version_id=` и `bind_version()`. Меняется только отображение
`version_id → папка на диске` (через `version_group.json` в контейнере).

Раздел сравнения хранит собственные source-only версии документов внутри
`projects_v2/objects/<object>/comparison/stage_1|stage_2`; эта раскладка
независима от групп версий обычного audit-pipeline.

## `version_group.json` (манифест контейнера)

```json
{
  "schema_version": 1,
  "logical_project_id": "133_23-ГК-ЭМ1",
  "container": "133_23-ГК-ЭМ1(main)",
  "primary_version_id": "v1",
  "latest_version_id": "v2",
  "versions": [
    {"version_id": "v1", "version_no": 1, "label": "V1", "folder": "133_23-ГК-ЭМ1"},
    {"version_id": "v2", "version_no": 2, "label": "V2", "folder": "133_23-ГК-ЭМ1 V2"}
  ]
}
```

`folder` — имя братской папки относительно контейнера. У V1 это исходный
basename проекта.

## Promote-on-first-version

Первая загруженная папка автоматически = **V1**. Когда в UI помечаешь проект как
версию базового (или жмёшь «создать версию»), `version_service.create_next_version`:

1. создаёт контейнер `<база>(main)/`;
2. **перемещает** папку V1 внутрь под её именем (`shutil.move`);
3. создаёт братскую папку следующей версии (`<база> V{N}` по умолчанию или имя
   source-папки при merge);
4. пишет `version_group.json`;
5. сбрасывает кеш списка проектов.

`merge_project_as_version(source, target)` переносит файлы source-проекта в новую
братскую папку версии target (имя папки = имя source-папки) и удаляет source.
`_output/` source НЕ копируется (версия начинается с нуля); guard
`SourceOutputNotEmptyError` защищает от молчаливой потери готового аудита.

## Discovery / resolve

- `iter_project_dirs` распознаёт контейнер `(main)` и выдаёт **ровно один**
  проект на контейнер — primary-версию (V1) с её basename как `project_id`.
  Контейнер и старшие версии в список проектов не попадают (видны через
  `versions_summary`).
- `resolve_project_dir` ищет и по контейнерному пути `<parent>/<база>(main)/<база>`
  (в т.ч. для `project_id` со слешем, напр. `KJ/TGT2`).
- `resolve_active_project_dir` / `bind_version` дают папку активной версии.

## Реализация

- [backend/app/services/common/version_service.py](../backend/app/services/common/version_service.py)
  — `container_name_for`, `is_version_container`, `container_dir_for`,
  `promote_to_container`, `create_next_version`, `merge_project_as_version`,
  `create_version_from_existing_files`, `version_group.json` I/O + legacy fallback.
- [backend/app/services/common/project_service.py](../backend/app/services/common/project_service.py)
  — `iter_project_dirs` / `resolve_project_dir` (контейнер-aware).
- Legacy `_versions/v{N}` читается для обратной совместимости до миграции.

## Миграция legacy `_versions/v{N}` → `(main)/`

Старые проекты на `_versions/v{N}/` мигрируются одноразовым скриптом:

```bash
# Показать план (ничего не меняет):
python backend/scripts/migrate_versions_to_container.py --dry-run
# Реальный прогон:
python backend/scripts/migrate_versions_to_container.py
```

Скрипт идемпотентен (уже мигрированные контейнеры пропускаются), пишет лог
перемещений и сохраняет basename папок версий (project_id не меняется).

> ⚠️ У части legacy-проектов имя папки оканчивается на `.pdf`
> (`13АВ-РД-КЖ6-К1К2 (1).pdf`) → контейнер получит имя
> `…(1).pdf(main)`. По умолчанию basename не нормализуется. Перед реальным
> прогоном сверьте план `--dry-run`.

## Тесты

- [tests/test_version_service.py](../tests/test_version_service.py),
  [tests/test_merge_project_as_version.py](../tests/test_merge_project_as_version.py),
  [tests/test_version_file_upload.py](../tests/test_version_file_upload.py),
  [tests/test_project_version_from_candidate.py](../tests/test_project_version_from_candidate.py),
  [tests/test_pipeline_write_path_versioned.py](../tests/test_pipeline_write_path_versioned.py),
  [tests/test_migrate_versions_to_container.py](../tests/test_migrate_versions_to_container.py).

> Примечание: `tests/conftest.py` выключает портальную аутентификацию
> (`PORTAL_AUTH_ENABLED=false`) для TestClient-тестов, иначе prod-`.env`
> отдаёт 401 на все API-запросы без логина.
