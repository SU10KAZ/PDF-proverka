# projects_v2 — стандарт новой структуры хранения

**Дата:** 2026-06-15
**Статус:** ЭТАП 1 (подготовка). Параллельная папка `projects_v2/`. **Backend/UI
НЕ подключены.** `projects/` и `comparison/` не изменяются.

Этот документ — нормативное описание целевой раскладки. Инструменты этапа 1
(inventory / migrate / validate) реализуют её в read-only режиме к legacy.

## Зачем

Текущая `projects/` смешивает вход и производные файлы, держит легаси-форматы и
backup-папки рядом с боевыми данными, а версии кодирует папкой `<имя>(main)/` с
братскими каталогами (включая каталоги с `.pdf` в имени). См. аудит структуры.
`projects_v2` разделяет данные по назначению и вводит строгую индексацию версий.

## Дерево

```text
projects_v2/
  _system/
    schema.json               # машиночитаемое описание стандарта (этот layout)
    migration_inventory.json  # инвентарь legacy (read-only снимок)
    migration_inventory.csv
    old_to_new_map.json       # карта old_path -> new_path + checksum по версиям

  objects/
    <readable_object_folder>/            # напр. 214_Alia_ASTERUS (object_id — в object.json, НЕ в имени)
      object.json                        # object_id + display_name + folder_name + legacy_path
      disciplines/
        <discipline_code>/               # EOM, AR, OV, VK, SS, KJ, KM, GP, TX, ...
          documents/
            <document_code>/             # СТАБИЛЬНАЯ папка документа (раздела)
              document.json              # список версий, current_version, kind, legacy
              current_version.txt        # текущий version_id (vNNN)
              versions/
                v001/
                  version.json           # метаданные версии + legacy-имя папки
                  01_input/              # НЕИЗМЕНЯЕМЫЙ входной комплект
                    <base>.pdf
                    <base>_document.md
                    <base>_ocr.html
                    <base>_result.json
                    project_info.json
                    input_manifest.json  # legacy-имена + sha входного комплекта
                  02_work/               # нормализованные рабочие копии
                    document.pdf
                    document.md
                    ocr.html
                    result.json
                  03_analysis/
                    runs/<run_id>/       # VERBATIM копия legacy _output (lossless)
                    latest/              # ключевые артефакты (01/02/03_findings, ...)
                  04_review/             # critic/corrector/review артефакты
                  05_export/             # отчёты, excel, csv
                  99_service/            # логи, pipeline_log, бэкапы, intermediate
                v002/
                  ...
              comparisons/               # ЭТАП 2 (пока не заполняется)
                v001_vs_v002/
                  comparison_link.json   # ссылка на сессию/пару сравнения
```

## Имена папок объектов (человекочитаемые)

Папка объекта в `projects_v2/objects/` **обязана** быть человекочитаемой.
Технический `object_id` хранится только внутри `object.json`, **не** в имени
папки. Использовать `obj_<hash>` как имя папки **запрещено** (допустимо лишь как
legacy/runtime до переименования скриптом `rename_object_folders.py`).

Правильные имена:

```text
objects/213_Mosfilmovskaya_31A_KingSons/
objects/214_Alia_ASTERUS/
```

Имя строит `make_object_folder_name(display_name, object_id)`:

- ведущий номер объекта остаётся первым сегментом (`214. …` → `214_…`);
- кириллица транслитерируется в латиницу (`Мосфильмовская` → `Mosfilmovskaya`);
- `&` удаляется (`King&Sons` → `KingSons`);
- пробелы/точки/кавычки/скобки/слэши → `_`; повторные `_` схлопываются;
- ведущие/хвостовые `_` убираются;
- при конфликте имён добавляется суффикс `_<object_id>` (только при конфликте).

`object.json` хранит и id, и человекочитаемые поля:

```json
{
  "object_id": "73a0e59a",
  "display_name": "214. Alia (ASTERUS)",
  "folder_name": "214_Alia_ASTERUS",
  "legacy_path": ".../projects/214. Alia (ASTERUS)"
}
```

Резолв пути к объекту (`resolve_object_folder`) идёт по читаемому имени, затем
по `object_id` из `object.json`, затем по legacy `obj_<id>` — поэтому код не
ломается ни до, ни после переименования.

## Уровни сущностей

| Уровень | Что это | Ключ |
|---|---|---|
| **Объект** | Стройка/заказчик («214. Alia») | `object_id` (objects.json) |
| **Дисциплина** | Раздел проектирования (EOM, AR…) | `discipline_code` |
| **Документ** | Один логический раздел документации | `document_code` (стабильный) |
| **Версия** | Конкретная загрузка/стадия документа | `version_id` = `vNNN` |

`document_code`:
- обычный проект → имя папки без хвостового `.pdf`;
- контейнер `<base>(main)` → `logical_project_id` из `version_group.json`.

## Входной комплект (01_input) — главное правило

В систему загружается комплект из **4 файлов** (по суффиксам имени, базовое имя
произвольно):

| Роль | Суффикс legacy | 01_input | 02_work (норм.) |
|---|---|---|---|
| PDF | `.pdf` | оригинальное имя | `document.pdf` |
| Markdown | `_document.md` | оригинальное имя | `document.md` |
| OCR HTML | `_ocr.html` | оригинальное имя | `ocr.html` |
| OCR result | `_result.json` | оригинальное имя | `result.json` |

Плюс `project_info.json` (конфиг) — тоже в `01_input`.

**Инварианты:**
- `01_input` **неизменяем** — это источник истины загрузки. Backend пишет только
  в `02_work/` и дальше.
- `02_work/` — нормализованные копии со стабильными именами, чтобы код не зависел
  от произвольного базового имени.
- `input_manifest.json` хранит legacy-имена и sha256 каждого файла комплекта.

## Версии — строгая индексация

- Формат: `vNNN` (zero-padded): `v001`, `v002`, `v003`.
- Обычный проект (без контейнера) → одна версия `v001`.
- Контейнер `(main)` → `version_no` из `version_group.json` маппится в `vNNN`.
- **Legacy gotcha:** папка вида `13АВ-РД-ЭО-К3 V2.pdf/` (каталог с `.pdf` в имени)
  становится `versions/v002/`. Оригинальное имя сохраняется в `version.json`
  (`legacy_folder_name`) и `input_manifest.json`.
- `current_version.txt` + `document.json.current_version` = `latest_version_id`
  из манифеста (иначе максимальный `version_no`).

## 03_analysis — без потерь

- `03_analysis/runs/<run_id>/` — **verbatim-копия** всего legacy `_output`
  (включая блоки, intermediate, бэкапы). Это **источник истины** — здесь ничего
  не теряется.
- `03_analysis/latest/` — копии ключевых артефактов прогона:
  `01_text_analysis.json`, `02_blocks_analysis.json`, `03_findings.json`,
  `document_graph.json`, `norm_checks.json`, `optimization.json`.
- `04_review/` / `05_export/` / `99_service/` — **эвристически** разложенные
  копии артефактов верхнего уровня `_output` (удобство, не источник истины).

`run_id` детерминирован: `run_<YYYYMMDDThhmmss>` по mtime `pipeline_log.json`.

### Live-дописанный анализ: `run_refresh_<timestamp>` + `analysis_status`

Если backend проводит аудит документа уже ПОСЛЕ миграции, в legacy `_output/`
появляются новые analysis-файлы, которых нет в snapshot (drift-тип
`legacy_new_file_not_in_map`). Их добавляют контролируемо
(`refresh_migrated_snapshot.py --include-new-files`, только whitelist, со
stability-check):

- новые файлы кладутся в **отдельный** `03_analysis/runs/run_refresh_<timestamp>/`
  (verbatim), чтобы не смешивать старый snapshot с live-дописанным анализом;
- критичные/основные analysis-файлы (`01_text_analysis.json`,
  `02_blocks_analysis.json`, `03_findings.json`, `norm_checks.json`,
  `03a_norms_verified.json`, `optimization.json`) дублируются в
  `03_analysis/latest/`;
- в `version.json` проставляется состояние анализа:

```json
{
  "analysis_status": "none | partial | complete",
  "analysis_refreshed_at": "...",
  "analysis_refresh_reason": "legacy_new_analysis_artifacts"
}
```

`complete` — есть все три `01/02/03`; `partial` — часть; `none` — ни одного.
Отсутствие анализа — НЕ ошибка: входные данные мигрируются всегда, анализ
помечается явным статусом.

### `_ocr.html` — optional

`_ocr.html` считается **опциональным** входным файлом. Если есть
`.pdf + _document.md + _result.json`, отсутствие `_ocr.html` не блокирует
перенос: `02_work/ocr.html` не создаётся, а в `input_manifest.json`
фиксируется `missing_optional_files: ["ocr_html"]`.

### Документ всегда контейнер версий

В projects_v2 каждый документ имеет `documents/<code>/versions/vNNN/`, даже если
в legacy не было `(main)`-контейнера. Папки legacy вида `... V1.pdf/ / V2.pdf/`
без `(main)`/`version_group.json`: одиночная → один document с `v001` (старое имя
в metadata); несколько однозначно связанных → объединить в `versions/v001..v00N`;
неоднозначные → manual review (не объединять автоматически).

### `analysis_status`, `missing_analysis_files`, legacy-generation

`version.json` фиксирует состояние анализа явно (отсутствие/неполнота — НЕ ошибка):

```json
{
  "analysis_status": "none | partial | complete | legacy_partial",
  "missing_analysis_files": ["02_blocks_analysis.json", "norm_checks.json"],
  "analysis_generation": "legacy",                 // только для legacy-снимков
  "preserve_reason": "legacy_algorithm_with_kb_findings"
}
```

- `none` — анализа нет, перенесён только вход;
- `partial` — часть analysis-файлов (перечислены отсутствующие в `missing_analysis_files`);
- `complete` — есть `01+02+03`;
- `legacy_partial` — проект ранних алгоритмов (напр. King&Sons): полного набора
  файлов нового pipeline нет, но есть ценные данные (`03_findings.json` и/или
  старый анализ, связь с `knowledge_base/decisions_log.json`, экспертные
  решения) — переносится как legacy-снимок, ничего не теряя. Отсутствие новых
  файлов для таких проектов НЕ повод блокировать миграцию.

> `analysis_status` и `missing_analysis_files` пишутся `migrate_version` при
> КАЖДОЙ миграции (вычисляются из перенесённых `01/02/03` в `03_analysis/latest`);
> `legacy_partial` + `analysis_generation`/`preserve_reason` ставятся, только
> когда миграция запущена с соответствующей policy (`LEGACY_KB_PRESERVE`).
> `input_manifest.json.missing_optional_files` тоже пишется всегда (сейчас —
> `["ocr_html"]`, если OCR-HTML отсутствует).

### legacy-bundle снимок (King&Sons blocked/manual)

Для проектов ранних алгоритмов с блокерами (несколько PDF/MD/result, неполный
комплект), где НЕ нужно добиваться идеальной новой структуры, применяется
снимок «как есть» (`POLICY_READY_LEGACY_FINDINGS_PRESERVE`): primary PDF не
выбирается, ничего не теряется.

```text
versions/v001/
  01_input/legacy_bundle/     # ВСЕ pdf/md/ocr/result/прочее как есть
  03_analysis/latest/         # 03_findings.json / 01 / 02 / pipeline_log если есть
  99_service/legacy_output/   # полная копия legacy _output/
```

`version.json`: `analysis_status=legacy_partial`, `analysis_generation=legacy`,
`preserve_reason=king_sons_legacy_findings_preserve`,
`source_files_strategy=legacy_bundle`,
`primary_goal=preserve_findings_and_kb_links`.

Главный приоритет — сохранить найденные замечания (`03_findings.json` и др.) и
связь с `knowledge_base/decisions_log.json`. Неоднозначные/непонятные файлы тоже
кладутся в `legacy_bundle` (zero data loss).

## Сравнение версий (этап 2, зарезервировано)

`documents/<document_code>/comparisons/<vA>_vs_<vB>/comparison_link.json` будет
ссылаться на существующее дерево `comparison/sessions/<sid>/pairs/<pid>/`
(сравнение НЕ дублируется, только связывается). На этапе 1 не заполняется.

## Будущий storage adapter + feature flag (НЕ включать на этапе 1)

Когда структура будет валидирована на полном объёме, планируется тонкий
адаптер путей в backend (НЕ реализуется в этом этапе):

- `StorageAdapter.version_dir(object_id, discipline, document_code, version_id)`
  → путь активной версии в `projects_v2`;
- `input_path(...)` → `02_work/` (нормализованные копии, не `01_input`);
- `analysis_path(...)` → `03_analysis/latest/`.
- Переключение через feature flag, например `STORAGE_BACKEND=v2` (default `legacy`),
  с двойным чтением (read v2, fallback legacy) на переходный период.

Адаптер и флаг на этом этапе **только спроектированы**, в код backend не
вносятся. См. `docs/projects_v2_migration_plan.md`.

## Инварианты безопасности

1. legacy `projects/` и `comparison/` — **только чтение**, никогда не изменяются.
2. Все записи только в `projects_v2/`.
3. Миграция идемпотентна; verbatim run-копия гарантирует отсутствие потерь.
4. backend/UI к `projects_v2` на этапе 1 **не подключены**.

## Связанные файлы

- `scripts/projects_v2/v2lib.py` — общая библиотека.
- `scripts/projects_v2/inventory_legacy_projects.py` — read-only инвентарь.
- `scripts/projects_v2/migrate_one_project_to_v2.py` — миграция одного проекта.
- `scripts/projects_v2/validate_migration.py` — проверка миграции.
- `tests/test_projects_v2.py` — тесты.
- `docs/projects_v2_migration_plan.md` — план миграции.
