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
    obj_<object_id>/                     # object_id из backend/app/data/objects.json
      object.json                        # метаданные объекта + legacy-имя/путь
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
