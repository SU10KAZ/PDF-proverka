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
  `02_text_analysis.json`, `01_blocks_analysis.json`, `03_findings.json`,
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
- критичные/основные analysis-файлы (`02_text_analysis.json`,
  `01_blocks_analysis.json`, `03_findings.json`, `norm_checks.json`,
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
  "missing_analysis_files": ["01_blocks_analysis.json", "norm_checks.json"],
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

- `source_only` — legacy-снимок, в котором сохранены ТОЛЬКО исходники, без
  анализа (напр. один PDF). `preserve_reason=king_sons_source_only_legacy_bundle`.

> `analysis_status` и `missing_analysis_files` пишутся `migrate_version` при
> КАЖДОЙ миграции (вычисляются из перенесённых `01/02/03` в `03_analysis/latest`);
> `legacy_partial` + `analysis_generation`/`preserve_reason` ставятся, только
> когда миграция запущена с соответствующей policy (`LEGACY_KB_PRESERVE`).
> `input_manifest.json.missing_optional_files` тоже пишется всегда (сейчас —
> `["ocr_html"]`, если OCR-HTML отсутствует).

#### Metadata-only нормализация (`normalize_version_metadata.py`)

`version.json` ранних прогонов миграции мог не содержать `analysis_status`
(старая схема). Перед read-only storage adapter metadata приводится к единому
виду инструментом
[scripts/projects_v2/normalize_version_metadata.py](../scripts/projects_v2/normalize_version_metadata.py):

- проходит ВСЕ `version.json`, классифицирует `analysis_status` строго по
  фактически существующим файлам в `03_analysis/latest`
  (`complete/partial/none`; для legacy-снимков `legacy_partial`, если есть
  analysis-файлы, иначе `source_only`);
- `--dry-run` (default) ничего не меняет; `--execute` пишет ТОЛЬКО `version.json`
  (никаких копий/удалений/правок analysis-артефактов; legacy `projects/` не
  читается);
- по умолчанию **заполняет только отсутствующий** `analysis_status`
  (+ `missing_analysis_files`) и не перезаписывает уже выставленный статус —
  расхождение «existing≠proposed» лишь репортится (напр. документ с findings в
  KB, но без файлов в `latest`, осознанно остаётся `legacy_partial`).
  `--correct-existing` (default OFF) форсит перерасчёт существующих;
- идемпотентно; отчёт —
  `projects_v2/_system/version_metadata_normalization_report.{json,csv}`.

### legacy-bundle снимок (King&Sons blocked/manual)

Для проектов ранних алгоритмов с блокерами (несколько PDF/MD/result, неполный
комплект), где НЕ нужно добиваться идеальной новой структуры, применяется
снимок «как есть» (`POLICY_READY_LEGACY_FINDINGS_PRESERVE`): primary PDF не
выбирается, ничего не теряется.

```text
versions/v001/
  01_input/legacy_bundle/<rel>            # ВСЕ pdf/md/ocr/result/прочее как есть (структура версий сохранена)
  03_analysis/latest/<name>               # 03_findings.json / 01 / 02 / pipeline_log / norm_checks / optimization если есть
  03_analysis/runs/run_legacy_preserve_<ts>/<rel>   # значимые .json/.jsonl из каждого _output
  04_review/kb_decisions_link.json        # только если есть связь с KB (база НЕ меняется)
  99_service/legacy_output/<rel>          # полная копия КАЖДОГО legacy _output/ (вкл. бэкапы/png)
```

`version.json` — два варианта:

- есть findings/анализ → `analysis_status=legacy_partial`,
  `preserve_reason=king_sons_legacy_findings_preserve`,
  `primary_goal=preserve_findings_and_kb_links`;
- только исходники (напр. один PDF, без анализа) → `analysis_status=source_only`,
  `preserve_reason=king_sons_source_only_legacy_bundle` (БЕЗ fake-файлов).

Общее: `analysis_generation=legacy`, `source_files_strategy=legacy_bundle`,
`migration_kind=legacy_findings_preserve`.

Главный приоритет — сохранить найденные замечания (`03_findings.json` и др.) и
связь с `knowledge_base/decisions_log.json`. Неоднозначные/непонятные файлы тоже
кладутся в `legacy_bundle` (zero data loss). KB-связь — это отдельный
metadata-файл `04_review/kb_decisions_link.json` (`source_project` + список
записей); сама база знаний не изменяется.

Инструмент — `scripts/projects_v2/migrate_legacy_findings_preserve.py`
(обязательный флаг `--legacy-findings-preserve`, dry-run без `--execute`,
отказывается мигрировать не-King&Sons; checksum по каждой копии). `validate_migration.py`
для записей `migration_kind=legacy_findings_preserve` пропускает строгий
input-quad (source-only допустим), но checksum/legacy-неизменность/критичные
артефакты проверяет как обычно.

## Сравнение версий (этап 2, зарезервировано)

`documents/<document_code>/comparisons/<vA>_vs_<vB>/comparison_link.json` будет
ссылаться на существующее дерево `comparison/sessions/<sid>/pairs/<pid>/`
(сравнение НЕ дублируется, только связывается). На этапе 1 не заполняется.

## Read-only backend adapter (подготовлен, НЕ подключён)

[backend/app/services/storage/projects_v2_adapter.py](../backend/app/services/storage/projects_v2_adapter.py)
— тонкий **read-only** слой чтения `projects_v2`. Подготовительный этап: код
есть, но production его НЕ вызывает (никакой read-path backend на него не
переключён), поэтому поведение системы не меняется.

`ProjectsV2Adapter` умеет читать:

- список объектов / дисциплин / документов (`list_objects`,
  `list_disciplines`, `list_documents`, `find_document`);
- `document.json` + версии + `current_version` (`get_document`, `list_versions`,
  `current_version_id`);
- `version.json` и metadata (`version_metadata`: `analysis_status`,
  `missing_analysis_files`, `migration_kind`, `preserve_reason`, флаги
  `is_legacy_preserve / is_source_only / is_legacy_partial`);
- входные файлы версии (`input_files` — `01_input`, включая `legacy_bundle`);
- `03_analysis/latest` артефакты (`latest_analysis_files`, `read_text_analysis`,
  `read_blocks_analysis`);
- замечания с тем же приоритетом, что и legacy findings_service
  (`03a_norms_verified.json > 03_findings.json > 03_findings_pre_merge.json`):
  `findings_path / read_findings / findings_count / findings_by_severity`;
- `pipeline_log.json` из нескольких возможных мест (`03_analysis/latest/` для
  legacy-снимков, `99_service/` и `03_analysis/runs/<run>/` для обычных
  миграций): `pipeline_log_path / has_pipeline_log / read_pipeline_log`;
- сводный `document_snapshot(...)`.

### Почему adapter ничего не пишет

Это слой ЧТЕНИЯ для подготовки cutover. Любая запись (создание/удаление файлов,
`mkdir`, правка metadata, запуск анализа) запрещена by design — adapter не
содержит таких операций. Это гарантирует, что включение/тестирование адаптера
не может повредить ни `projects_v2`, ни legacy. Тест
`test_adapter_writes_nothing` фиксирует инвариант (байт/множество файлов до и
после полного прогона read-поверхности идентичны).

### Никакого fallback в legacy

При чтении v2 adapter **не** обращается к legacy `projects/`. Если чего-то нет в
`projects_v2` — возвращается `None`/пусто, а не «подмешивание» старого
хранилища. Это нужно, чтобы parity-проверка и будущий cutover работали с
честным состоянием v2, а не маскировали пробелы старыми данными.

### Feature flag `AUDIT_STORAGE_BACKEND`

- значения: `legacy` (default) | `projects_v2`;
- читается `get_storage_backend()` / `is_v2_backend_enabled()` в самом модуле
  адаптера (env), в `core/config.py` НЕ выносится, чтобы не трогать production;
- **default остаётся `legacy`**, и пока ни один production read-path этот флаг
  не проверяет → поведение backend/UI идентично сборке без адаптера. Флаг —
  только подготовка к будущему переключению.

### Parity report (как читать)

[scripts/projects_v2/check_backend_parity.py](../scripts/projects_v2/check_backend_parity.py)
сравнивает legacy ↔ v2 по выборке документов всех типов (complete / partial /
none / source_only / legacy_partial / versioned / King&Sons preserve). v2
читается ТОЛЬКО через adapter, legacy — напрямую (read-only «сверка»). Отчёт —
`projects_v2/_system/backend_parity_report.{json,md,csv}`:

- `parity_ok` — нет hard-расхождений;
- per-doc checks: `document_exists`, `version_count`, `current_version`,
  `analysis_status_present`, `findings_no_loss` (ГЛАВНЫЙ инвариант),
  `findings_exact_match`, `artifacts_01_02_03_parity`, `pipeline_log_present`;
- подсчёт findings **симметричный** (один приоритет файла и в legacy `_output`,
  и в v2 `latest`), поэтому числа сопоставимы;
- расхождение числа версий у King&Sons legacy-preserve (v2=1 vs legacy-контейнер
  с несколькими папками) помечается `expected_difference` и НЕ роняет parity.

### Предусловия будущего cutover

Перед переключением `AUDIT_STORAGE_BACKEND=projects_v2` в проде:

1. `validate_migration.py` — `[PASS]`; drift scan — 0; readiness —
   `ALREADY_MIGRATED=N, MANUAL_REVIEW_REQUIRED=0`.
2. `check_backend_parity.py` — `parity_ok=true`, `findings_no_loss=true` на
   широкой выборке (в идеале на всех документах).
3. Подключить adapter к реальным read-path (project list, findings, pipeline
   summary, versions) за флагом + двойное чтение (v2 с логированием расхождений)
   на shadow-объекте.
4. Прогон backend-тестов и ручная проверка UI на shadow-объекте под флагом.
5. Только потом — переключение флага в проде (с быстрым откатом в `legacy`).

## Read-only shadow API (подготовлен, выключен по умолчанию)

[backend/app/api/routers/projects_v2_shadow.py](../backend/app/api/routers/projects_v2_shadow.py)
— тонкий **read-only** REST-слой над `ProjectsV2Adapter` для проверки чтения
`projects_v2` через backend, БЕЗ подключения к основному UI/API. Это НЕ cutover:
основной backend по-прежнему работает с legacy `projects/`.

### Endpoints (префикс `/api/projects-v2-shadow`)

| Endpoint | Назначение |
|---|---|
| `GET /health` | статус adapter, `object_count`, `document_count`, `read_only`, default backend |
| `GET /objects` | список объектов |
| `GET /documents` | список документов (фильтры `object_folder`/`discipline`/`analysis_status`/`limit`) |
| `GET /documents/{code}` | document.json + summary |
| `GET /documents/{code}/versions` | версии + metadata (`analysis_status`, legacy-preserve флаги) |
| `GET /documents/{code}/snapshot` | полный снимок (версии, статусы, findings_count, pipeline_log) |
| `GET /parity/sample` | существующий parity-отчёт (read-only, не пересчитывает) |

### Почему выключен по умолчанию

Флаг `AUDIT_PROJECTS_V2_SHADOW_API_ENABLED` (default `false`). При выключенном
флаге КАЖДЫЙ endpoint возвращает **404** (как будто роутера нет). Флаг читается
на каждый запрос. Роутер всегда `include`-ится в app, но при `false` инертен →
production/UI поведение идентично сборке без него (проверено: существующие
endpoint'ы работают одинаково при любом значении флага). Это безопасный rollout:
включение не трогает основной поток.

### Read-only

Все endpoint'ы читают только через `ProjectsV2Adapter` (без записи/создания/
удаления, без fallback в legacy). Сам shadow-роутер ничего не пишет в
`projects_v2`. Тест `test_adapter_read_only_via_api` фиксирует, что прогон всех
endpoint'ов не меняет файлы `projects_v2`.

### Как включить локально (НЕ в проде)

```bash
AUDIT_PROJECTS_V2_SHADOW_API_ENABLED=true \
  uvicorn backend.app.main:app --port 8082   # отдельный порт, не трогая prod :8081
curl -s localhost:8082/api/projects-v2-shadow/health | jq
```

### Как проверить endpoints без живого сервера

[scripts/projects_v2/check_shadow_api.py](../scripts/projects_v2/check_shadow_api.py)
— `--in-process` поднимает TestClient в отдельном процессе (флаг ON, auth OFF,
`startup`/lifespan НЕ запускается → нет побочных эффектов на prod), гоняет health
+ снимки документов всех типов (complete/partial/none/source_only/legacy_partial/
versioned/King&Sons), сверяет с parity и пишет
`projects_v2/_system/shadow_api_check_report.{json,md}`. Режим по умолчанию —
HTTP против `--base-url` (для локального backend с включённым флагом).

### Controlled HTTP smoke (реальный сокет, без рестарта prod)

[scripts/projects_v2/http_smoke_shadow_api.py](../scripts/projects_v2/http_smoke_shadow_api.py)
— поднимает **минимальный** app (только shadow-router + один read-only legacy
router) на ЭФЕМЕРНОМ порту через uvicorn и бьёт по реальному HTTP-сокету. Важно:
он НЕ импортирует `backend.app.main` и НЕ запускает его `lifespan`
(`cleanup_zombies` / `load_persisted_queue` / recover-stale) — поэтому никаких
побочных эффектов на общий state и на production backend (тот живёт отдельно на
:8081 и не трогается). Флаг читается на запрос → один сервер, тумблер env между
фазами.

```bash
python scripts/projects_v2/http_smoke_shadow_api.py   # порт 0 (эфемерный), НЕ 8081
```

Проверяет за один прогон: (1) без флага `/health`→404; (2) с флагом
health/objects/documents/snapshot/parity→200; (3) legacy `/api/objects`→200 и без
флага, и с флагом; (4) `storage_backend_default=legacy`; (5) read-only
(snapshot `objects/` до/после идентичен); (6) тумблер обратно→404. Отчёт —
`projects_v2/_system/shadow_api_http_smoke_report.{json,md}`.

### Почему это не cutover / условия следующего этапа

Shadow API только ЧИТАЕТ v2 в изолированном namespace — основной backend его не
использует. Следующий этап (НЕ здесь): подключить adapter к реальным read-path
за `AUDIT_STORAGE_BACKEND` с двойным чтением на shadow-объекте, прогнать
backend-тесты и UI, и лишь затем — переключение флага в проде (см. «Предусловия
будущего cutover» выше).

## UI/API contract parity (read-only, НЕ cutover)

[scripts/projects_v2/check_ui_contract_parity.py](../scripts/projects_v2/check_ui_contract_parity.py)
сравнивает то, что отдаёт legacy `projects/`, с тем, что СМОЖЕТ отдать
`projects_v2` (через `ProjectsV2Adapter`), по полям, важным для UI/API. v2
читается только через adapter, legacy — напрямую (read-only сверка). Backend НЕ
переключается, `AUDIT_STORAGE_BACKEND` остаётся `legacy`. Отчёт —
`projects_v2/_system/ui_contract_parity_report.{json,md,csv}`.

**Режим `--all` (весь корпус):** по умолчанию проверяется выборка по типам; флаг
`--all` сверяет ВЕСЬ корпус (все документы), отчёт пишется в
`full_corpus_parity_report.{json,md,csv}`. `cutover_readiness` предпочитает
полнокорпусный отчёт (если есть) → при зелёном полном прогоне рекомендация может
дорасти до `ready_for_read_only_canary`. Полный прогон 184 документа: MATCH 179 /
EXPECTED_DIFFERENCE 5 (только King&Sons + СОТ V1) / MISMATCH 0, findings/version
loss 0.

> **production code/data split:** adapter `_default_v2_root` использует
> `config.DATA_DIR/projects_v2` (а не путь к файлу модуля). В production код живёт
> в `…-deploy`, данные — в основном репо через `AUDIT_DATA_DIR`, поэтому
> code-relative путь указывал бы на несуществующий `…-deploy/projects_v2`.
> Переопределение — env `AUDIT_PROJECTS_V2_DIR`.

### Какие поля сравниваются

`object_display_name`, `discipline`, `document_code`, `current_version_no`,
`version_count`, `analysis_status`, наличие `02_text_analysis.json` /
`01_blocks_analysis.json` / `03_findings.json`, `findings_count`,
`findings_by_severity` (soft), `pipeline_log_present` + `pipeline_log_stage_count`
(soft), `has_blocks_analysis`, флаги `v2_legacy_preserve` / `source_only`, и
`kb_link_entry_count` (только для King&Sons legacy preserve).

### Классификация (на поле и на документ)

`MATCH` | `EXPECTED_DIFFERENCE` | `EXPECTED_NAMING_DIFFERENCE` | `MISMATCH` |
`MISSING_IN_V2` | `MISSING_IN_LEGACY`. Подсчёт findings — **симметричный** (один
приоритет файла `03a_norms_verified > 03_findings` и в legacy `_output`, и в v2
`latest`). `EXPECTED_NAMING_DIFFERENCE` не блокирует cutover (как `EXPECTED_DIFFERENCE`).

### Как сопоставляется legacy ↔ v2 (robust matcher, 2026-06-16)

Legacy-папка резолвится по приоритету: **`old_to_new_map`** (авторитетная связь по
`document_code` + `object_name`/`discipline`/`legacy_folder_name`/`legacy_folder_path`)
→ `document.json.legacy_project_path` → вывод object/discipline из пути относительно
`projects_root` → скан `projects_root/<obj>/<disc>` по нормализованному имени
(`document_code_for` снимает `.pdf`/`(main)`, дополнительно снимается ` V{N}`).
`projects_root` по умолчанию = sibling `<v2-root>/../projects` (НЕ code-relative),
переопределяется `--legacy-root`. Существование папки проверяется на диске; битый
`legacy_project_path` без записи в map и без папки → честный `MISSING_IN_LEGACY`
(не фабрикуется ложный `MISMATCH`).

### Какие расхождения считаются EXPECTED

- **King&Sons legacy preserve** — v2 намеренно хранит legacy snapshot:
  `version_count` / `current_version` отличаются (коллапс в один `v001`),
  `analysis_status` = `legacy_partial` / `source_only`, флаги legacy/source_only —
  v2-only;
- **source_only / проекты без анализа** — отсутствие `01/02/03` нормально с обеих
  сторон (→ `MATCH`); `analysis_status` v2-специфичный (`legacy_partial` для
  документов, чьи findings лежат в KB, а не файлами) → `EXPECTED_DIFFERENCE`;
- **иной формат legacy version container** при равном нормализованном
  `version_count` → `MATCH`; расхождение числа версий допускается только для
  King&Sons;
- **naming artifacts** (`EXPECTED_NAMING_DIFFERENCE`) — legacy-папка существует и
  связана с v2, но её имя отличается от чистого `document_code` лишь:
  хвостовым `.pdf` (`13АВ-РД-ВК1-К2.pdf`), контейнером `(main)`
  (`133_23-ГК-ЭМ1(main)`) или версионным суффиксом (`133_23-ГК-АР2 V2`). Это
  артефакт ИМЕНОВАНИЯ, НЕ потеря данных: `old_to_new_map` связывает папку с v2
  `document_code`, findings/versions сохранены, UI/adapter резолвят по basename.
  Полная нормализация имён — отдельный необязательный этап.

### Что блокирует будущий cutover

- `MISMATCH` по `findings_count` (потеря/искажение замечаний);
- `MISMATCH` по `version_count` вне King&Sons (потеря версий);
- `MISMATCH` по `analysis_status` для обычных документов;
- `MISMATCH` по наличию `01/02/03` или `object/discipline/code`;
- `MISSING_IN_V2` для KB-link у King&Sons (потеря связи с базой знаний).

### Почему это ещё не cutover

Сравнение только читает обе стороны и пишет отчёт. Реальные read-path backend
по-прежнему используют legacy; переключение произойдёт отдельным этапом после
`contract_ok=true` на широкой выборке (в идеале на всех 184 документах) и
двойного чтения на shadow-объекте.

### Shadow endpoint (gated, read-only)

`GET /api/projects-v2-shadow/ui-contract/sample` (default 404 без флага) отдаёт
**только v2-сторону** UI-контракта по выборке типов (без legacy, без записи
отчётов) — быстрый просмотр того, что v2 сможет отдать. Полную legacy↔v2 сверку
делает CLI.

## Dual-read shadow + storage read facade + cutover readiness (подготовка, НЕ cutover)

Подготовительный слой для будущего read-only cutover. Ничего не переключает:
`AUDIT_STORAGE_BACKEND` остаётся `legacy`, production read-path не подключён.

### Dual-read service

[backend/app/services/storage/projects_v2_dual_read.py](../backend/app/services/storage/projects_v2_dual_read.py)
— `DualReadService`: для документа собирает legacy-snapshot (из `projects/` через
`old_to_new_map`, read-only) и v2-snapshot (через adapter), сравнивает поля
(object/discipline/code, versions, current version, analysis_status, 01/02/03,
findings_count, severity, pipeline_log, blocks analysis, KB-link King&Sons) и
возвращает статус: `match | expected_difference | mismatch | missing_legacy |
missing_v2`. Только чтение, без fallback из v2 в legacy, без записи. `sample()`
прогоняет выборку типов.

### Storage read facade

[backend/app/services/storage/storage_read_facade.py](../backend/app/services/storage/storage_read_facade.py)
— `StorageReadFacade` инкапсулирует выбор backend:
`legacy` (default) | `projects_v2` | `dual_read_shadow` (env `AUDIT_STORAGE_BACKEND`).
**Default всегда `legacy`**; в legacy-режиме фасад — no-op (обслуживание остаётся
за существующими сервисами, v2 не читается). Фасад НЕ подключён к production
endpoints — это подготовленный класс. `production_uses_v2()` всегда возвращает
`False` на этом этапе.

### Canary/cutover endpoints (gated, read-only)

В shadow-роутере (default 404 без флага):
- `GET /api/projects-v2-shadow/dual-read/sample`
- `GET /api/projects-v2-shadow/dual-read/document/{document_code}`
- `GET /api/projects-v2-shadow/cutover-readiness`

Ничего не пишут в `projects_v2`.

### Cutover readiness

[scripts/projects_v2/check_cutover_readiness.py](../scripts/projects_v2/check_cutover_readiness.py)
+ `projects_v2_dual_read.cutover_readiness()` (единая логика для CLI и endpoint).
Собирает validate (CLI — subprocess; endpoint — из последнего отчёта), drift,
backend parity, UI contract parity, live dual-read sample → рекомендация:

- `not_ready` — hard-проблема (validate FAIL / drift>0 / mismatch / потеря
  findings|versions) ИЛИ validate/drift не определены;
- `ready_for_shadow_prod` — базово зелено, но contract parity покрыт по ВЫБОРКЕ
  (не по всему корпусу) → можно включить shadow API в prod для наблюдения;
- `ready_for_read_only_canary` — всё зелено И contract parity по ВСЕМУ корпусу И
  без потерь → read-only канарейка.

Отчёт `projects_v2/_system/cutover_readiness_report.{json,md}`.

### Почему default остаётся legacy

Ни один production read-path не читает `projects_v2`. Включение
`AUDIT_STORAGE_BACKEND=projects_v2` сейчас лишь подготавливает фасад, но не
меняет существующие endpoints (они продолжают читать legacy). Это гарантирует
нулевой риск для production до отдельного, явно авторизованного этапа подключения.

### Порядок будущего production cutover

1. **deploy flags off** — выкатить код с `AUDIT_STORAGE_BACKEND=legacy` и shadow
   API выключенным (поведение не меняется);
2. **enable shadow API** — `AUDIT_PROJECTS_V2_SHADOW_API_ENABLED=true` на проде,
   наблюдать `/cutover-readiness` и `/dual-read/*`;
3. **monitor dual-read** — гонять dual-read по всему корпусу, добиться
   `mismatch=0`, отсутствия потерь, `ready_for_read_only_canary`;
4. **read-only canary** — подключить v2-чтение к части read-path за флагом на
   shadow-объекте, сверять с legacy;
5. **rollback flag** — при любом расхождении мгновенно вернуть
   `AUDIT_STORAGE_BACKEND=legacy` (откат без миграции данных);
6. **full cutover** — после стабильной канарейки переключить чтение на
   `projects_v2`, legacy оставить архивом.

## Контракт записи (write facade, Step 8/10 — подготовка)

Модуль [backend/app/services/storage/storage_write_facade.py](../backend/app/services/storage/storage_write_facade.py)
— фасад записи данных проекта в `projects_v2`. **По умолчанию (`legacy`) ничего
не пишет в v2.** С Step 9/10 фасад **подключён** к write-chokepoints (см. ниже),
но это no-op в режиме legacy.

**Подключение (Step 9/10).** Safe-обёртки `shadow_mirror_project_path_safe` /
`shadow_mirror_project_id_safe` вызываются ПОСЛЕ успешной legacy-записи в:
`project_service.register_external_project / register_project / save_project_info`,
`version_service.save_files_to_version / create_next_version`,
`knowledge_base_service.save_expert_review`, и в `pipeline/manager` на завершении
аудита. В режиме legacy обёртки выходят немедленно (no-op, без импорта v2lib и
резолва путей); в `dual_write_shadow` — зеркалят проект через проверенную
`v2lib.migrate_project` (идемпотентно, обновляет `old_to_new_map`). Каждый хук
обёрнут в `try/except` → legacy байт-идентичен даже при сбое импорта хука.
Canary-валидация: `projects_v2/_system/dual_write_canary_report.{json,md}`.

**Режим записи — env `AUDIT_PROJECTS_V2_WRITE_MODE` (default `legacy`):**

| Режим | Поведение |
|---|---|
| `legacy` (default, prod) | фасад — no-op для v2; авторитетна только legacy-запись |
| `dual_write_shadow` | legacy ПЕРВОЙ (авторитетна) → затем v2-тень; сбой v2 fail-soft, не ломает legacy |
| `projects_v2_primary` | v2 primary → затем legacy как архив (будущее) |

Значение читается из env на КАЖДЫЙ вызов; неизвестное/пустое → `legacy`
(fail-safe: непонятный конфиг никогда не включает запись в v2).

**Инварианты записи:**
1. production default = `legacy`;
2. в `dual_write_shadow` v2-запись происходит ТОЛЬКО после успешной legacy;
3. сбой v2-записи в shadow логируется и НЕ ломает legacy (никакой silent loss);
4. деструктивные операции в v2 (`clean_project_data`, `delete_pair(hard)`, rmtree
   версии) **заблокированы** (`DestructiveWriteBlocked`) до появления контракта
   backup + явного подтверждения (Step 9/10);
5. имена входных файлов/артефактов санируются до basename (нет выхода за пределы
   `01_input` / `03_analysis`).

**Безопасные методы (реализованы):** `save_version_metadata` (`version.json` +
каркас документа), `save_input_bundle` (`01_input/`), `save_analysis_artifact`
(`03_analysis/latest/` + опц. `runs/<run>/`). Dry-run симулятор:
[scripts/projects_v2/simulate_write_cutover.py](../scripts/projects_v2/simulate_write_cutover.py).

Карта всех путей записи backend (где что пишется, какие chokepoint'ы заводить
через фасад) — runtime-отчёт `projects_v2/_system/write_path_audit_report.{json,md}`
+ Этап 6 в [projects_v2_migration_plan.md](projects_v2_migration_plan.md).

## Инварианты безопасности

1. legacy `projects/` и `comparison/` — **только чтение**, никогда не изменяются.
2. Все записи только в `projects_v2/`.
3. Миграция идемпотентна; verbatim run-копия гарантирует отсутствие потерь.
4. backend/UI к `projects_v2` на этапе 1 **не подключены**.
5. Запись в `projects_v2` управляется `AUDIT_PROJECTS_V2_WRITE_MODE` (default
   `legacy` → запись выключена); см. «Контракт записи» выше.

## Связанные файлы

- `scripts/projects_v2/v2lib.py` — общая библиотека.
- `scripts/projects_v2/inventory_legacy_projects.py` — read-only инвентарь.
- `scripts/projects_v2/migrate_one_project_to_v2.py` — миграция одного проекта.
- `scripts/projects_v2/validate_migration.py` — проверка миграции.
- `tests/test_projects_v2.py` — тесты.
- `docs/projects_v2_migration_plan.md` — план миграции.
