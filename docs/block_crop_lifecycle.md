# Жизненный цикл кропов блоков: дедупликация → восстановление → эвакуация

Кропы блоков (PNG в `blocks_stage02_100` / `blocks_gemma_100` / `blocks_gemma_300` /
`blocks`) были крупнейшей устранимой статьёй на диске: **12.2 ГБ / 64 764 файла**
при заполнении 98%. Механизм ниже убирает дубли, делает пропажу кропов громкой и
позволяет держать локально только то, что реально нужно.

## Эмпирика, на которой построен дизайн (замеры 2026-08-03)

| Что | Значение |
|---|---|
| Дубль `blocks_gemma_100` ↔ `blocks_stage02_100` | **16564 из 16564 файлов байт-идентичны** (политики рендера совпадают, различается только строка `profile`) |
| Дубль `blocks` ↔ `blocks_stage02_100` | 79 папок идентичны, 87 — единственные (алиас там ПЕРВИЧЕН), 9 расходятся |
| Живость `crop_url` | **33/39 (85%)**; возраст смерть не предсказывает (мёртвая на 32 днях, живая на 110) |
| Локальный ре-рендер vs облачный кроп | тот же размер (ровно `render_size`), **99.52% пикселей в пределах 6% яркости** |
| Скорость | ре-рендер 0.07 с (100 DPI) / 0.22 с (300 DPI); скачивание ~0.6 с |
| Вектор-кроп PDF vs PNG | **PDF в 5× БОЛЬШЕ** — хранить PDF вместо PNG нельзя |
| Где лежит index блоков | у **183 из 440 версий — ТОЛЬКО в `runs/`**, то есть run-папка и обслуживает UI |

## Почему восстановление local-first, а не из облака

В `backend/app/services/common/crop_cache.py:3-6` зафиксировано решение от
13–14.07.2026: crop-токены живут per-generation, при пере-генерации документа на
портале меняются ВСЕ ссылки, поэтому «качать лениво когда понадобится» нельзя.
Замер подтвердил: 15% ссылок уже мертвы.

Зато `02_work/document.pdf` не протухает, а координаты блока сохраняются в
sidecar. Порядок: **локальный ре-рендер → `crop_url` → отказ** (env
`BLOCK_CROP_RESTORE_ORDER`).

## Этап 0 — дедупликация (сделано, −3.6 ГБ)

```bash
python scripts/projects_v2/dedupe_block_crops.py scan --json /tmp/dedupe.json
python scripts/projects_v2/dedupe_block_crops.py apply --confirm DEDUPE_BLOCK_CROPS
```

Ничего не удаляется — доказанно идентичные файлы (размер + sha256) заменяются
жёсткими ссылками. `index.json` НИКОГДА не связывается: он различается между
папками. `sync_v2_read_canary_blocks_alias` (`crop_blocks/runner.py`) теперь тоже
строит алиас жёсткими ссылками, а не `copytree`.

**Жёсткие ссылки, а не симлинки:** `backup_version_before_destructive` делает
`copytree(..., symlinks=True)` — симлинк уехал бы в бэкап и мог бы указывать на
живые данные другой версии.

**Предусловие дедупа:** запись PNG сделана атомарной
(`crop_blocks/blocks.py::_save_pixmap_atomic`, tmp + `os.replace`). Без этого
`pix.save()` писал бы по общему inode и пере-кроп одной папки молча переписал бы
кроп соседней. Расширение временного файла обязано совпадать с целевым —
PyMuPDF определяет формат по суффиксу.

## Этап 1 — пропажа кропов стала громкой

`block_context/contract.py::crops_materialized(blocks_dir)` проверяет наличие и
пригодность (≥1 КБ) КАЖДОГО файла из `index.json`. Подключён в четыре точки
готовности: `manager._ensure_stage02_crops`, `manager._precrop_project`,
`prepare_service`, `resume_detector`, плюс жёсткий отказ в
`gemma_findings_only.check_prerequisites`.

Раньше «index есть, PNG нет» проходило как «кропы готовы» — и это состояние
достижимо БЕЗ эвакуации: resume засевает run-папку одним `index.json`.

Попутно починено: `codex_runner._normalize_image_paths` больше не выбрасывает
недоступные картинки молча; `block_file_for()` вернул авторитетность полю `file`
(галерея пишет `.webp`, а несколько мест хардкодили `block_{id}.png`);
`discussion_service` и `prompt_builder` перестали хардкодить папку `blocks`;
`ProjectsV2Adapter.blocks_dir()` выбирает run-папку по mtime, как и
`_fallback_run_dir` (раньше лексикографически — победители расходились у 53%
версий с несколькими прогонами); в `/api/info` добавлен блок `disk`.

## Этап 2 — восстановление по требованию

* `services/common/block_crop_store.py` — единая точка. Две входные функции:
  * `hydrate_blocks_dir(...)` — массово, **обратно в исходную папку**, поэтому
    весь код пайплайна, читающий `blocks_dir / block["file"]`, правок не требует;
  * `resolve_block_image(...)` — одиночный блок через LRU-кэш (UI, обсуждения, агенты).
* Лестница: локальный файл → LRU → ре-рендер из PDF → `crop_url` → `None` + WARNING.
* Политика рендера берётся ИЗ ПАПКИ (`index.json` → sidecar → карта по имени),
  поэтому `blocks_gemma_300` восстанавливается в 300 DPI, а `stage02` — в 100.
* `services/common/block_crop_lru.py` — кэш вне деревьев проектов, потолок
  `BLOCK_CROP_CACHE_MAX_BYTES` (1.5 ГБ), вытеснение по mtime (`get()` делает
  `os.utime` — на `relatime` atime заморожен и LRU выродился бы в FIFO), запись
  моложе `BLOCK_CROP_CACHE_MIN_AGE_S` не вытесняется, межпроцессный `flock`.
* Sidecar `crops_evicted.json` рядом с `index.json`. **`index.json` не трогаем
  никогда:** `blocks_index_hash()` хеширует его целиком, а
  `validate_gemma_enrichment_summary` роняет гейт при расхождении.
  В sidecar лежит `page_px` — размеры страницы из `result.json`, которых НЕТ в
  index, а без них `crop_from_pdf` не пересчитает координаты в точки PDF.
* Асинхронность: восстановление в запросе — только через `asyncio.to_thread`.
* `GZipMiddleware` больше не жмёт PNG/WebP уровнем 9 в event loop
  (`_ImageSafeGZipMiddleware` в `main.py`): для уже сжатых картинок это нулевая
  экономия и чистая нагрузка, а вкладка «Блоки» тянет их десятками.

## Этап 3 — эвакуация

Хук `PipelineManager._maybe_evict_block_crops` вызывается из `_run_batch_queue`
после `_shadow_mirror_completed_audit` — это единая точка завершения любого
action (full/resume/retry/optimization). Условия: `COMPLETED` ∧ не `is_running`
∧ по этой версии нет `pending/running/interrupted` ∧ в `pipeline_log` нет стадии
`running` ∧ папка не `latest`. Полностью fail-soft.

Ретро-скрипт:

```bash
python scripts/projects_v2/evict_block_crops.py scan   --json /tmp/evict.json
python scripts/projects_v2/evict_block_crops.py plan   --report /tmp/evict.json
python scripts/projects_v2/evict_block_crops.py verify --report /tmp/evict.json --sample 3
python scripts/projects_v2/evict_block_crops.py apply  --report /tmp/evict.json \
    --confirm EVICT_BLOCK_CROPS_RUNS_ONLY --max-bytes 1G
python scripts/projects_v2/evict_block_crops.py purge --older-than-days 14 --apply
```

Защищено безусловно: живой путь чтения (`ProjectsV2Adapter.resolved_blocks_dirs()`,
пересечение прерывает ВЕСЬ запуск), `03_analysis/latest`, идущие аудиты, свежие
папки, блоки с `promoted_to_full` / `compact` / соседом `_full.png` / без
`crop_px` / без размеров страницы / без локального PDF. Перед удалением каждый
блок проходит контрольный ре-рендер. Удаление — переносом в `.evicted/`, реальное
стирание отдельной командой `purge`.

Версии с вердиктами эксперта по умолчанию пропускаются (`04_review`); опт-аут —
`--allow-versions-with-verdicts` (на живом дереве это разница 0.10 ГБ против 3.43 ГБ).
Решением Андрея Ивановича от 2026-08-03 опт-аут применён: вердикты ссылаются на
замечания из `latest`, а кропы ИСТОРИЧЕСКИХ прогонов их доказательной базой не
являются.

Две тонкости, найденные на первом боевом прогоне:

* **Zero-overlap guard пересчитывается в `apply`, а не берётся из отчёта**:
  между `scan` и `apply` мог пройти новый прогон, и папка-кандидат могла стать
  живым путём чтения. Штатный пропуск защищённых папок при сканировании (их 628)
  прерыванием не считается.
* **`verify` пишет временный файл ВНЕ проверяемой папки** и свежесть считается
  по mtime `index.json`, а не папки. Иначе проверка (по смыслу читающая) сдвигает
  mtime продовой папки и сама же роняет эвристику «папку недавно трогали».
* **`resolved_blocks_dirs()` обязан повторять ПОВЕДЕНИЕ читателя, а не его
  «лидера».** `blocks_dir()` идёт по прогонам по порядку и берёт ПЕРВЫЙ с
  `index.json`; прогон-лидер вполне может индекса не иметь (`run_refresh_*`, где
  кроп ещё не делался), и читатель проваливается на следующий. Первая версия
  защиты проверяла только двух лидеров, возвращала пустой набор — и 4 папки,
  которые реально обслуживали UI, ушли под эвакуацию. Починено (проход до
  первого прогона с индексом в каждом из двух порядков), файлы возвращены на
  место, добавлен регресс-тест
  `test_resolved_blocks_dirs_mirrors_reader_fallthrough`.

## Платный кэш Stage 02 — переведён на идентичность (schema v2)

Раньше `stage02_paid_cache.compute_cache_key` подмешивал в ключ БАЙТЫ PNG.
Восстановленный кроп байт-идентичным не бывает (замер: 1818 КБ против 1770 КБ у
одного блока; плюс облачные кропы имеют поле ~16 px, которого нет у локальной
нарезки), поэтому каждый восстановленный блок давал бы гарантированный промах —
то есть повторную оплату ровно в том сценарии, ради которого кэш заводили
(инцидент 2026-05-16, один блок оплачен 9–15 раз).

Теперь в ключ идёт `build_image_identity(block, index_top)`:
`block_id | page | file | dpi | min_long_side | compact | render_size | crop_px`.
Ре-рендер того же блока из того же PDF даёт ту же строку. Если координат нет
(старые индексы, ~9% записей) — честный маркер `nocoords` плюс `size_kb`.

`CACHE_SCHEMA_VERSION` поднят 1 → 2: `try_load_cached` сравнивает версию и на
несовпадении возвращает `None`, поэтому старые записи инвалидируются чисто, а не
отдают ответ, посчитанный по другому правилу.

## Флаги (`backend/app/core/config.py`, все default OFF/консервативно)

`BLOCK_CROP_RESTORE_ENABLED`, `BLOCK_CROP_RESTORE_ALLOW_NETWORK`,
`BLOCK_CROP_RESTORE_ORDER`, `BLOCK_CROP_RESTORE_CONCURRENCY`,
`BLOCK_CROP_RESTORE_BUDGET_S`, `BLOCK_CROP_RESTORE_TIMEOUT_S`,
`BLOCK_CROP_CACHE_DIR`, `BLOCK_CROP_CACHE_MAX_BYTES`,
`BLOCK_CROP_CACHE_MAX_FILE_BYTES`, `BLOCK_CROP_CACHE_MIN_FREE_BYTES`,
`BLOCK_CROP_CACHE_MIN_AGE_S`, `BLOCK_CROP_CACHE_SWEEP_EVERY`,
`BLOCK_CROP_EVICTION_ENABLED`, `BLOCK_CROP_EVICTION_DRY_RUN`,
`BLOCK_CROP_EVICT_LATEST`.

Порядок включения строго **RESTORE → EVICTION**. Даже включённый
`BLOCK_CROP_EVICTION_ENABLED` при `DRY_RUN=true` (default) ничего не удалит.

## Тесты

`tests/test_block_crop_store.py` (24) — sidecar, эвакуация, лестница
восстановления, LRU, анти-traversal, политика по папке.
`tests/test_crop_rehydration_gate.py` (8) — гейт готовности и fail-soft хука.
