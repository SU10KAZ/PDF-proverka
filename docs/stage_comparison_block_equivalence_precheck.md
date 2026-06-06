# Stage Comparison — pre-Qwen block equivalence gate (Stage 1: observe)

**Дата:** 2026-06-06
**Статус:** Stage 1 — **observe only**, по умолчанию **ВЫКЛЮЧЕНО** (один флаг).
НИЧЕГО не пропускает мимо Qwen. Safe-skip — отдельный второй этап.
**Модуль:** [backend/app/services/stage_comparison/block_equivalence_precheck.py](../backend/app/services/stage_comparison/block_equivalence_precheck.py)

## Зачем

Текущий pipeline сравнения стадий гонит ВСЕ image/imagine/text-блоки листа
через Qwen (дорого, медленно), даже если между OLD и NEW версией блок не
изменился. Прекчек ДО Qwen определяет, какие блоки идентичны, чтобы в будущем
(Stage 2) безопасно пропускать Qwen для неизменённых блоков.

На первом этапе (этот PR) — только **наблюдение**: строится отчёт + diagnostics,
Qwen-конвейер не меняется. Это даёт статистику «сколько блоков реально можно
было бы пропустить» без риска что-то скрыть.

## Конвейер

```text
result.json OLD (left/старая стадия) + NEW (right/новая стадия)
  → extract_blocks_for_equivalence()      # нормализованные блоки + OCR-текст
  → pair_blocks_by_iou()                   # сопоставление по coords_norm/IoU
      ├─ detect_split_merge_candidates()   # один↔много → split_merge_uncertain
      ├─ one-to-one уверенный → paired
      ├─ unmatched old → deleted_candidate
      └─ unmatched new → added_candidate
  → per paired block:
      compare_text_blocks()                # строгое canonical-равенство текста
      compare_visual_blocks()              # ECC-выравнивание (cv2) + diff-метрики
  → decide_block_pair()                    # decision + qwen_action
  → build_block_equivalence_report()       # отчёт + summary
  → артефакт block_equivalence/block_equivalence_report.json
     + debug PNG block_equivalence/debug/{block_id}_diff.png (для changed_visual)
```

`left` = OLD (старая стадия), `right` = NEW (новая стадия).

## Pairing

- координаты — `coords_norm` (0..1), устойчиво к разным DPI рендера OLD/NEW;
- сопоставление ВНУТРИ пары страниц: карта `(old_page, new_page)` берётся из
  `page_alignment.json` (находит уехавшие листы); без карты — identity по номеру;
- учитывается тип блока (text/image/table); смена типа text↔image = change;
- основной матч — жадный one-to-one по IoU ≥ `IOU_THRESHOLD`;
- **split/merge**: один блок перекрывает ≥2 блока другой стороны (IoU ≥
  `OVERLAP_THRESHOLD`) → `split_merge_uncertain` (НЕ skip, исключается из
  one-to-one);
- непарные OLD → `deleted_candidate`, непарные NEW → `added_candidate`;
- **one-sided страницы**: страница, которой нет в карте `page_pairs` (лист есть
  только в OLD или только в NEW — частый случай разреженного/одностороннего
  `page_alignment`, когда листы сильно «уехали»), НЕ отбрасывается: все её блоки
  идут в `deleted_candidate` (OLD-only) / `added_candidate` (NEW-only) → все
  `qwen_required`. Это гарантирует полный охват прекчека (а не только
  двусторонних страниц).

## Text compare

- `canonicalize_text`: NFKC, trim, нормализация пробелов и переводов строк
  (регистр/содержимое сохраняются);
- строгое равенство canonical → `text_equal=true`;
- **fuzzy-skip не включён** — `similarity` (difflib) только логируется;
- текстовые блоки (text/table) полностью покрываются OCR — текст авторитетен.

## Visual compare (опционально, cv2)

- источник изображения: локальный РАСТР `image_file` (если есть и не /tmp-PDF),
  иначе рендер из ИСХОДНОГО PDF по `coords_norm` (в проде `image_file` — обычно
  недоступный /tmp-PDF от OCR-джобы, поэтому почти всегда рендерим сами);
- оба crop'а приводятся к общему размеру; лёгкий blur гасит суб-пиксельный
  antialiasing от разницы DPI;
- выравнивание — `cv2.findTransformECC` (`MOTION_EUCLIDEAN`);
- метрики: `total_diff_ratio` (доля изменённых пикселей по серому),
  `colored_overlay_diff_ratio` (через HSV saturation — ловит цветную правку),
  `diff_bbox` (нормализованный), `alignment_score` (корреляция ECC);
- ECC не сошёлся (cc < `ECC_MIN_SCORE`) → `alignment_failed` (visual_uncertain);
- для **чертежей визуал авторитетен**: OCR-текст на графике шумный и сам по себе
  НЕ даёт `changed_text` — используется только как fallback, когда визуал не дал
  решения;
- **cv2 опционален**: без него визуал → `visual_unavailable` → `qwen_required`
  (никогда не превращается в ложный skip).

## Decision / qwen_action

| decision | когда | qwen_action |
|---|---|---|
| `identical_text` | text-блок, canonical-текст совпал | `qwen_skip_candidate` |
| `identical_visual` | визуал идентичен (diff/colored ниже порогов) | `qwen_skip_candidate` |
| `changed_text` | text-блок различается / визуал недоступен + OCR различается | `qwen_required` |
| `changed_visual` | визуал изменён | `qwen_required` |
| `added_candidate` | блок только в NEW | `qwen_required` |
| `deleted_candidate` | блок только в OLD | `qwen_required` |
| `split_merge_uncertain` | один↔много перекрытие | `qwen_required` |
| `render_failed` | не удалось получить изображение | `qwen_required` |
| `alignment_failed` | ECC не сошёлся | `qwen_required` |
| `uncertain` | нет решающего текст/визуал сигнала | `qwen_required` |

`qwen_skip_candidate` ставится ТОЛЬКО при уверенной идентичности. Любое
сомнение → `qwen_required`. Stage 1 (observe) всё равно НЕ пропускает Qwen.

## Интеграция (observe, не меняет Qwen)

- Прекчек запускается из
  [md_enrichment_jobs.run_md_enrichment_job](../backend/app/services/stage_comparison/md_enrichment_jobs.py)
  один раз на пару (после preflight, до цикла enrichment), только если флаг
  включён. Рендер/cv2 — CPU-bound, уводится в `asyncio.to_thread` (не блокирует
  event loop).
- Отчёт пишется в `comparison/sessions/<sid>/pairs/<pid>/block_equivalence/`,
  компактная диагностика прикладывается к job.json (`job["block_equivalence"]`)
  и в `aggregate_job_progress(...)["block_equivalence"]`.
- [pipeline_queue._qwen_process_pair](../backend/app/services/stage_comparison/pipeline_queue.py)
  читает готовый отчёт и кладёт диагностику в per-pair статус
  (`item["block_equivalence"]`) — без повторного расчёта.
- **Fail-soft на всех уровнях**: любая ошибка прекчека не влияет на enrichment.
- GRSH/GRSH_FEEDERS/GRSH_CORE_SYSTEMS и dense_grsh НЕ трогаются — для них Stage 1
  только observe (и так ничего не пропускается).

## Diagnostics (pipeline status)

`block_equivalence`:
- `enabled`, `mode`, `skip_qwen`, `cv2_available`
- `total_old_blocks`, `total_new_blocks`, `paired`
- `identical_text`, `identical_visual`, `changed_text`, `changed_visual`
- `added_candidates`, `deleted_candidates`, `split_merge`, `uncertain`
- `potential_qwen_saved` (= число `qwen_skip_candidate`)

## Артефакты

- `block_equivalence/block_equivalence_report.json` — полный отчёт (summary +
  per-pair решения + текст/визуал метрики + added/deleted/split_merge);
- `block_equivalence/debug/{block_id}_diff.png` — OLD | NEW(aligned) | overlay
  для changed_visual блоков.

Это НОВЫЕ артефакты в отдельной папке. `comparison_result.json`,
`expert_review.json`, `v2_review_status.json` НЕ затрагиваются.

## Env-флаги

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED` | `false` | **главный включатель** |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_MODE` | `observe` | Stage 1: только observe |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_SKIP_QWEN` | `false` | **в observe форсится в False** (защита от случайного skip); включается только во втором этапе |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_IOU_THRESHOLD` | `0.5` | порог уверенного one-to-one |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_OVERLAP_THRESHOLD` | `0.2` | порог перекрытия для split/merge |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_RENDER_LONG_SIDE` | `1000` | длинная сторона рендера блока |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_VISUAL_DIFF_PIXEL_THRESHOLD` | `30` | порог «изменённого пикселя» (серый) |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_VISUAL_IDENTICAL_MAX_RATIO` | `0.02` | total_diff_ratio ≤ → identical |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_COLORED_DIFF_SAT_THRESHOLD` | `40` | порог цветного отличия (HSV S) |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_COLORED_IDENTICAL_MAX_RATIO` | `0.01` | colored_overlay_diff_ratio ≤ → identical |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_ECC_MIN_SCORE` | `0.55` | ниже корреляции ECC → alignment_failed |
| `STAGE_COMPARISON_BLOCK_EQUIVALENCE_MAX_VISUAL_COMPARES` | `600` | верхняя граница визуальных сравнений на пару |

## Зависимости

Визуальный слой требует `opencv-python-headless` (+ numpy/Pillow/PyMuPDF, уже
есть в окружении). **cv2 опционален**: без него прекчек работает (text + pairing),
а визуальное сравнение деградирует до `qwen_required` — ложного skip не будет.
Установка:

```bash
pip install --user opencv-python-headless
```

## Как включить (observe)

```env
STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED=true
# MODE остаётся observe, SKIP_QWEN остаётся false
```
Рестарт backend (uvicorn без `--reload` держит модуль в памяти):
```bash
pkill -f "uvicorn backend.app.main"
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
```
Запустить (или перезапустить) md-enrichment для пары через UI/API. Отчёт
появится в `block_equivalence/`, диагностика — в job/pipeline status.

## Как отключить / откатить

```env
STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED=false
```
Рестарт backend → поведение идентично сборке без фичи (no-op).

## Stage 2 (потом, отдельно)

После анализа статистики observe-прогонов:
- включить safe-skip (`SKIP_QWEN=true` вне observe) для надёжных категорий
  (`identical_text`, и далее `identical_visual` после калибровки порогов);
- ужесточить визуальные пороги под мелкие изменения на больших листах;
- dense_grsh оставить под Qwen дольше всех.

## Контролируемая проверка (без Qwen)

Прекчек = детерминированный анализ result.json + рендер из PDF (Qwen/сеть не
нужны). На реальных парах сессии Балчуг (`ba413a93c5754f6c`):

- self-compare (OLD vs OLD) пары `p08e4601e` (АР2): 36/36 блоков →
  `identical_text=4` + `identical_visual=32`, `potential_qwen_saved=36`
  (механизм корректно ловит идентичность);
- реальная OLD↔NEW `p08e4601e` (АР2 ↔ АР2-КОРР): 29 paired (27 changed_text,
  2 changed_visual), 3 added, 3 deleted, `potential_qwen_saved=0` (исправленная
  версия — всё изменилось, безопасных skip нет — корректно);
- реальная OLD↔NEW `p9692b6b5` (ГРЩ ИОС1.1): всё → `qwen_required`,
  ничего не пропущено.

## Связанные файлы

- [block_equivalence_precheck.py](../backend/app/services/stage_comparison/block_equivalence_precheck.py) — extract/IoU/pairing/split-merge/text/visual/decision/report + pair-orchestration
- [paths.py](../backend/app/services/stage_comparison/paths.py) — `block_equivalence_dir/report_path/debug_dir`
- [md_enrichment_jobs.py](../backend/app/services/stage_comparison/md_enrichment_jobs.py) — observe-хук + diagnostics
- [pipeline_queue.py](../backend/app/services/stage_comparison/pipeline_queue.py) — surface в per-pair status
- [blocks.py](../backend/app/services/stage_comparison/blocks.py) — `normalize_blocks_from_result_json` (родственная нормализация)
- [block_pdf_source.py](../backend/app/services/stage_comparison/block_pdf_source.py) — block-PDF render/текст-слой (родственный слой)
- [tests/test_stage_comparison_block_equivalence.py](../tests/test_stage_comparison_block_equivalence.py)
