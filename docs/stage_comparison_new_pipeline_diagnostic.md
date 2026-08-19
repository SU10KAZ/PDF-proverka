# Диагностический вывод новой цепочки сравнения во вкладку «Расхождения»

**Статус:** диагностика, не production-интеграция. Введено 19.08.2026.

Витрина показывает результат новой ДЕТЕРМИНИРОВАННОЙ цепочки сравнения рядом
со старым Opus-путём, чтобы качество алгоритма можно было оценить глазами до
того, как его начнут улучшать дальше.

```
PreparedDocument → сопоставление листов → идентичность → геометрическое
совмещение → canonical vector diff → atomic regions → change groups (5Б.4)
→ semantic diff 6А.2  →  вкладка «Расхождения», режим «Новый алгоритм»
```

## Где переключатель

Раздел «Сравнение стадий» → вкладка **«3. Расхождения»** → первая кнопка в ряду:
**🧪 Новый алгоритм (диагностика)**. Повторный клик возвращает обычный режим.

Пока режим включён, старые панели (V2 и классический unified) скрыты — два
источника никогда не смешиваются в одном списке.

## Что режим НЕ делает

Осознанные ограничения — это диагностика, а не продукт:

* не читает и не меняет `comparison_result.json` старого Opus-пути;
* не создаёт findings и не трогает экспертные решения;
* не считает влияние / severity;
* не вызывает LLM, Vision и OCR;
* **не дедуплицирует результат** — одинаковые «Было → Стало» остаются
  отдельными строками и только помечаются счётчиком
  `same_semantic_result_as_other_groups`. Это известная слабость локализации
  semantic diff, и прятать её нельзя: её надо увидеть;
* не перезаписывает артефакты этапов 5Б/6А.

## Источник данных

| Что | Файл |
|-----|------|
| Смысл изменений | `comparison/semantic_diff_v6a2/semantic_diff.json` (kind `…_v6a2_mass`) |
| Группы, регионы, матрицы | `comparison/change_detection/change_detection.json` (kind `…_v5b4`) |
| Листы, штампы, размеры страниц | `…/03_analysis/latest/prepared_comparison/prepared_document.json` |
| Слот для перехода | карта страниц пары (`page-alignment`) |
| Готовые кропы 12 пилотных групп | `comparison/semantic_diff_v6a1/diagnostics/v2_NNN_v3_NNN_<group>_{v2,v3,overlay}.png` |

Если `semantic_diff_v6a2` или `change_detection` отсутствуют, витрина честно
отдаёт `available:false` с причиной (`…_missing_run_stage_6a2_first` /
`…_missing_run_stage_5b4_first`) и подсказывает порядок этапов.

## Система координат

`bbox` групп и atomic-регионов заданы в **точках страницы левого (V2) PDF** —
так их построил этап 5Б: `change_regions.analyze_pair` переводит правую
страницу в левую матрицей этапа 5А. Витрина:

* нормирует bbox по `page_size` левой страницы → `bbox_norm_left` (доли 0..1);
* пересчитывает **обратной** матрицей в координаты V3 → `bbox_right`,
  затем нормирует по правой странице → `bbox_norm_right`.

Вырожденная матрица не роняет витрину: правая сторона просто становится `null`.

## API (read-only)

```
GET  /api/stage-comparison/sessions/{sid}/pairs/{pid}/diagnostic/new-pipeline
GET  /api/stage-comparison/sessions/{sid}/pairs/{pid}/diagnostic/new-pipeline/crop
       ?left_page=&right_page=&group_id=&side=v2|v3|overlay&target_long_side=
```

Кроп отдаёт заголовок `X-Crop-Source`:

* `pilot_file` — переиспользован готовый PNG этапа 6А.1 (12 групп);
* `on_demand_render` — область отрисована из PDF **в память**, на диск ничего
  не пишется.

`side=overlay` доступен только там, где пилотный файл уже есть: overlay строит
этап 6А.1 своей логикой совмещения, и вторую такую логику здесь не заводим.

## Что видно в таблице

| Колонка | Содержимое |
|---------|-----------|
| Место | лист V2 → V3, стр. PDF обеих сторон, имя листа из штампа, метка «лист: проверить» |
| Изменение | тип группы, `change_kind`, `change_summary`, метка «⧉ такой же смысл ещё у N» |
| Было / Стало | `before` / `after` из 6А.2 без правок |
| Диагностика | `evidence_level`, `confidence`, `source`, «требует проверки», `next_analysis`, `unresolved_reason` |

Подробности строки: кроп V2 / V3 / overlay, структурные изменения
(`Позиция / Колонка / Было → Стало` из таблиц, чисел и полей штампа),
добавленные и удалённые строки таблиц, список atomic regions с bbox и типами
evidence.

Фильтры: `все / exact / strong / contextual / insufficient / требует проверки`
и по типу группы. Ничего не скрывается по умолчанию.

## Переход к месту

Кнопка «→» открывает вкладку «Связь блоков», прокручивает к слоту пары и
подсвечивает на **обеих** страницах:

* **красная сплошная рамка** — сама change group;
* **синий пунктир** — atomic regions внутри неё.

Над панелями появляется плашка с кнопкой «Снять подсветку».

## Флаг

`STAGE_COMPARISON_NEW_PIPELINE_DIAGNOSTIC_ENABLED` — default **ON**. При `0`
endpoint отдаёт `available:false` с причиной `disabled_by_flag:…`, а кроп — 403.

## Файлы

| Файл | Роль |
|------|------|
| `backend/app/services/stage_comparison/diagnostic_new_pipeline.py` | сборка витрины, геометрия, кропы |
| `backend/app/services/stage_comparison/store.py` | `get_new_pipeline_diagnostic`, `render_new_pipeline_crop` |
| `backend/app/api/routers/stage_comparison.py` | два GET-endpoint'а |
| `frontend/index.html` | кнопка-переключатель, панель, оверлеи подсветки |
| `frontend/static/js/app.js` | состояние `scNp*`, фильтры, кропы, переход |
| `backend/tests/test_diagnostic_new_pipeline.py` | 16 тестов, включая «витрина ничего не пишет» |

## Замер на тестовой паре (13АВ-РД-АР0.1-ПА V2 ↔ V3)

93 группы · exact 16 · strong 32 · contextual 24 · insufficient 21 ·
требуют проверки 45 · **дублирующийся смысл 60** (93 группы дают лишь 46
уникальных пар «Было → Стало»). Готовый overlay есть у 12 групп из 93.
