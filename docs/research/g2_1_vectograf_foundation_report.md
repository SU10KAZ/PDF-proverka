# G2.1 — Vectograf Foundation Refactor

## 1. Изменённые файлы

- `backend/app/pipeline/stages/block_grounding/vector_evidence.py` — новый
  дисциплинарно-нейтральный слой PDF-vector evidence.
- `backend/app/pipeline/stages/block_grounding/singleline_graph_geometry.py` —
  classic path переведён на common evidence, добавлены prepared `page_index`,
  раздельные quality gates и provenance.
- `backend/app/pipeline/stages/block_grounding/block_source_router.py` — prepared
  page metadata передаётся в Вектограф; слова для клипа приводятся к visual space.
- `backend/app/api/routers/blocks.py` — page metadata блока сохраняется и используется
  как основной источник в preview/diagnostic path.
- `tests/test_vector_evidence.py` — тесты rotation 90/180/270, prepared page index,
  legacy fallback и диагностируемой ошибки extraction.
- `backend/tests/test_vectograf_gate.py` — тесты независимых extraction/structure gates.
- `tests/test_vectograf_eom_profiles.py` — classic regression и additive provenance.

## 2. Исправление `/Rotate`

Common extractor читает слова и drawing primitives в исходном PDF data space и
применяет `page.rotation_matrix` ровно один раз. После этого слова, линии, кривые и
полигоны находятся в visual space, размеры которого берутся из `page.rect`, и только
затем клипуются prepared polygon/bbox.

На страницах с rotation `0` преобразование слов является no-op, поэтому координаты
classic path не меняются. Тесты для `/Rotate 90`, `/Rotate 180` и `/Rotate 270`
сверяют итоговый bbox слова и линию с однократным применением `rotation_matrix` и
одновременно проверяют попадание в visual-space polygon.

## 3. Определение страницы

`build_singleline_graph()` получил additive параметры `page_index` и `block_id`.
Если `page_index` присутствует в prepared block metadata, он используется напрямую
и `_find_page_index()` не вызывается. Если metadata отсутствует, сохраняется старый
поиск страницы по отличительным токенам.

`result.json`-индекс предпочитает `block.page_index`, затем наследует
`pages[].page_index`; отсутствие обоих значений оставляет legacy fallback. Источник
решения фиксируется как `prepared_block` или `legacy_fallback` в provenance.

## 4. Вынесенные helpers и common evidence

В `vector_evidence.py` перенесены без дублирования:

- `_clip_words_to_bbox`, `_clip_words_to_polygon`, `_point_in_polygon`;
- `_convex_hull`, `_median`, `_near`, `_near_xy`;
- общий gap-clustering primitive;
- visual coordinate transforms и извлечение/клип слов, paths, lines, curves,
  polygons.

Старые импорты из `singleline_graph_geometry.py` остаются рабочими через re-export,
поэтому существующие consumers не требуют миграции. `VectorEvidence` не содержит
QF/feeder/bus/electrical roles и при ошибке возвращается как объект с явными
`extraction_gate.reason/reasons`, а не как silent `None`.

## 5. Что осталось неизменным

- `structure_singleline_text()` и правила classic singleline не менялись.
- Колоночная привязка QF↔код, анализ рядов, секций, вводов и потребителей не менялись.
- Старые пороги `evaluate_vectograf_gate()` и итоговое решение `use` сохранены.
- Старые ключи graph/gate сохранены; `quality_gates` и `provenance` добавлены
  аддитивно.
- Graphic G1, Stage 5.3 и text pipeline не менялись.
- Dense GRSh, SYSTEM_GRAPH, comparator, Mode 2 и новые дисциплины не реализовывались.

## 6. Точки расширения для будущего dense GRSh

Будущий профиль сможет вызвать `extract_vector_evidence()` с теми же prepared
`page_index`, polygon и bbox, проверить независимый extraction gate и получить уже
нормализованные visual words/drawings/paths. После этого профиль добавит только свои
диалектные anchors/roles и structure gate. Classic path останется первым и продолжит
использовать существующий structurer и topology builder.

В G2.1 профиль или dialect router намеренно не создан: подготовлен только общий вход
evidence и раздельная диагностика качества.

## 7. Проверки

- Целевой Vectograf/common-evidence набор: `43 passed`.
- Vectograf + singleline + основные block-grounding интеграции: `88 passed,
  23 skipped` (локальные PDF-корпусы, которых нет в checkout, штатно skipped).
- Полный Stage Comparison набор: `300 passed`.
- Расширенный набор всех тестов, импортирующих `block_grounding`: `1362 passed,
  35 skipped`; один environment-only сбой из-за локального `.env`, где
  `STAGE01_DUAL_GAP_SEARCH_ENABLED=True`, при ожидаемом тестом `False`.
- Тот же конфигурационный тест с `STAGE01_DUAL_GAP_SEARCH_ENABLED=false`: `1 passed`.

Дополнительно выполнены `py_compile` изменённых Python-модулей и `git diff --check`.

## 8. Регрессии

Функциональных регрессий в затронутых наборах не обнаружено. Реальный classic EOM
кейс сравнивается между legacy page-finder и prepared-page путём: весь прежний граф,
включая validation и quality gates, совпадает; отличается только additive provenance
источника страницы.

Единственное красное состояние расширенного запуска вызвано значением существующего
локального `.env` и воспроизводимо проходит при ожидаемой тестом настройке. Production
config в рамках G2.1 не изменялся.
