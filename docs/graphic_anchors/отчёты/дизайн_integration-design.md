Кабинетная проработка завершена (все 8 отчётов прочитаны, ключевые точки сверены с кодом: состав `block_grounding/`, `block_context/builder.py`, флаги в `backend/app/core/config.py:834-864`, тесты `tests/test_vectograf_*`, корпус `experiments/блоки разных дисциплин/audit_all_disciplines.py`). Ниже — дизайн-карта архитектуры интеграции.

---

# АРХИТЕКТУРА ИНТЕГРАЦИИ: универсальный граф-слой для всех графических блоков

## 1. СТРУКТУРА МОДУЛЕЙ

### 1.1 Новый пакет `graphic_primitives` (единая библиотека, ярусы A/B)

Создать `backend/app/pipeline/stages/block_grounding/graphic_primitives/` — извлечение общих примитивов из трёх мест, где они сейчас размазаны (singleline_graph_geometry.py = ярусы A/B ЭОМ, hvac_geometry.py = «материнский» модуль ОВ/ВК/АР/КЖ/КМ/ГП/ТХ, vector_path_graph.py = paths-backbone СС):

| Модуль | Содержимое | Откуда переносится |
|---|---|---|
| `text_extract.py` | клипы fail-closed (`_clip_words_to_bbox/_polygon`), `_filter_text_lines_to_region` (с уроком «порядок строк pdfplumber сохраняется»), `find_page_index(needle_re)` — параметризованный | singleline_graph_geometry.py:52-183; дедуп 3 копий `_find_page_index` (G:52, low_voltage_geometry.py:132, water_supply_geometry.py:66) |
| `spatial.py` | `near/near_xy`, `point_in_polygon`, `point_segment_distance`, `cluster_by_gap(xs, thr)` (сейчас 3 инлайн-копии G:1139/1266/1377), `median` | G:161-237, vector_path_graph.py |
| `columns.py` | `bind_columnwise` (offset-corrected nearest column с алиасами δ±шаг и GEOMETRY_CONFLICT), `split_by_y_rows`, монотонный fallback+cross-check, допуски в **долях медианного шага** | G:645-761, G:1640-1668 |
| `residual_labels.py` | движок «вычесть известное → остаток = подпись» с параметром `known_re` | G:552-599 (`_CONSUMER_KNOWN_RE` уходит в конфиг профиля) |
| `anchor_collect.py` | «якорь-с-единицей + вертикальный/строчный сбор атрибутов», «атрибут по Y-строке хозяина», восстановление разорванных CAD-токенов «число+единица» | G:1359-1394, G:237, G:1575-1584 |
| `components.py` | **единый** union-find: endpoint-режим (`_components` hvac_geometry.py:170) и T-junction-режим (`build_segment_components` vector_path_graph.py:455) + spatial-hash индекс (см. риски) | hvac_geometry.py:170-207, vector_path_graph.py:455-553 |
| `paths.py` | достроенный слой get_drawings: флэттен `re/qu/c`→полилинии, классификатор fill-заливок (стрелка/junction-dot/штриховка), извлечение `dashes`, использование `width` | план из vector-paths-drawings.md §4, п.1-3 |
| `dimensions.py` | **НОВЫЙ**: размерная линия (тонкая + засечки/fill-стрелки на концах) + выносные линии + число у середины → примитив `dimension`; сборка цепочек; сумма-чек | нет аналога |
| `leaders.py` | **НОВЫЙ**: генерализация `extract_callout_leaders` (убрать хардкод префиксов :590, полилиния «диагональ+полочка», привязка к любой компоненте) | vector_path_graph.py:582-623 |
| `axes_grid.py` | **НОВЫЙ**: осевой кружок (окружность из c-items + один токен внутри) + штрихпунктирная осевая + сетка; замена текстовой `_axes` как единственного источника | hvac_geometry.py:219-242, alia_remaining_geometry.py:88-104 |
| `tables.py` | **НОВЫЙ**: grid-детектор (горизонтали+вертикали+`re`-рамки → ячейки → слова), обобщение `_bind_floor_geometry` и словных эвристик journal/spec_table_geometry | structural_access_geometry.py:175-200, experiments/vector_pipeline/*.py |
| `ledger.py` | единый `semantic_ledger`/`semantic_facts` (реестр «факты без рёбер») | hvac_geometry.py:245-270, G:1429-1451 |
| `honesty.py` | каркас честности: occurrences/unique-счётчики, duplicate_*/unbound_*, GEOMETRY_CONFLICT-нотация, каркас гейта | G:1992-2030, G:2499 |
| `envelope.py` | единый конверт графа v2 + канонический реестр provenance (§2) | новый |

**Правило миграции — только re-export shims.** В `hvac_geometry.py`, `singleline_graph_geometry.py`, `vector_path_graph.py` имена сохраняются (`_components = graphic_primitives.components.endpoint_components` и т.п.) — тесты (`tests/test_hvac_geometry.py` и др. 20 файлов) и monkeypatch-точки не ломаются, `ci_regression_gate.py` остаётся зелёным. Дисциплинарные модули становятся тонкими: classify (лексика) + build (композиция примитивов) + gate (пороги из конфига) + render.

### 1.2 Вычистка хардкодов в декларативные конфиги

Расширить `backend/app/pipeline/stages/block_context/reference_catalog/` новым подкаталогом `profile_rules/` (loader по образцу `loader.py:26-78`, lru_cache + жёсткая валидация, версия в manifest):

```
reference_catalog/profile_rules/
  EOM.json   ← из ярусa C Вектографа: каталоги моделей _extract_power (G:512-523: «ВА-305 320А 35кА»,
               «ВР-101-630», «Мосэнергосбыт»), if-каталог _extract_service_elements (G:977-1028),
               панели _TT_ANCHOR_RE (G:769), стоп-маркеры _extract_notes (G:964)
  HVAC.json  ← _ID_HINTS 37 блоков (hvac_geometry.py:31-54), формат этажа L\d+_K\d+ (:318)
  VK.json    ← _ID_HINTS ~50 блоков (water_geometry.py:34-67)
  SS.json    ← _ID_HINTS 19 блоков (alia_remaining_geometry.py:29-40), _GATE_RULES под ALIA
               (alia_scheme_geometry.py:901-916: «ровно ≥7 шкафов», «ровно 2/4 корпуса»
               structural_access_geometry.py:737-772), словари цвет→тип сети (:677-680,
               alia_remaining_geometry.py:105-117), марки конкретного альбома (fallback-ярус :111-138)
  <disc>.json ← гейт-пороги всех evaluate_*_gate, needle-регэкспы find_page_index, render_budget
```

Ключевые принципы:
- **block_hints и альбомные марки — project-scoped**: секция `{"object_scope": "214_Alia_ASTERUS", "hints": {...}}`; роутер передаёт object id (доступен через `_locate`-путь `projects_v2/objects/<id>/`), хинты применяются только при совпадении. Это убирает главный барьер генерализации (перечислен во всех geom-отчётах) без потери точности на ALIA.
- **Регэкспы-грамматики сущностей остаются в коде** (`_QF_RE`, `_ADDRESS_RE`, `_SYSTEM_RE`, PATTERNS-словари) — они версионируются тестами и не проектно-специфичны. В конфиг уходят только каталоги моделей/марок, пороги и project-scoped данные.
- Гейт-пороги читаются через один helper `gate_thresholds(discipline, profile_id)`; захардкоженные числа остаются default-значениями в коде (fail-soft при отсутствии конфига).

## 2. ЕДИНАЯ СХЕМА ГРАФА

### 2.1 Конверт v2 (аддитивный к schema_version=1 `_base`)

```jsonc
{
  "graph_schema_version": 2,          // НОВОЕ поле; отсутствие = v1
  "profile_id": "...", "subtype": "...", "source": {...},
  "containers": [], "nodes": [], "networks": [], "edges": [],
  "semantic_ledger": [],
  // ── НОВЫЕ слои (все опциональны — рендеры обязаны терпеть отсутствие) ──
  "graphic_anchors": [                // детектированные графические примитивы-якоря
    {"id": "ga-1", "kind": "dimension_line|extension_line|tick|arrowhead|leader|leader_shelf|axis_circle|axis_line|table_grid|hatch_zone|junction_dot|frame",
     "geometry": {...}, "width": 0.2, "dash": "dot-dash", "color": [..]}
  ],
  "dimensions": [                     // размер как первоклассная сущность
    {"id": "dim-1", "value_node_id": "node-17", "value": 580, "unit": "mm",
     "dim_line_id": "ga-1", "extension_ids": ["ga-2","ga-3"],
     "target_ids": ["node-5"],        // элемент(ы), к которым привязан
     "chain_id": "chain-2", "provenance": "dimension_line_confirmed"}
  ],
  "dimension_chains": [{"id": "chain-2", "member_ids": [...], "sum": 1155,
     "declared_total": 1160, "closure_state": "mismatch|closed|open"}],
  "leaders": [{"id": "ld-1", "text_node_id": "...", "anchor_id": "ga-9",
     "target_ids": [...], "provenance": "leader_polyline_confirmed"}],
  "axis_grid": {"axes": [{"mark": "А", "circle_anchor_id": "ga-4", "line_anchor_id": "ga-5",
     "orientation": "h|v", "provenance": "axis_circle_confirmed|text_cluster_only"}],
     "cells_state": "derivable|materialized"},
  "tables": [{"id": "tbl-1", "grid_anchor_id": "ga-7", "rows": N, "cols": M,
     "cells": [{"r":0,"c":1,"text_ids":[...]}], "header_texts": [...]}],
  "validation": { ..., "dimensions_total": N, "dimensions_linked": M,
     "chains_closed": K, "leaders_linked": L },   // аддитивные счётчики
  "readiness": {...}, "warnings": [], "status": "..."
}
```

Решения:
- **Связь «размер→элемент» — это НЕ новый вид ребра-строки, а сущность `dimension` с evidence-ссылками на `graphic_anchors`**: LLM и детерминированные проверки получают и значение, и адресата, и доказательство. В `edges` при этом добавляются обычные рёбра `edge_type: measures|annotates|located_on_axis` — чтобы существующие потребители (localization, package_display, reference-сигнатуры) видели связность без знания новых слоёв.
- `structure_signature` каталога (`_graph_signature` block_profile_registry.py:149) считает `*_total` из validation — новые счётчики попадают в сигнатуры автоматически при пересборке каталога (`build_catalog.py`, bump `catalog_version`); старые записи совместимы (log-совпадение счётчиков толерантно к отсутствующим ключам).
- `SCHEMA_VERSION` пакета в `block_profile_registry.py:32` **остаётся 6**: контракт `make_package` не меняется (граф — opaque dict), `load_prepared_package` валидирует только версию пакета. Bump до 7 — лишь если изменится сам конверт пакета.

### 2.2 Канонический реестр provenance (сейчас размазан по 6 модулям)

Собрать в `envelope.py` единый реестр с **иерархией доказательности** (ранжирование уже де-факто едино во всех модулях: «код в марке > текстовая аннотация пары > цветной CAD-путь > близость» — geom-ss-lowvoltage.md, сквозное наблюдение №2). Существующие строковые коды НЕ переименовываются (обратная совместимость + словари `profiled_graph_localization.py:287-328` работают как есть); добавляется функция `provenance_tier(code) -> int`:

| Tier | Смысл | Коды (существующие + новые) |
|---|---|---|
| 5 | семантика кода/маркировки | `semantic_confirmed`, `confirmed_by_equipment_code`, `semantic_code_confirmed`, `semantic_system_source` |
| 4 | текстовая аннотация связи | `annotation_confirmed`, пары «К3.2-STR3.2» |
| 3 | подтверждённая CAD-геометрия | `same_cad_component`, `path_confirmed`, `confirmed_pair`, `colored_path_confirmed`, `blue_path_component`, `cad_endpoint_component`, **`dimension_line_confirmed`**, **`leader_polyline_confirmed`**, **`axis_circle_confirmed`**, **`table_grid_cell`** |
| 2 | геометрическая близость/порядок | `nearest_geometry`, `nearest_*_geometry`, `same_row_geometry`, `column_alignment`, `x_order_geometry`, `row_geometry_grouped`, `vertical_order_in_section`, **`text_cluster_only`** (оси без кружка) |
| 1 | инвентарь/декларация | `present`, `spatial_inventory`, `legend_only`, `engineering_annotation`, `equipment_role_inventory`, `secondary_description_only`, `inferred_from_*` (с обязательным warning) |
| 0 | отсутствие/непроверенность | `not_extracted`, `visual_unverified`, `shown_empty` (отдельная семантика: «так нарисовано»), `requires_review` |

Ортогональная ось — **кросс-валидация** (`physics_check: passed|failed|n/a`): физика P/I ЭОМ, замыкание размерной цепочки, монотонность ⌀ ВК. Она не повышает tier, а даёт готовые findings. Три существующих поля-носителя (`field_state` узлов, `edge_state`/`path_state` связей, `evidence_state` ledger) сохраняются; реестр — их общий словарь.

## 3. ПОРЯДОК РЕАЛИЗАЦИИ

**Фаза 0 — экстракция библиотеки (чистый рефакторинг, 0 поведенческих изменений).**
Пакет `graphic_primitives/` + shims. Гейт: `python -m pytest tests backend/tests` зелёный без изменений; `scripts/ci_regression_gate.py`; прогон `experiments/.../audit_all_disciplines.py` по корпусу 1133 эталонов — **бинарное сравнение** counts/gate_use с текущим `DISCIPLINE_COVERAGE.json` (983/124/26 не должны сдвинуться).

**Фаза 1 — достройка paths-слоя (`paths.py`), никем не потребляется.**
Флэттен `re/qu/c`, fill-классификатор (стрелки ~1-3 pt², junction-dot Ø0.8-2 pt, штриховки по площади), `dashes`+`width`, spatial-hash для T-junction. Только unit-тесты на PDF из корпуса experiments (по образцу skipif-паттерна `tests/test_low_voltage_geometry.py`). Эмпирические пороги — из проб коллег-агентов по зонам дисциплин.

**Фаза 2 — новые примитивы `dimensions.py`/`leaders.py`/`axes_grid.py`/`tables.py`.**
Каждый — с юнит-тестами на 3-5 корпусных PDF своего типа + негативные кейсы (ось «1000», размер без линии). Ничего не подключено к роутеру.

**Фаза 3 — ПИЛОТ «размер→элемент»: дисциплина АР** (профили `ar_masonry_plan`, `ar_masonry_detail`, `ar_wall_elevation`). Обоснование:
1. крупнейший корпус (247 эталонов, 935 dimension-узлов) — статистика для гейта;
2. **боевой прод-кейс ошибки уже есть**: размерная цепочка «580+580 против 1155 мм» (комментарий classify_ar_profile, architecture_geometry.py:30-32) — ловится ТОЛЬКО связыванием+сумма-чеком, т.е. пилот сразу даёт измеримую ценность для аудита;
3. лучший self-check без разметки: кладочные размеры почти всегда в цепочках → замыкание = автоматическая метрика точности;
4. механика 1:1 переносится на КЖ (опалубка), КМ, ТХ, ГП — раскатка дешёвая;
5. классификатор АР зрелый (21 профиль), маршрут `structured_architecture` в проде, причём с «полным текстом полигона» в user_text (block_source_router.py:786-796) — деградация невозможна by design.

Подключение: внутри `architecture_geometry._plan/_view_graph` под флагом; shadow-артефакт (см. фазу 5). Гейт пилота: на ручной выборке (§5) precision связей ≥0.95; закрытые цепочки ≥60% от собранных; 0 регрессий корпусного аудита.

**Фаза 3b — второй пилот «выноска→элемент»: КЖ `kj_reinforcement_plan`** — модуль сам декларирует выноску как недостающий механизм (warning structural_geometry.py:82: «попарная связь не создаётся без выноски или отдельного правила»); связь «поз. N → стержень/зона шага» — максимальная аудиторская ценность (армирование = КРИТ-класс).

**Фаза 4 — раскатка**: КЖ/КМ/ТХ (dimensions+leaders), все дисциплины (axes_grid: `_bind_nearest_axes` наконец вызывается везде, а не только в ЭОМ — electrical_geometry.py:142), таблицы (ведомости дверей/спецификации — сейчас профилей таблиц нет вообще, gallery-taxonomy-construct.md §6.5). ЭОМ ярус C переезжает на конфиги (§1.2) — отдельным коммитом с A/B на 15 боевых однолинейках.

**Фаза 5 — конвейерная обвязка**: рендер v2-слоёв в Markdown (§6), гейт-расширение, снятие флагов по дисциплинам.

**Тесты/гейты на каждом шаге** (по образцу `tests/test_vectograf_eom_profiles.py`): юнит на примитив → интеграционный на построитель (monkeypatch `_locate/_extract_block` как в `test_block_source_router_profile_routing.py`) → корпусный регресс (`audit_all_disciplines.py`, новые колонки `dimension_link_rate/leader_link_rate/chains_closed_rate`, baseline-файл в репо по паттерну `ci_regression_gate --record`) → shadow в проде.

## 4. РИСКИ

| Риск | Митигация |
|---|---|
| **O(N²) T-junction** (vector_path_graph.py:491-492, признано в коде), 120К hatch-путей ГП (general_plan_geometry.py:155-159), 71926 paths вентиляции (CLLU-QK33-EC9), 141915 paths диспетчеризации | (а) spatial-hash grid-bucket в `components.py` — O(N) кандидатов; (б) **порядок фильтров**: сначала классификация по width/dash/fill (размерные=тонкие, штриховки=fill) и отсев штриховок ДО связности; (в) жёсткие капы per-класс (образец: cap 60000 в `_route_geometry` electrical_geometry.py:113) + тайм-бюджет на блок (~3с) с fail-soft деградацией до текущего поведения — стадия block_context детерминированная и уже небыстрая, нельзя удвоить её время |
| Сканы без вектора | обобщить `source_layer_state` ВК (water_geometry.py:459-471) на все дисциплины; новые слои просто пусты, гейты не поднимают tier; растровый путь (`qwen_grounding.py`, флаг OFF) не трогается |
| **Ложные детекции** (ось «1000» = повторяющийся размер, 7DU7-346V-DN6; «Данный» как opening) | правило промоции: текстовый кластер осей получает tier 3 только при подтверждении кружком/штрихпунктиром (`axis_circle_confirmed`), иначе `text_cluster_only` (tier 2) + `node_state_needs_review`; dimension без размерной линии остаётся плоским узлом — **унаследованный инвариант «конфликт → заметка, не молчаливый выбор» (G:700-708) распространяется на все новые примитивы** |
| Обратная совместимость structure.json | все новые слои аддитивны, `graph_schema_version=2` присутствует только у новых графов; рендеры и `package_display` обязаны терпеть отсутствие полей; `load_prepared_package` не трогается; sidecar `graph_artifact` (незакоммиченный diff block_profile_registry.py:453-468) совместим — граф остаётся opaque |
| **Незакоммиченная параллельная работа** в `block_source_router.py` (+444 ветки СС, feature/block-vector-graphs) и полузакоммиченный symbol-evidence | НЕ трогать каскад роутера до влития СС-ветки; фазы 0-2 не касаются router; фаза 3 меняет только `architecture_geometry.py` (не в diff ветки); координация через пофайловые коммиты (урок `git add -A`) |
| Флаги | все default OFF, ленивое чтение per-call (образец `_bbox_clip_enabled` G:65): `GRAPHIC_PRIMITIVES_DIMENSIONS_ENABLED`, `_LEADERS_ENABLED`, `_AXES_ENABLED`, `_TABLES_ENABLED` (+ per-discipline суффикс при раскатке); shadow-флаг `GRAPHIC_PRIMITIVES_SHADOW_ENABLED` default **ON** (образец `VECTOGRAF_SHADOW_ENABLED`, config.py:834) — пишет `_output/graphic_primitives_shadow.json`: per-block счётчики «нашёл бы N размеров, связал бы M, цепочек K, конфликтов C», ничего не меняя в промптах |
| dashes отсутствуют в CAD-экспорте (штрихпунктир нарезан короткими сегментами) | fallback-детектор: коллинеарная цепочка коротких сегментов с регулярными зазорами = осевая-кандидат; подтверждение пробами на корпусе |

## 5. ИЗМЕРЕНИЕ КАЧЕСТВА связей «размер→элемент»

**Без ручной разметки (self-check, автоматически на всём корпусе 1133):**
1. **Замыкание цепочек**: sum(сегментов цепочки) == габаритный размер той же линии (допуск 1 мм в единицах чертежа). Доля замкнувшихся = прокси точности привязки чисел к линиям; расхождение = либо ошибка привязки (наша), либо ошибка проекта (finding) — различаются кратностью: одиночный промах на величину соседнего размера = алиас привязки.
2. **Согласованность масштаба**: least-squares scale по парам (px-длина размерной линии, значение); остатки >5% = подозрительная связь. Работает и для осей: межосевые из цепочек vs px-дистанции осевых кружков.
3. **Физика per-discipline** (расширение готовой идеи `_deterministic_protection_check`): ГП — Δh = i·L (уклон×длина против пары отметок); ВК — монотонность ⌀; КЖ — шаг×количество ≈ длина зоны.
4. **Уникальность**: одно значение → два элемента, две выноски → один текст = GEOMETRY_CONFLICT-rate как метрика шума (уже есть в гейте однолинейки: ≤15%).
5. **Каннибализация ledger**: доля dimension-узлов, получивших связь (recall-прокси), — уже считается корпусным аудитом как категория, нужна лишь новая колонка.

**Ручная выборка (единожды, ~2-3 часа разметки):** стратифицированно 5 блоков × 5 профилей = 25 блоков из experiments (АР: masonry_plan, wall_elevation; КЖ: reinforcement_plan; КМ: connection_detail; ГП: road_structure), ~30-50 связей на блок. Формат: CSV `block_id, dimension_id, verdict ∈ {ok, wrong_target, ambiguous}` рядом с корпусом. Метрика гейта: **precision ≥0.95 на связанных** (ложная связь в промпте хуже отсутствия — унаследованный принцип «слабый граф хуже честного „нет графа“», G:2463-2465); recall НЕ гейтится (непривязанное честно остаётся в ledger).

## 6. ЧТО ПОПАДАЕТ В ПРОМПТ Stage 02

Точки встраивания не меняются: `resolve_block_package` (gemma_findings_only.py:1970-1981) → `user_text` пакета → `build_effective_block_context` (:886). Меняются только рендереры (`render_*_markdown`) + `profiled_graph_localization` (новые лейблы).

**Приоритет слоёв при рендере (бюджет ~5-6К токенов для строительных дисциплин; однолинейка остаётся при своих 10-12К):**
1. **Шапка + вердикт гейта** (3-5 строк).
2. **Готовые детерминированные факты** — самое ценное, всегда полностью: «⚠ Цепочка размеров по оси А: 580+580 = 1160, габарит 1155 — РАСХОЖДЕНИЕ 5 мм»; «поз. 7 (Ø12 А500С) — выноска не ведёт ни к одной зоне армирования». Это прямой аналог секции «⚠ Проверка защиты» Вектографа (G:120).
3. **Связанные сущности компактно**: цепочка = ОДНА строка («цепочка: 580+580+1155 → элемент: стена в осях А-Б, замкнута»), не строка-на-размер; выноски таблицей `текст → элемент`; cap ~60 строк, излишек → счётчик.
4. **Непривязанное** — только счётчики + top-N значимых («размеров без привязки: 14, в т.ч. 3 на видах без рамки») — полный ledger в промпт НЕ идёт (это и сейчас так).
5. **Анти-boilerplate инструкции** (по образцу G:450-456): «непривязанность размера — ограничение разметки, НЕ дефект проекта, замечаний не делать» — обязательна, иначе новые слои породят волну ложных findings.

Provenance в рендере — **только когда tier < 3**: суффикс «(по близости)»; подтверждённое печатается без пометки (экономия токенов, фокус внимания LLM на слабом). `render_budget` per-profile — в `profile_rules/<disc>.json`; общий рендерер `envelope.py::render_envelope_markdown(graph, budget)` вызывается тонкими дисциплинарными рендерами для новых слоёв, существующие секции не переписываются.

**FINDINGS-классы, которые даёт расширенный граф детерминированно** (со ссылкой на классы норм; конкретные пункты — верифицировать через norms-MCP при имплементации, не выдумывать): незамкнутые размерные цепочки / расхождение сумм — класс оформления РД (ГОСТ Р 21.101) + предметные СП по кладке; позиция без адресата на схеме армирования — комплектность РД КЖ; EI на двери без связи с проёмом — противопожарные СП (класс «Проверить по смежным»); уклон×длина ≠ Δотметок — СП по благоустройству/водоотводу.

---

**Сводный порядок ценность×сложность:** Фаза 0 (обязательная база, риск ~0) → Фаза 1 (средняя сложность, разблокирует всё) → Фаза 3 АР-пилот (высокая ценность: боевой кейс, дешёвый self-check) → Фаза 3b КЖ-выноски (максимальная аудит-ценность, КРИТ-класс) → Фаза 4 (низкая предельная сложность). Таблицы (`tables.py`) — независимая ветка, можно параллелить: единственный потребитель-кандидат сразу — ведомости дверей АР (пары Д-N/EI30 лежат рядом без ребра, gallery-taxonomy-construct.md §1.5) и спецификации КЖ.

Ключевые файлы для правок: `backend/app/pipeline/stages/block_grounding/graphic_primitives/` (новый пакет), `singleline_graph_geometry.py` (shims + вынос ярусa C в конфиг), `hvac_geometry.py:129-270` (shims базовых хелперов), `vector_path_graph.py:71-107` (флэттен), `architecture_geometry.py:132-155` (пилот), `structural_geometry.py:69-83` (пилот 3b), `block_context/reference_catalog/profile_rules/*` (новые конфиги + loader), `block_context/builder.py:135-198` (shadow-хук по образцу summary), `backend/app/core/config.py` (флаги), `profiled_graph_localization.py` (лейблы новых kind/edge_type), тесты `tests/test_graphic_primitives_*.py` + расширение `experiments/.../audit_all_disciplines.py` (или его прод-порт `backend/scripts/audit_graph_primitives.py` с baseline-файлом).