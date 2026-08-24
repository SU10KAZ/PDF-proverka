# Предлагаемая архитектура: EOM VECTOGRAF → SYSTEM_GRAPH

Research-документ. Production-код не менялся.

## 1. Что сейчас

```
vector_text блока
   │
   ├─ structure_singleline_text()        ← ЕДИНСТВЕННЫЙ вход. Ищет построчный
   │      PARAM_RE / SEPARATE_PARAM_RE     расчётный якорь. Нет якоря → None
   │      (>= 2 якоря, потом >= 3 фидера)
   ▼
build_singleline_graph()
   │  fitz.open + _find_page_index(по токенам текста!)
   │  клип words по bbox/polygon  ← БЕЗ учёта поворота страницы
   │  _QF_RE = QF\d+(\.\d+){1,2}  ← обязательна точка; < 3 → None
   │  y_split = 60 % размаха Y    ← отходящие vs вводные
   │  _bind_codes_columnwise      ← ЛУЧШАЯ часть: привязка код↔колонка по δ
   │  _extract_bus_sections       ← X-разрыв > 3 шага И маркер L1,L2,L3
   │  _extract_metering / _extract_input_devices / _extract_feeder_pairs
   │  _extract_power / _extract_*_calcs / _extract_notes  ← по ТЕКСТУ листа
   ▼
evaluate_vectograf_gate()          ← один набор порогов расчётного диалекта
```

Диалект зашит не в один параметр, а в семь мест сразу: якорь расчёта, форма
метки аппарата, схема нумерации панели, форма кода цепи, порог секции, состав
вводной зоны, метрики гейта.

## 2. Что предлагается

```
ПОДГОТОВЛЕННЫЙ БЛОК (upstream: block_id, page_index, coords_norm, polygon)
   │
   ▼
① EVIDENCE SCAN  (общий; дисциплинарно нейтрален)
   words в ВИЗУАЛЬНЫХ координатах (rotation_matrix) + клип по полигону
   + канонический построчный текст блока
   ├─ пути: block_grounding/vector_evidence.py (новый, но код взят из
   │        _clip_words_to_polygon и graphic_comparison/extraction.py)
   ▼
② CLASSIC-FIRST ROUTER
   structure_singleline_text(text) даёт >= 3 фидера?
        ДА  → СТАРЫЙ ПУТЬ, без единого изменения (регресс = 0 по построению)
        НЕТ → ③
   ▼
③ DIALECT DETECTOR  (по уликам, не по документу)
   сигнатуры: доля точечных QF, доля префиксных QF, число построчных якорей,
   число key-value подписей, маркеры шин, наличие QS/ВР, приборы учёта, АУКРМ
   → profile_id + minimum evidence profile
   ▼
④ PROFILE  — задаёт ТОЛЬКО:
   device_re · role_rules · regex-варианты кода/потребителя ·
   пороги (min_devices, окно колонки) · допустимая топология · нормализация
   ▼
⑤ SHARED BACKBONE  (единый для всех профилей И будущих дисциплин)
   device_row_detection      Y-кластеризация аппаратов
   section_partition         непрерывный X-раскрой, максимизирующий согласие
                             с семантикой метки + премия за реальный разрыв
   role_assignment           секционник = верхний аппарат В РАЗРЫВЕ;
                             ввод = максимальный номинал своей колонки;
                             защита = маркер УЗИП/ОПН/FU рядом;
                             остальное = UNKNOWN_NODE (честно)
   column_binding            production _bind_codes_columnwise, применённый
                             ПОРЯДНО (по Y-рядам) и СРАЗУ КО ВСЕМУ ряду листа
   source_path               якорь источника ниже ввода + промежуточные узлы
   functional_groups         учёт / компенсация / служебные — по X-секциям
   ▼
⑥ SYSTEM_GRAPH  (единый контракт, provenance на каждом узле и ребре)
   ▼
⑦ HONESTY GATES (раздельные): source / bus / section / feeder_coverage /
   identity_coverage / unresolved_nodes / unresolved_edges
```

Компаратор стоит ОТДЕЛЬНО и о дисциплине не знает:

```
SYSTEM_GRAPH LEFT ─┐
                   ├─→ COMPARATOR (A: остов, B: группы, C: аппараты)
SYSTEM_GRAPH RIGHT ┘        + отдельный проход «детализация ≠ изменение»
                            ↓
                     GraphicChangeLedger (тот же конверт, что у G1)
```

## 3. Что переиспользуется из существующего Вектографа

| Механика | Статус |
|---|---|
| `_clip_words_to_polygon` / `_clip_words_to_bbox` | берётся как есть |
| `_bind_codes_columnwise` (δ-алиасы, конфликты границы колонки) | ядро привязки, берётся как есть |
| `_split_codes_by_y_rows` (идея PRIMARY/SECONDARY рядов) | обобщается на N рядов |
| `_median`, `_near`, `_near_xy`, `_cluster` по X-разрывам | берутся как есть |
| `_extract_metering` (пофидерная гребёнка учёта по X) | идея берётся, регэксп приборов уходит в профиль |
| `_extract_bus_sections` | идея «кластер + маркер шины» берётся, жёсткие пороги уходят в профиль |
| `_extract_input_devices` (типизация QS/FU/УЗИП/РН/HL) | таблица типов берётся как есть |
| `evaluate_vectograf_gate` | форма «use + reasons + metrics» берётся, набор метрик разделяется |
| `structure_singleline_text` | остаётся ЦЕЛИКОМ как профиль `classic_calc_singleline` |

## 4. Что меняется в существующем коде (для production-задачи G2)

1. **Поворот страницы.** `build_singleline_graph` обязан переводить слова в
   визуальные координаты (`page.rotation_matrix`) перед клипом по нормированной
   области блока. Сейчас на листах с `/Rotate` клип промахивается молча.
2. **Страница по контракту, а не подбором.** Вместо `_find_page_index` брать
   `page_index` подготовленного блока (fallback на подбор оставить).
3. **Вынести из `build_singleline_graph` общий слой** (клип, ряды, колонки) в
   отдельный модуль, чтобы им пользовались оба пути.
4. **Гейт разбить** на независимые показатели вместо одного набора порогов.

Ничто из этого не меняет поведение классического диалекта: пункты 1–2 на листах
без поворота — no-op, пункты 3–4 — рефакторинг и расширение отчётности.

## 5. Границы применимости

* Компаратор дисциплинарно нейтрален: чтобы подключить ВК или ОВ, нужен только
  их экстрактор, отдающий тот же `SYSTEM_GRAPH`. Ни один тип узла в схеме не
  назван по-электрически (`SOURCE`, `BUS_SECTION`, `SECTION_DEVICE`,
  `OUTGOING_DEVICE`, `LOAD`, `SERVICE_NODE` — это роли в системе, а не аппараты).
* `TRANSFORMER` намеренно НЕ отдельный тип узла, а `SOURCE.subclass`. Иначе
  «показали трансформатор подробнее» неизбежно читается как `NODE_TYPE_CHANGED`.
* Vision в схеме отсутствует как строитель графа. Его место — разрешение
  локальных неоднозначностей (`UNCERTAIN_STRUCTURAL_CHANGE` с provenance
  `VECTOR` → повторная проверка с provenance `BOTH`).
