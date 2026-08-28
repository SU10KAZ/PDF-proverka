# G2.4.5 — эталонный корпус и фактические справочники

> **Исторический корпус v1.** В этом документе зафиксирована прежняя,
> инвертированная ориентация GRAPHIC (`LEFT/RIGHT`). Он сохранён только для
> трассировки исходного исследования и не является действующим production-
> контрактом. Актуальная ориентация, семантика связей и выводы приведены в
> [g2_4_5_policy_corpus_v2.md](g2_4_5_policy_corpus_v2.md). Числовые результаты
> ниже оставлены без изменений.

**Режим:** read-only исследование. Код не менялся, коммитов нет.
**Точка отсчёта:** `main`, HEAD `61553e16` («fix: harden graphic coverage semantics»), дерево чистое.
**Дата:** 2026-08-26.

Каждое числовое утверждение ниже сопровождается путём к файлу или командой, которой
оно получено. Там, где данных/поля нет — написано `ABSENT` или `NOT PRESENT IN REAL DATA`.
Синтетические примеры не придумывались.

---

## 0. Периметр реальных данных

Всего в репозитории **три** пары Stage 5 / Stage 5.3 с готовыми артефактами
(`find . -name high_level_project_changes.json -not -path "./node_modules/*"`):

| Ключ в отчёте | pair_id | Документы (LEFT → RIGHT) | Дисциплина | SYSTEM_GRAPH |
|---|---|---|---|---|
| **ИОС/ГРЩ** | `p26c08b83a6` | `АА_БЭ-03-ДС3-ИОС1.1` → `АА-БЭ-03-ДС3-ИОС1.1` | EOM | есть (1 пара блоков) |
| **АР-1** | `p570d156f57` | `13АВ-РД-АР0.1-ПА_V2` → `13АВ-РД-АР0.1-ПА_V3` | AR | ABSENT |
| **АР-2** | `p16b108b9f5` | `АА_БЭ-03-ДС3-АР1` → `АА_БЭ-03-АР1-КОРР` | AR | ABSENT |

Сессия одна: `comparison/sessions/121d764109184c13/` (объект
`272_Sadovnicheskaya_76_Balchug_Esteyt`, сравнение `stage_1` ↔ `stage_2`).

Артефакты entity/scope/coverage построены **только для двух** пар:
`experiments/g2_4_4_scope_side_coverage/ios/` ← `p26c08b83a6`,
`experiments/g2_4_4_scope_side_coverage/ar/` ← `p570d156f57`
(подтверждено полем `text_entities.json → source_artifact.pair_id`).
Для **АР-2** entity-артефактов нет вообще.

> Единственное реальное MODE_2-сравнение во всём репозитории — одно
> (`experiments/g2_4_4_scope_side_coverage/ios/comparison_result.json`,
> байт-в-байт семантически равно `experiments/g2_system_graph_comparator/comparison_result.json`,
> проверено `json.load(a)==json.load(b)` → `True`). Из него получены все 6 графических изменений.

---

## ЧАСТЬ 1. Инвентаризация артефактов

### 1.1 `high_level_project_changes.json` — Stage 5.3, TEXT

* **Пути на диске:**
  `comparison/sessions/121d764109184c13/pairs/{p16b108b9f5,p26c08b83a6,p570d156f57}/high_level_project_changes.json`
* **Продюсер:** [backend/app/services/stage_comparison/high_level_project_changes.py](backend/app/services/stage_comparison/high_level_project_changes.py);
  запись — `store.py` (`high_level_project_changes_path()` в [paths.py:88](backend/app/services/stage_comparison/paths.py#L88))
* **Версия схемы:** ЕСТЬ — `schema_version: "1.0"` + `version: 1` + `kind: "stage_comparison_high_level_project_changes"`
* **Поля верхнего уровня (по данным):**
  `version, schema_version, kind, pair_id, generated_at, source_signature, source_artifact,
  prompt_version, validator_version, model, reasoning_effort, status, evidence_sources,
  high_level_changes, detail_level_increased, material_review, non_material_review,
  unresolved, service_structure_summary, semantic_groups, summary, constraints`
* **Запись изменения (элемент `high_level_changes[]` и всех остальных бакетов — один и тот же тип):**
  `change_id ("hlc_<16hex>"), type, title, status, confidence, reason, decision_source,
  evidence_sources, evidence_ids[], sheet_groups[], semantic_subject, count, details[]`
* **`details[]` (атом доказательства):**
  `evidence_id ("ev_<16hex>"), evidence_source, source_status, summary, before, after, reason,
  group_id, left_pages[], right_pages[], left_labels[], right_labels[],
  left_fragment_ids[], right_fragment_ids[], left_anchors[], right_anchors[],
  stage5_class, stage5_category, stage5_title, pair_status, cross_sheet_counterpart_evidence_ids`
* **`semantic_groups[]`:** `group_id, route, route_reason, candidate_type, subject, decision, decision_source, evidence_ids[]`
* **`service_structure_summary`:** `collapsed, groups, evidence_count, evidence_ids[], items[]` —
  `items[]` содержат **полноценные записи изменений** того же формата. Это **шестой бакет**, который
  легко пропустить: именно оттуда берётся 74-элементный `hlc_d3913c0ab7b930d2` в ИОС.
* **⚠ Дублирование:** `unresolved[]` — точная копия `material_review[]` во всех трёх парах
  (15/15, 4/4, 3/3). При подсчётах его нужно исключать, иначе двойной учёт.
* **`evidence_sources`** во всех трёх парах = `["TEXT"]`; `constraints.text_only = true`.

### 1.2 `project_change_summary.json` — Stage 5, TEXT

* **Пути:** те же три каталога `pairs/*/project_change_summary.json`
* **Продюсер:** [backend/app/services/stage_comparison/project_change_summary.py](backend/app/services/stage_comparison/project_change_summary.py),
  путь — [paths.py:84](backend/app/services/stage_comparison/paths.py#L84)
* **Версия схемы:** ⚠ **поля `schema_version` НЕТ**. Есть только `version: 1` и
  `kind: "stage_comparison_project_change_summary"` (+ `prompt_version`, `validator_version`).
* **Поля верхнего уровня:** `version, kind, pair_id, generated_at, source_signature,
  prompt_version, validator_version, model, reasoning_effort, status, sheet_groups, summary, constraints`
* **`sheet_groups[]`:** `group_id, left_pages, right_pages, left_labels, right_labels,
  pair_precheck, source_group_sha256, aggregation_status, error, usage,
  project_changes, service_structure, review, atomic_evidence`

### 1.3 `text_entities.json` — `text-entities.v1`

* **Реальные файлы:** `experiments/g2_4_4_scope_side_coverage/{ios,ar}/text_entities.json`,
  `experiments/g2_4_3_entity_producers/{ios,ar}/text_entities.json`
* **Продюсер:** [unified_entity_bridge/text_entity_producer.py](backend/app/services/stage_comparison/unified_entity_bridge/text_entity_producer.py)
  (`build_text_entities`, `validate_text_entities`, `is_stale`)
* **Схема:** [text_entities.schema.json](backend/app/services/stage_comparison/unified_entity_bridge/text_entities.schema.json)
* **Версия схемы:** ЕСТЬ — `schema_version: "text-entities.v1"` + `producer_version: "stage5-3-text-entity-producer-v1"` + `normalizer_version: "entity-normalizer-v1"`
* **Верхний уровень:** `schema_version, kind, producer_version, normalizer_version,
  source_signature, source_artifact, entities, quality_report`
* **`entities[]`:** `entity_id ("txt_ent_<20hex>"), canonical_name, display_names[],
  entity_type, domain_subtype, system, parent_context{}, sheet_groups[], pages[],
  evidence_ids[], fragment_ids[], source_change_ids[], confidence, provenance{}`
* **`provenance`:** `source_artifact_digest, evidence_index_digest, producer_version,
  normalizer_version, mentions[{evidence_id, field, rule}]`
* **Продовая обвязка:** `text_entities_path()` [paths.py:96](backend/app/services/stage_comparison/paths.py#L96),
  `store.get_text_entities_state`, GET-эндпоинт
  [stage_comparison.py:428](backend/app/api/routers/stage_comparison.py#L428). **На диске в
  `comparison/sessions/**` файла нет ни в одной паре** (`find comparison -name text_entities.json` → пусто).

### 1.4 GRAPH_ENTITIES — `graph-entities.v2`

* **Реальные файлы:**
  * v2 — внутри `experiments/g2_4_4_scope_side_coverage/{ios,ar}/side_graph_entities.json` → `sides.{LEFT,RIGHT}`
  * v1 (устаревший прогон) — `experiments/g2_4_3_entity_producers/{ios,ar}/graph_entities.json`
    (`schema_version: "graph-entities.v1"`, `adapter_version: "system-graph-entity-adapter-v1"`)
* **Продюсер:** [unified_entity_bridge/graph_entity_adapter.py](backend/app/services/stage_comparison/unified_entity_bridge/graph_entity_adapter.py)
  (`build_graph_entities`, `validate_graph_entities`, `is_stale`)
* **Версия схемы:** ЕСТЬ — `schema_version: "graph-entities.v2"`, `adapter_version: "system-graph-entity-adapter-v2"`
* **Верхний уровень:** `schema_version, kind ("system_graph_entities"), adapter_version,
  normalizer_version, source_signature, source_graphs[], entities[], quality_report`
* **`entities[]`:** `entity_id ("gfx_ent_<20hex>"), graph_node_ids[], canonical_name,
  display_labels[], entity_type, domain_subtype, functional_role, system, parent_context{parent_group,parent_node_ids},
  section_context, graph_scope{}, edge_ids[], external_connections[], source_tokens[],
  locations[], evidence_refs[], confidence, provenance{}`
* **`external_connections[]`:** `edge_id, edge_type, direction (INCOMING|OUTGOING), neighbour_node_id`
  — добавлено в v2, это ядро neighbour-aware coverage.
* **`graph_scope`:** `source_graph_index, graph_digest, profile_id, block_id, page_index, discipline, source_path[]`
* **Продовая обвязка:** `graph_entities_path()` объявлена
  [paths.py:100](backend/app/services/stage_comparison/paths.py#L100), но **нигде не используется**
  (`grep -rn "graph_entities_path" backend/ tests/ scripts/` даёт только само объявление и `__all__`).

### 1.5 ENTITY BRIDGE — `entity-bridge.v2`

* **Реальные файлы:**
  * не-side — `experiments/g2_4_3_entity_producers/{ios,ar}/entity_links.json`
  * side-aware — `experiments/g2_4_4_scope_side_coverage/{ios,ar}/side_entity_links.json`
    (`side-entity-links.v1`, обёртка с двумя ветками `LEFT`/`RIGHT`, каждая = полный `entity-bridge.v2`)
  * **фикстура** `backend/app/services/stage_comparison/unified_entity_bridge/entity_links.json`
    (это `entity-bridge.v1`, пример из G2.4.2, не production-выход)
* **Продюсер:** [unified_entity_bridge/entity_bridge.py](backend/app/services/stage_comparison/unified_entity_bridge/entity_bridge.py)
  + [side_entity_contract.py](backend/app/services/stage_comparison/unified_entity_bridge/side_entity_contract.py)
* **Версия схемы:** ЕСТЬ — `schema_version: "entity-bridge.v2"`, `bridge_version: "deterministic-entity-bridge-v1"`
* **Верхний уровень (v2):** `schema_version, kind ("text_graphic_entity_links"), bridge_version,
  normalizer_version, source_signatures{text_entities,graph_entities}, input_artifacts{text,graphic},
  input_entity_ids{text,graphic}, links[], diagnostics{}`
* **`links[]`:** `entity_link_id ("eln_<20hex>"), text_entity_id, graphic_entity_id,
  relation, confidence, evidence[]`
* **`evidence[]`:** `rule, level (0..4), outcome (MATCH|CONFLICT|AMBIGUITY), tokens[], normalization[], context{}`
* **`diagnostics`:** `text_entity_count, graphic_entity_count, candidate_link_count,
  relation_counts{SAME_ENTITY,POSSIBLE_ENTITY,UNKNOWN},
  confidence_counts{HIGH,MEDIUM,LOW,UNKNOWN}, unresolved_text_entity_ids[], unresolved_graphic_entity_ids[]`
* **Инвариант схемы:** `SAME_ENTITY ⇒ confidence=HIGH`; `POSSIBLE_ENTITY ⇒ MEDIUM|LOW`;
  `UNKNOWN ⇒ UNKNOWN` (см. `allOf` в [entity_links.schema.json](backend/app/services/stage_comparison/unified_entity_bridge/entity_links.schema.json)
  и код [entity_bridge.py:998-1005](backend/app/services/stage_comparison/unified_entity_bridge/entity_bridge.py#L998)).
* **Продовая обвязка:** `entity_links_path()` [paths.py:104](backend/app/services/stage_comparison/paths.py#L104) —
  **объявлена и не используется**.

### 1.6 SYSTEM_GRAPH comparison result (G2.3 / G2.3.1)

* **Реальные файлы:** `experiments/g2_system_graph_comparator/comparison_result.json`,
  копия `experiments/g2_4_4_scope_side_coverage/ios/comparison_result.json`
* **Продюсер:** [backend/app/pipeline/stages/block_grounding/system_graph_comparator.py](backend/app/pipeline/stages/block_grounding/system_graph_comparator.py)
  (`compare_system_graphs`, `validate_comparison_result`), матчер
  [graph_identity_matcher.py](backend/app/pipeline/stages/block_grounding/graph_identity_matcher.py),
  политика [system_graph_comparison_policy.py](backend/app/pipeline/stages/block_grounding/system_graph_comparison_policy.py)
* **Версия схемы:** ЕСТЬ — `schema_version: "system-graph-comparison.v1"`
* **Верхний уровень:** `schema_version, status, left_graph, right_graph, backbone,
  functional_groups, matching, comparison_quality, changes[], summary, provenance, validation`
* **`changes[]`:** `change_id, type, level, subject, summary, confidence (число 0..1),
  left_nodes[], right_nodes[], evidence` — **поля `certainty` НЕТ** (в данных `None`).
* **`matching`:** `matcher_version, matches[], medium_matches[], unmatched_left[], unmatched_right[],
  ambiguous[], ambiguous_left_ids[], ambiguous_right_ids[], relation_conflicts[], metrics{}, policy{}, detail_matches[]`
* **Вход:** `experiments/g2_dense_sectioned_board/{left,right}_system_graph.json`
  (`system-graph.v1`, profile `dense_sectioned_board`; LEFT 82 узла/111 рёбер, RIGHT 73/99;
  LEFT `rotation=0`, RIGHT `rotation=270`).

### 1.7 GraphicChangeLedger v2 (G2.4 / G2.4.1)

* **Реальный файл:** `experiments/g2_4_4_scope_side_coverage/ios/graphic_change_ledger.json`
* **Продюсер:** [graphic_comparison/graphic_change_ledger_adapter.py](backend/app/services/stage_comparison/graphic_comparison/graphic_change_ledger_adapter.py)
  (`adapt_system_graph_comparison_to_ledger`), валидатор
  [graphic_comparison/contract.py](backend/app/services/stage_comparison/graphic_comparison/contract.py)
* **Версия схемы:** ЕСТЬ — `schema_version: "graphic-change-ledger.v2"`
* **Верхний уровень:** `schema_version, comparison_scope{left_blocks,right_blocks},
  route ("MODE_2_REQUIRED"), mode ("MODE_2"), policy{adapter,confidence_mapping}, quality{}, changes[], diagnostics{}`
  — **поля `kind` НЕТ** (важно, см. Часть 5, п. 4).
* **`changes[]`:** `change_id, mode, type, summary, raw_confidence (0..1), mapped_confidence,
  left_region, right_region, evidence[], address_hints[], confidence, provenance[], structural{}`
* **`structural`:** `level (SYSTEM|GROUP|NODE|EDGE), source_level (A|B|C), subject{},
  left_nodes[], right_nodes[], left_edges[], right_edges[], relation{}`,
  плюс `equivalence: "representation_expansion"` — **только** для `DETAIL_LEVEL_INCREASED`
  (схема запрещает это поле для всех прочих типов).
* **Confidence-политика:** `system-graph-ledger-confidence-v1`, HIGH ≥ 0.85, MEDIUM ≥ 0.60
  ([confidence_policy.py](backend/app/services/stage_comparison/graphic_comparison/confidence_policy.py)).
* **Продовая обвязка:** `graphic_change_ledger_path()` [paths.py:92](backend/app/services/stage_comparison/paths.py#L92),
  `store.get_graphic_change_ledger_state`, GET
  [stage_comparison.py:306](backend/app/api/routers/stage_comparison.py#L306). На диске в `comparison/**` — нет.

### 1.8 graphic-coverage (G2.4.4 / G2.4.4.1) — **фактически `graphic-coverage.v2`, не v1**

* **Реальные файлы:** `experiments/g2_4_4_scope_side_coverage/{ios,ar}/graphic_coverage.json`
* **Продюсер:** [unified_entity_bridge/graphic_coverage.py](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py)
  (`build_graphic_coverage`, `coverage`, `validate_graphic_coverage`,
  `graphic_coverage_is_stale`, `saved_coverage_bundle_is_stale`), политика
  [graphic_coverage_policy.py](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage_policy.py)
* **Версия схемы:** ЕСТЬ — `schema_version: "graphic-coverage.v2"`,
  `builder_version: "graphic-coverage-builder-v2"`, `coverage_policy.version: "graphic-coverage-policy-v2"`
* **Верхний уровень:** `schema_version, kind, builder_version, coverage_policy, versions{scope_join,side_bridge},
  source_signature, source_artifacts{}, scope_processing[], coverage[], summary{}`
* **`coverage[]` (семантические записи):** `coverage_id ("coverage_<20hex>"), scope_ref,
  subject{kind (TEXT_ENTITY|GRAPH_ENTITY), id}, dimension, side (LEFT|RIGHT|BOTH),
  state (CHECKED|NOT_CHECKED|CHECK_BLOCKED|NOT_APPLICABLE), reason_codes[], source_refs{}`
* **`scope_processing[]` (технические записи о запуске маршрута):** `scope_ref, dimension,
  processing_state (SCOPE_PROCESSED|SCOPE_CHECK_BLOCKED|SCOPE_NOT_PROCESSED|SCOPE_NOT_APPLICABLE),
  reason_codes[], source_refs{}` — **это НЕ evidence по subject**.
* **`source_refs`:** `block_scope_refs[], block_pair_refs[], ledger_digests[],
  comparison_digests[], graph_node_ids[], entity_link_ids[]`
* **Продовая обвязка: ABSENT.** `grep -rn "graphic_coverage\|scope_join\|side_graph_entities\|side_entity_links"
  backend/app --include=*.py` вне каталога `unified_entity_bridge/` не даёт **ни одного** совпадения:
  нет path-хелпера, нет store-геттера, нет роутера. Единственный производитель —
  CLI [scripts/run_g2_4_4_scope_side_coverage.py](scripts/run_g2_4_4_scope_side_coverage.py).

### 1.9 Дополнительно: scope join (нужен G2.4.5, в исходном ТЗ не перечислен)

* **Файлы:** `experiments/g2_4_4_scope_side_coverage/{ios,ar}/scope_join.json`
* **Продюсер:** [unified_entity_bridge/comparison_scope.py](backend/app/services/stage_comparison/unified_entity_bridge/comparison_scope.py)
* **Версия:** `schema_version: "text-graphic-scope-join.v1"`, `scope_join_version: "explicit-page-base-scope-join-v1"`
* **Верхний уровень:** `schema_version, kind, scope_join_version, page_convention{},
  versions{entity_bridge,side_bridge}, source_signature, source_artifacts{}, scopes[], diagnostics{}`
* **`scopes[]`:** `scope_ref ("scope_<20hex>"), scope_level ("SHEET"),
  status (RESOLVED|UNRESOLVED_SCOPE), reason_codes[], text_scope{}, graphic_scope_group{}, child_block_scopes[]`
* **`text_scope`:** `sheet_group_id, left{pdf_pages_1based,canonical_page_indexes_0based},
  right{...}, evidence_ids[], source_change_ids[], text_entity_ids[], pair_review_required, source_link_uncertain`
* **Конвенция страниц:** `pdf-page-1based-to-index-0based-v2`, TEXT = `pdf_page_1based`,
  GRAPHIC = `page_index_0based`, канон = `page_index_0based`.

---

## ЧАСТЬ 2. Фактические enum-значения (по данным, не по документации)

### A. Типы TEXT high-level change

**Объявлено в коде** ([high_level_project_changes.py:35-47](backend/app/services/stage_comparison/high_level_project_changes.py#L35)),
`HIGH_LEVEL_TYPES`, 11 значений:
`DESIGN_PRINCIPLE_CHANGED, SYSTEM_OPERATION_CHANGED, SYSTEM_STRUCTURE_CHANGED,
SPACE_PROGRAM_CHANGED, CALCULATION_APPROACH_CHANGED, PARAMETER_SET_CHANGED,
EQUIPMENT_OR_MATERIAL_CHANGED, QUANTITY_OR_CAPACITY_CHANGED, DETAIL_LEVEL_INCREASED,
NO_HIGH_LEVEL_CHANGE, UNRESOLVED_HIGH_LEVEL_CHANGE`.

**Встречается в данных** (бакеты `high_level_changes`, `detail_level_increased`,
`material_review`, `non_material_review`, `service_structure_summary.items`;
`unresolved` исключён как дубль `material_review`):

| type | ИОС/ГРЩ `p26c08b83a6` | АР-1 `p570d156f57` | АР-2 `p16b108b9f5` | Итого |
|---|---:|---:|---:|---:|
| `NO_HIGH_LEVEL_CHANGE` | 7 (4 non-material + 3 service) | 26 (20 + 6) | 9 (6 + 3) | **42** |
| `UNRESOLVED_HIGH_LEVEL_CHANGE` | 4 | 3 | 15 | **22** |
| `PARAMETER_SET_CHANGED` | 0 | 1 | 1 | **2** |
| `SPACE_PROGRAM_CHANGED` | 0 | 2 | 0 | **2** |
| `DETAIL_LEVEL_INCREASED` | 0 | 1 | 0 | **1** |
| `DESIGN_PRINCIPLE_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |
| `SYSTEM_OPERATION_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |
| `SYSTEM_STRUCTURE_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |
| `CALCULATION_APPROACH_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |
| `EQUIPMENT_OR_MATERIAL_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |
| `QUANTITY_OR_CAPACITY_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |

> **Ключевой факт для G2.4.5:** в финальных бакетах реально существуют только 5 из 11
> типов, и всего **5 записей** несут материальное изменение (`PARAMETER_SET_CHANGED` ×2,
> `SPACE_PROGRAM_CHANGED` ×2, `DETAIL_LEVEL_INCREASED` ×1). Остальные 64 — `REVIEW_REQUIRED`.

**Но:** промежуточный `semantic_groups[].candidate_type` использует более широкий словарь
(до фильтрации/деградации в `UNRESOLVED_*`/`NO_*`):

| candidate_type | ИОС | АР-1 | АР-2 | Итого |
|---|---:|---:|---:|---:|
| `DESIGN_PRINCIPLE_CHANGED` | 4 | 8 | 13 | **25** |
| `PARAMETER_SET_CHANGED` | 2 | 13 | 11 | **26** |
| `EQUIPMENT_OR_MATERIAL_CHANGED` | 3 | 8 | 1 | **12** |
| `SPACE_PROGRAM_CHANGED` | 0 | 3 | 0 | **3** |
| `QUANTITY_OR_CAPACITY_CHANGED` | 2 | 0 | 0 | **2** |
| `SYSTEM_STRUCTURE_CHANGED` | 0 | 1 | 0 | **1** |
| `SYSTEM_OPERATION_CHANGED` | 0 | 1 | 0 | **1** |
| `CALCULATION_APPROACH_CHANGED` | 0 | 0 | 0 | **0** → DECLARED_BUT_UNUSED |

Сопутствующие фактические значения:

* `status`: `REVIEW_REQUIRED` = 64, `CONFIRMED` = 5.
* `confidence` (строчными!): `medium` = 67, `high` = 2. `low` — DECLARED? в коде не объявлен явный enum; в данных отсутствует.
* `decision_source`: `DETERMINISTIC` = 64, `AI` = 3, `MIXED` = 1, `FAIL_CLOSED` = 1.
* `semantic_groups[].route`: `SERVICE`, `NON_MATERIAL`, `MATERIAL_REVIEW`, `CONFIRMED`, `AI_REVIEW`.
* `semantic_groups[].decision` (= `AI_DECISIONS` + служебные): `NO_HIGH_LEVEL_CHANGE`,
  `INSUFFICIENT_CONTEXT`, `REAL_CHANGE`, `DETAIL_ONLY`.
* `route_reason` (реальные): `TWO_SIDED_TEXT_PROOF, INSUFFICIENT_CONTEXT, SOURCE_LINK_UNCERTAIN,
  CROSS_SHEET_COUNTERPART, NO_PROJECT_DECISION_SIGNAL, NO_SEMANTIC_CHANGE, SERVICE_ONLY_REVIEW,
  SERVICE_STRUCTURE, LOW_VALUE_UNCERTAIN, DETAIL_VS_REAL_CHANGE, DESIGNATION_ONLY`.
* `details[].source_status`: `ADDED, REMOVED, CHANGED, UNCERTAIN`.

Команда воспроизведения — см. Приложение A.1.

### B. Типы GRAPHIC change

**Объявлено в коде** ([contract.py:31-41](backend/app/services/stage_comparison/graphic_comparison/contract.py#L31)),
`STRUCTURAL_CHANGE_TYPES` (MODE_2), 9 значений; и `CHANGE_TYPES` (MODE_1), 4 значения.

**MODE_2, встречается в данных** (`experiments/g2_4_4_scope_side_coverage/ios/graphic_change_ledger.json`,
он же `comparison_result.json`; всего 6 изменений):

| type | Кол-во | change_id | confidence / raw |
|---|---:|---|---|
| `DETAIL_LEVEL_INCREASED` | 2 | `chg_5d6e7ac75c0d`, `chg_da7da89fba3c` | HIGH / 0.94 |
| `UNCERTAIN_STRUCTURAL_CHANGE` | 2 | `chg_66542577620e`, `chg_84afcbe773c3` | LOW / 0.35, 0.49 |
| `GROUP_COUNT_CHANGED` | 1 | `chg_4aafe649bd59` | HIGH / 0.867 |
| `NODE_TYPE_CHANGED` | 1 | `chg_1b601fa171f2` | HIGH / 0.92 |
| `SYSTEM_BACKBONE_CHANGED` | 0 | — | DECLARED_BUT_UNUSED |
| `FUNCTIONAL_GROUP_CHANGED` | 0 | — | DECLARED_BUT_UNUSED* |
| `NODE_ADDED` | 0 | — | DECLARED_BUT_UNUSED |
| `NODE_REMOVED` | 0 | — | DECLARED_BUT_UNUSED |
| `CONNECTION_CHANGED` | 0 | — | DECLARED_BUT_UNUSED* |

\* `FUNCTIONAL_GROUP_CHANGED` (1) и `CONNECTION_CHANGED` (1) встречаются **только** в
устаревшем исследовательском файле
`experiments/g2_vectograf_system_graph_research/artifacts/grsh_mode2_ledger.json`
(10 изменений), который **не проходит текущий контракт**:
`validate_ledger(...)` → `LedgerValidationError: invalid mode` (у него
`schema_version="graphic-change-ledger.v1"` при `mode="MODE_2"`, а v1-схема разрешает
`mode ∈ {MODE_1, null}`). Считать его продовым корпусом нельзя.

**MODE_1** (`ADDED_GRAPHIC, REMOVED_GRAPHIC, GEOMETRY_CHANGED, UNCERTAIN_GRAPHIC_CHANGE`):
**NOT PRESENT IN REAL DATA** — ни одного `graphic-change-ledger.v1` файла с `mode: "MODE_1"`
в репозитории нет.

Прочие фактические значения MODE_2:
`structural.level`: `SYSTEM` ×2, `GROUP` ×2, `NODE` ×2 (`EDGE` — DECLARED_BUT_UNUSED);
`structural.source_level`: `A` ×2, `B` ×1, `C` ×3;
`provenance`: `["VECTOR"]` ×6 (`VISION`, `BOTH` — DECLARED_BUT_UNUSED);
`mapped_confidence` = `confidence`: `HIGH` ×4, `LOW` ×2 (`MEDIUM` — DECLARED_BUT_UNUSED);
`structural.subject.kind`: `source_path`, `reserve_function`, `repeated_node_group`,
`individual_node`, `unresolved_correspondence`.

### C. Значения coverage

Enum: `CHECKED | NOT_CHECKED | CHECK_BLOCKED | NOT_APPLICABLE`
([graphic_coverage.py:40](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L40)).

#### ИОС/ГРЩ — `experiments/g2_4_4_scope_side_coverage/ios/graphic_coverage.json`
Всего 2856 семантических записей; `summary.by_state` =
`{CHECKED: 76, NOT_CHECKED: 995, CHECK_BLOCKED: 0, NOT_APPLICABLE: 1785}`;
`by_subject_kind` = `{GRAPH_ENTITY: 864, TEXT_ENTITY: 1992}`.

**GRAPH_ENTITY** (LEFT 56 сущностей, RIGHT 52; BOTH-записей нет вовсе):

| dimension | LEFT CHECKED | LEFT NOT_CHECKED | LEFT N/A | RIGHT CHECKED | RIGHT NOT_CHECKED | RIGHT N/A |
|---|---:|---:|---:|---:|---:|---:|
| CONNECTION | 12 | 44 | 0 | 12 | 40 | 0 |
| STRUCTURE | 12 | 44 | 0 | 12 | 40 | 0 |
| TYPE | 14 | 42 | 0 | 14 | 38 | 0 |
| QUANTITY | 0 | 0 | 56 | 0 | 0 | 52 |
| PARAMETER / METHOD / PRINCIPLE / SPACE | 0 | 0 | 56 каждое | 0 | 0 | 52 каждое |

**TEXT_ENTITY** (19 сущностей × 5 scope-ов; по 83 записи на «сторону-измерение»):

| dimension | LEFT | RIGHT | BOTH |
|---|---|---|---|
| CONNECTION | NOT_CHECKED 83 | NOT_CHECKED 83 | NOT_CHECKED 83 |
| STRUCTURE | NOT_CHECKED 83 | NOT_CHECKED 83 | NOT_CHECKED 83 |
| TYPE | NOT_CHECKED 83 | NOT_CHECKED 83 | NOT_CHECKED 83 |
| QUANTITY | N/A 83 | N/A 83 | N/A 83 |
| PARAMETER/METHOD/PRINCIPLE/SPACE | N/A 83 каждое | N/A 83 каждое | N/A 83 каждое |

> **CHECKED для TEXT_ENTITY = 0 во всех реальных данных.** Все 76 `CHECKED` —
> исключительно `GRAPH_ENTITY`.

#### АР-1 — `experiments/g2_4_4_scope_side_coverage/ar/graphic_coverage.json`
1632 записи, `by_state` = `{CHECKED: 0, NOT_CHECKED: 612, CHECK_BLOCKED: 0, NOT_APPLICABLE: 1020}`,
`by_subject_kind` = `{GRAPH_ENTITY: 0, TEXT_ENTITY: 1632}`.
CONNECTION/STRUCTURE/TYPE = `NOT_CHECKED` 68 на каждую сторону (LEFT/RIGHT/BOTH),
остальные 5 измерений = `NOT_APPLICABLE` 68 на сторону.

#### `CHECK_BLOCKED`
**0 записей в обоих реальных наборах** → DECLARED_BUT_UNUSED в реальных данных
(в коде порождается, покрыт только синтетическими тестами, см.
`G2_4_4_1_COVERAGE_HARDENING_REPORT.md`, «Negative tests», случаи 3–5, 7).

#### Реальные reason_codes coverage (ИОС)
`dimension_not_observable_by_system_graph_mode2` 1096, `dimension_not_observable_on_either_side` 415,
`scope_join_unresolved` 402, `quantity_not_observable_for_individual_entity` 274,
`subject_not_reliably_checked_on_both_sides` 249, `comparator_identity_ambiguous_for_graph_entity` 162,
`graph_entity_not_fully_high_matched_by_comparator` 78, `no_high_side_entity_link` 66,
`graph_entity_and_all_external_neighbours_high_matched` 48,
`all_graph_entity_nodes_high_matched_by_comparator` 28, `side_entity_link_ambiguous` 27,
`NEIGHBOUR_IDENTITY_UNRESOLVED` 8, `linked_graph_entity_not_reliably_covered` 3.

> ⚠ Код `NEIGHBOUR_IDENTITY_UNRESOLVED` — единственный в UPPER_SNAKE; все остальные — lower_snake.
> Это несогласованность стиля в едином поле.

#### `scope_processing` (технический слой, НЕ evidence)
ИОС: `SCOPE_PROCESSED` 4 (CONNECTION/STRUCTURE/TYPE/QUANTITY на единственном resolved scope),
`SCOPE_NOT_PROCESSED` 16, `SCOPE_NOT_APPLICABLE` 20. `SCOPE_CHECK_BLOCKED` = 0.
АР-1: `SCOPE_NOT_PROCESSED` 32, `SCOPE_NOT_APPLICABLE` 32, остальные 0.

### D. Значения entity link strength

Enum relation: `SAME_ENTITY | POSSIBLE_ENTITY | UNKNOWN`
([entity_bridge.py:34](backend/app/services/stage_comparison/unified_entity_bridge/entity_bridge.py#L34)).
Enum confidence: `HIGH | MEDIUM | LOW | UNKNOWN`.

| Набор | Сторона | links | SAME_ENTITY | POSSIBLE_ENTITY | UNKNOWN | HIGH | MEDIUM | LOW | UNKNOWN(conf) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ИОС `side_entity_links.json` | LEFT | 18 | 0 | 0 | 18 | 0 | 0 | 0 | 18 |
| ИОС `side_entity_links.json` | RIGHT | 10 | **1** | 0 | 9 | **1** | 0 | 0 | 9 |
| АР-1 `side_entity_links.json` | LEFT | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| АР-1 `side_entity_links.json` | RIGHT | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| ИОС `g2_4_3/entity_links.json` (не side) | — | 10 | 1 | 0 | 9 | 1 | 0 | 0 | 9 |
| АР-1 `g2_4_3/entity_links.json` | — | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

* **`POSSIBLE_ENTITY` — DECLARED_BUT_UNUSED** (0 во всех реальных наборах).
  Как следствие, `confidence ∈ {MEDIUM, LOW}` — тоже DECLARED_BUT_UNUSED.
* Единственная реальная связь `SAME_ENTITY/HIGH`: `eln_f4b0052318e9158bf477`,
  RIGHT, `txt_ent_77c26be9e51798a0ed1c` (`VRU_1`, «ВРУ-1») ↔ `gfx_ent_77ec87caaebd3911498f`
  (`VRU_1`, узлы `LOAD:1QF6@764`, `OUT:1QF6@764`).
* «HIGH» как **strength** самой связи бывает только вместе с `SAME_ENTITY` —
  это инвариант схемы, а не отдельное значение.

**Правила evidence в bridge:** объявлены `EXACT_CANONICAL_IDENTITY_MATCH` (level 1),
`NORMALIZED_DESIGNATION_MATCH` (2), `DESIGNATION_CONTEXT_MATCH` (3),
`FUNCTIONAL_ROLE_MATCH` (4), `AMBIGUOUS_CARDINALITY` (0), `CANONICAL_IDENTITY_CONFLICT` (0),
`GRAPHIC_IDENTITY_CONFLICT`, `ENTITY_TYPE_CONFLICT`, `CONTEXT_CONFLICT`,
`LOCAL_DESIGNATION_REQUIRES_CONTEXT`, `GRAPH_ENTITY_IDENTITY_UNCERTAIN`.

Встречаются в данных (ИОС): `EXACT_CANONICAL_IDENTITY_MATCH` 20 (10 LEFT + 10 RIGHT),
`AMBIGUOUS_CARDINALITY` 27 (18 + 9), `CANONICAL_IDENTITY_CONFLICT` 8 (LEFT).
Остальные **8 правил — DECLARED_BUT_UNUSED**.

**Побочный контракт `query_text_entity_side(...)`**
([side_entity_contract.py:381](backend/app/services/stage_comparison/unified_entity_bridge/side_entity_contract.py#L381)):
`presence ∈ {PRESENT, ABSENT, UNKNOWN}`, `match ∈ {HIGH, MEDIUM, UNKNOWN, NOT_MATCHED}`.
Реально встречается (19 сущностей ИОС + 27 АР-1, обе стороны):
presence `PRESENT`/`ABSENT`/`UNKNOWN` — все три; match — `HIGH` (1 раз), `UNKNOWN`, `NOT_MATCHED`.
**`match = MEDIUM` — DECLARED_BUT_UNUSED** (следствие отсутствия `POSSIBLE_ENTITY`).

---

## ЧАСТЬ 3. Предлагаемые таблицы перевода в dimension

Целевые оси: `PRINCIPLE, METHOD, OPERATION, STRUCTURE, CONNECTION, TYPE, PARAMETER, QUANTITY, SPACE`.
`DETAIL` — **не** ось, а outcome.

> ⚠ Расхождение с существующим кодом: `graphic_coverage_policy.DIMENSIONS` содержит
> **8** осей — `STRUCTURE, CONNECTION, TYPE, QUANTITY, PARAMETER, METHOD, PRINCIPLE, SPACE`.
> **`OPERATION` в текущем коде отсутствует.** Все таблицы ниже используют целевые 9 осей;
> при внедрении надо либо расширить `graphic-coverage-policy` до v3, либо явно
> отобразить `OPERATION` на существующее.

### 3.1 TEXT change type → dimension

| TEXT type | dimension | Обоснование |
|---|---|---|
| `DESIGN_PRINCIPLE_CHANGED` | `PRINCIPLE` | Тип по определению фиксирует смену проектного принципа. |
| `CALCULATION_APPROACH_CHANGED` | `METHOD` | Меняется способ расчёта, а не сам объект. |
| `SYSTEM_OPERATION_CHANGED` | `OPERATION` | Меняется режим/логика работы системы при неизменном составе. |
| `SYSTEM_STRUCTURE_CHANGED` | `STRUCTURE` **или** `CONNECTION` | Тип покрывает и состав, и связи; ось определяется содержимым `details` — см. ниже. |
| `SPACE_PROGRAM_CHANGED` | `SPACE` | Программа и состав помещений — пространственная ось. |
| `PARAMETER_SET_CHANGED` | `PARAMETER` **или** `QUANTITY` | «Набор параметров» включает и площади/толщины (PARAMETER), и счётные величины (QUANTITY) — см. ниже. |
| `EQUIPMENT_OR_MATERIAL_CHANGED` | `TYPE` | Смена типа оборудования/материала при сохранении функции. |
| `QUANTITY_OR_CAPACITY_CHANGED` | `QUANTITY` | Количество и мощность — счётная ось. |
| `DETAIL_LEVEL_INCREASED` | `UNKNOWN_DIMENSION` (outcome `DETAIL`) | См. п. 3.3: тип не называет ось, а называет характер результата. |
| `NO_HIGH_LEVEL_CHANGE` | `UNKNOWN_DIMENSION` | Это отсутствие утверждения об изменении, а не изменение по какой-то оси. |
| `UNRESOLVED_HIGH_LEVEL_CHANGE` | `UNKNOWN_DIMENSION` | Fail-closed: ось нельзя назвать, пока не доказано само изменение. |

**Многоосевые типы — реальные примеры:**

* `PARAMETER_SET_CHANGED` → `PARAMETER` **и** `QUANTITY` одновременно.
  `hlc_2cece3c61281e7f4` (АР-1, `comparison/sessions/121d764109184c13/pairs/p570d156f57/high_level_project_changes.json`)
  содержит и `ev_2168b0cfc9061d3b` («Помещение 7.К.4 «Кладовая» площадью 6,50 отсутствует справа» →
  ось SPACE/PARAMETER), и `ev_1547e83347f37515` («Изменены количество и площадь блока кладовых:
  28 и 150,80 м² против 12 и 64,60 м²» → QUANTITY + PARAMETER в одном атоме).
  Одна `hlc`-запись — минимум три оси.
* `SYSTEM_STRUCTURE_CHANGED` в финальных бакетах отсутствует; кандидатная форма — 1 шт.
  в АР-1 `semantic_groups`. Реального примера расщепления STRUCTURE/CONNECTION в данных нет →
  **NOT PRESENT IN REAL DATA**, правило остаётся гипотезой.

**Вывод для G2.4.5:** ось нельзя брать из `type` — её надо выводить из `details[]`
(`before`/`after`/`summary`) детерминированно, либо честно ставить `UNKNOWN_DIMENSION`.
Тип `PARAMETER_SET_CHANGED` — доказанно многоосевой на реальных данных.

### 3.2 GRAPHIC change type → dimension

| GRAPHIC type (MODE_2) | dimension | Обоснование |
|---|---|---|
| `NODE_TYPE_CHANGED` | `TYPE` | Сопоставленный узел сменил тип при сохранённой идентичности. |
| `CONNECTION_CHANGED` | `CONNECTION` | Изменение на уровне ребра (`structural.level = EDGE`). |
| `GROUP_COUNT_CHANGED` | `QUANTITY` | Меняется размер повторяющейся группы, состав элементов не утверждается. |
| `FUNCTIONAL_GROUP_CHANGED` | `STRUCTURE` | Меняется состав/наличие функциональной группы (`level = GROUP`). |
| `NODE_ADDED` | `STRUCTURE` | Появление узла — изменение состава подсистемы. |
| `NODE_REMOVED` | `STRUCTURE` | Исчезновение узла — изменение состава подсистемы. |
| `SYSTEM_BACKBONE_CHANGED` | `STRUCTURE` **или** `CONNECTION` | `level = SYSTEM` покрывает и состав секций, и магистральные связи; в данных отсутствует, разделить нечем. |
| `DETAIL_LEVEL_INCREASED` | `UNKNOWN_DIMENSION` (outcome `DETAIL`) | См. п. 3.3. |
| `UNCERTAIN_STRUCTURAL_CHANGE` | `UNKNOWN_DIMENSION` | Комаратор явно отказался утверждать изменение; назначать ось — значит придумывать факт. |
| MODE_1: `ADDED_GRAPHIC`, `REMOVED_GRAPHIC`, `GEOMETRY_CHANGED`, `UNCERTAIN_GRAPHIC_CHANGE` | `UNKNOWN_DIMENSION` | MODE_1 — локальная растровая/векторная дельта без семантики; `graphic_coverage_policy` явно объявляет для MODE_1 `not_applicable` по всем осям. |

**Многоосевой пример из реальных данных:** `chg_4aafe649bd59` (`GROUP_COUNT_CHANGED`, 30 → 27,
`subject.kind = repeated_node_group`, `node_type = OUTGOING_DEVICE`). По оси это `QUANTITY`,
но множество `left_nodes` содержит **все** узлы `OUT:*`, включая узлы, из которых собраны
GRAPH_ENTITY `VRU_1..VRU_A` (проверено сопоставлением
`graphic_change_ledger.json → changes[].structural.left_nodes` и
`side_graph_entities.json → sides.LEFT.entities[].graph_node_ids`).
То есть одно `QUANTITY`-событие адресует 30 сущностей, часть из которых у TEXT проходит по
другим осям. Это тот самый случай «1 TEXT → N GRAPHIC» / «N GRAPHIC → 1 subject».

### 3.3 `DETAIL_LEVEL_INCREASED` — отдельно

**Предложение:**

* **dimension = `UNKNOWN_DIMENSION`** (ось не изменилась — это ключевое утверждение типа).
* **outcome = `DETAIL`** (характер результата: изменилось представление, не решение).

**Обоснование по коду и данным:**

1. GRAPHIC-контракт прямо это кодирует: только для `DETAIL_LEVEL_INCREASED` обязательно
   `structural.equivalence = "representation_expansion"`, и схема **запрещает** это поле
   всем остальным типам ([graphic_change_ledger_v2.schema.json](backend/app/services/stage_comparison/graphic_comparison/graphic_change_ledger_v2.schema.json),
   последний `allOf`/`else`).
2. Реальный `relation` у `chg_5d6e7ac75c0d`: `classification = coarse_node_to_expanded_subgraph`,
   `boundary_preserved = true`, `relations_preserved = true`, `not_node_added = true`.
   Границы и связи **сохранены** — значит ни STRUCTURE, ни CONNECTION не менялись.
3. TEXT-сторона делает то же: `hlc_78243ba30fcf16c1` (АР-1) — «Добавлена ведомость материалов
   кладочных стен», reason: «Подтверждено добавление ведомости, но не изменение состава
   материалов или проектного решения», и Stage 5.3 выносит её в **отдельный бакет**
   `detail_level_increased`, а не в `high_level_changes`.
4. Следствие для merge: `DETAIL` нельзя объединять с `MATERIAL` (gate **M6**), но нельзя и
   трактовать как противоречие — это ортогональная величина.

---

## ЧАСТЬ 4. Корпус реальных случаев

Обозначения:
`SCOPE-IOS` = `scope_7cbca080da4885fed88e` (sheet group `link_3b1c7c47e1ab`, единственный `RESOLVED`
в ИОС; LEFT pdf p.1 → canonical 0, RIGHT pdf p.1,3 → canonical 0,2; child block pair
`block_pair_ba0e3fbd544790666a03`: LEFT `blk_039909ec…` p0, RIGHT `blk_2d72a670…` p0).
Все ссылки на TEXT — `comparison/sessions/121d764109184c13/pairs/<pair_id>/high_level_project_changes.json`.
Все ссылки на GRAPHIC — `experiments/g2_4_4_scope_side_coverage/ios/graphic_change_ledger.json`.
Coverage — `experiments/g2_4_4_scope_side_coverage/{ios,ar}/graphic_coverage.json`.

### Блок A. ИОС/ГРЩ — обе стороны присутствуют

---

**A1. Одна сущность, разные аспекты (обязательный случай ✔)**

1. TEXT: `hlc_d3913c0ab7b930d2` / `ev_e49035bd602be7cb` (бакет `service_structure_summary.items`, `p26c08b83a6`).
   GRAPHIC: `chg_4aafe649bd59` `GROUP_COUNT_CHANGED`.
2. TEXT: «Схема ВРУ-1 разделена на начало и конец» (`before`: «…Однолинейная расчетная схема ВРУ-1 22-23»,
   `after`: «л.2 Однолинейная схема ВРУ-1 (начало) / л.3 … (конец)») — изменилась **подача листа**.
3. GRAPHIC: количество элементов повторяющейся группы `OUTGOING_DEVICE` 30 → 27 — изменилось **количество отходящих**.
4. Entity link: `eln_f4b0052318e9158bf477`, RIGHT, `SAME_ENTITY / HIGH`
   (`txt_ent_77c26be9e51798a0ed1c` ↔ `gfx_ent_77ec87caaebd3911498f`); на LEFT — `UNKNOWN`
   (`eln_66b37aff8a225d6c2850`, `eln_ba33b6af8de77298ce8f`, причина `AMBIGUOUS_CARDINALITY`).
5. Scope: совпадает (обе записи внутри `SCOPE-IOS`).
6. Coverage `VRU_1`: LEFT `NOT_CHECKED / side_entity_link_ambiguous`;
   RIGHT `NOT_CHECKED / linked_graph_entity_not_reliably_covered` — по всем трём наблюдаемым осям.
7. **Вердикт: COMPLEMENTARY.**
8. Обоснование: один и тот же объект («ВРУ-1»), но TEXT говорит про DETAIL/представление, а GRAPHIC — про QUANTITY.
   Одна сущность ≠ одно изменение.
9. Решающий gate: **M4** (разные оси) — при пройденном M2.

---

**A2. Тот же GRAPHIC-факт, TEXT-подтверждения нет (1 GRAPHIC → 0 TEXT)**

1. GRAPHIC: `chg_1b601fa171f2` `NODE_TYPE_CHANGED`, subject `individual_node`,
   identity `SECTION-TIE#BUS1-BUS2`, `QF3 (CIRCUIT_BREAKER) → QS1 (SWITCH_DISCONNECTOR)`, HIGH / 0.92.
2. TEXT: ни одна TEXT-сущность и ни одна `hlc` не упоминает секционный аппарат — ABSENT.
3. GRAPHIC: тип секционного аппарата сменился с автомата на разъединитель.
4. Entity link: ABSENT (в `side_entity_links.json` нет ни одного кандидата на
   `gfx_ent_e1f1e558ed35602b6453` / `gfx_ent_c73162ed8f44aea4e754`).
5. Scope: `SCOPE-IOS` (GRAPHIC), TEXT-стороны нет.
6. Coverage: `SECTION_TIE_BUS_1_BUS_2` LEFT `TYPE=CHECKED`, RIGHT `TYPE=CHECKED`
   (reason `all_graph_entity_nodes_high_matched_by_comparator`); CONNECTION/STRUCTURE тоже `CHECKED`.
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: изменение доказано только графикой, но доказано хорошо (CHECKED на обеих сторонах,
   raw 0.92). Это самая ценная находка ИОС — и TEXT её не видит вообще.
9. Решающий gate: **M7** (источник валиден и реально проверял) — при отсутствии второго источника.

---

**A3. TEXT утверждает, GRAPHIC = NOT_CHECKED (обязательный случай ✔)**

1. TEXT: `hlc_af2dcf088860ad8f` `UNRESOLVED_HIGH_LEVEL_CHANGE` / `ev_b9db67e604bed43e`,
   sheet group `link_03d40f1e1f2e`.
2. TEXT: «Запись об изменении мощности на шинах ГРЩ ТП отсутствует справа»
   (`before`: «1.4 7 Изменение общей установленной и расчетной мощности на шинах ГРЩ ТП…»).
3. GRAPHIC: по этому листу графики нет — scope `scope_f1da8b0875a7c73e93ae` = `UNRESOLVED_SCOPE`
   (`no_graphic_scope_group_on_canonical_pages`).
4. Entity link: `txt_ent_c32229d4652a8f65854a` (`MSB_TП`, «ГРЩ ТП») — 8 кандидатов на LEFT,
   все `UNKNOWN`, evidence `CANONICAL_IDENTITY_CONFLICT` + `AMBIGUOUS_CARDINALITY`; на RIGHT кандидатов нет.
5. Scope: **UNRESOLVED_SCOPE**.
6. Coverage `MSB_TП` в `scope_f1da8b0875a7c73e93ae`: LEFT/RIGHT/BOTH `NOT_CHECKED / scope_join_unresolved`
   по CONNECTION/STRUCTURE/TYPE.
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: отсутствие доказательства ≠ доказательство отсутствия. Графика этот лист не смотрела,
   поэтому противоречия нет; TEXT остаётся единственным (и сам по себе `REVIEW_REQUIRED`).
9. Решающий gate: **M7** — сначала coverage, потом любая попытка сопоставления.

---

**A4. GRAPHIC = NOT_APPLICABLE, TEXT утверждает параметр (обязательный случай ✔)**

1. TEXT: `hlc_801f56494a5e3010` `UNRESOLVED_HIGH_LEVEL_CHANGE` / `ev_36b401845313b4bb`.
2. TEXT: «Запись об изменении суммарной нагрузки 2,3,4-комнатных квартир отсутствует справа»
   — ось `PARAMETER` (нагрузка).
3. GRAPHIC: ось `PARAMETER` объявлена ненаблюдаемой для MODE_2
   (`UNSUPPORTED_SEMANTIC_DIMENSIONS` в [graphic_coverage_policy.py](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage_policy.py)).
4. Entity link: ABSENT для этого TEXT-атома (сущность не выделена).
5. Scope: `scope_f1da8b0875a7c73e93ae` — UNRESOLVED_SCOPE.
6. Coverage: любая `PARAMETER`-запись = `NOT_APPLICABLE / dimension_not_observable_by_system_graph_mode2`
   (LEFT/RIGHT) и `dimension_not_observable_on_either_side` (BOTH). В ИОС таких 83×3 = 249 записей на PARAMETER.
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: `NOT_APPLICABLE` — это «графика в принципе не умеет такое видеть», а не «графика не согласна».
   Конфликта нет и быть не может.
9. Решающий gate: **M7** (coverage = N/A закрывает вопрос до M4/M5).

---

**A5. `CHECKED` только на одной стороне (обязательный случай ✔)**

1. GRAPHIC-subject: `SECTION_1` — LEFT `gfx_ent_8fe165e15c3330dcf031` (узел `BUS1`),
   RIGHT `gfx_ent_ee9954153d3878cee44c` (узел `BUS1`).
2. TEXT: ABSENT (текст не выделяет секции шин как сущность).
3. GRAPHIC: секция шин сопоставлена по типу, но её внешние соседи — нет.
4. Entity link: ABSENT.
5. Scope: `SCOPE-IOS`.
6. Coverage: **TYPE = `CHECKED`** на LEFT и на RIGHT
   (`all_graph_entity_nodes_high_matched_by_comparator`), но **CONNECTION = `NOT_CHECKED`**
   и **STRUCTURE = `NOT_CHECKED`** с `NEIGHBOUR_IDENTITY_UNRESOLVED` — на обеих сторонах.
   Всего таких записей 8 (`SECTION_1`/`SECTION_2` × LEFT/RIGHT × CONNECTION/STRUCTURE).
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: по одной оси субъект проверен, по двум другим — нет. Любое утверждение
   «связи секции не менялись» было бы ложным.
9. Решающий gate: **M7** — покрытие меряется **по оси**, а не по субъекту целиком.

> Случай «`CHECKED` ровно на одной из сторон LEFT/RIGHT, при `NOT_CHECKED` на другой» —
> **NOT PRESENT IN REAL DATA**: в ИОС все 76 `CHECKED` симметричны по сторонам
> (12/12, 12/12, 14/14 по CONNECTION/STRUCTURE/TYPE).
> Ближайший реальный аналог асимметрии — A6.

---

**A6. Асимметрия силы связи LEFT/RIGHT при одном TEXT-субъекте**

1. TEXT: `txt_ent_77c26be9e51798a0ed1c` (`VRU_1`), источник `hlc_d3913c0ab7b930d2`.
2. TEXT: «Схема ВРУ-1 разделена на начало и конец».
3. GRAPHIC: на RIGHT «ВРУ-1» однозначно опознан как один узел `OUT:1QF6@764`; на LEFT «ВРУ-1»
   присутствует дважды (`OUT:1QF1@434` и `OUT:1QF1@1736`).
4. Entity link: RIGHT `SAME_ENTITY/HIGH`; LEFT — два `UNKNOWN/UNKNOWN`.
5. Scope: совпадает (`SCOPE-IOS`).
6. Coverage `VRU_1`: LEFT `NOT_CHECKED / side_entity_link_ambiguous`;
   RIGHT `NOT_CHECKED / linked_graph_entity_not_reliably_covered`; BOTH `NOT_CHECKED`.
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: даже при `SAME_ENTITY/HIGH` на RIGHT сравнение невозможно — на LEFT идентичность
   не установлена. Это ровно та ситуация, ради которой существует M8.
9. Решающий gate: **M8** (cardinality safety, 1 TEXT → 2 GRAPHIC на LEFT).

---

**A7. Entity link = UNKNOWN из-за нескольких кандидатов (обязательный случай ✔)**

1. TEXT: `txt_ent_c32229d4652a8f65854a` (`MSB_TП`, «ГРЩ ТП»), источник `hlc_af2dcf088860ad8f`.
2. TEXT: изменение мощности на шинах ГРЩ ТП не подтверждено справа.
3. GRAPHIC: 8 узлов-кандидатов с именами `MSB_1_PП_111 / _211 / _114 / _214` (роли
   `OUTGOING_DEVICE` и `UNKNOWN_NODE`).
4. Entity link: 8 связей, все `UNKNOWN/UNKNOWN`; evidence — `CANONICAL_IDENTITY_CONFLICT`
   («ГРЩ ТП» vs «MSB_1_PП_111») **плюс** `AMBIGUOUS_CARDINALITY`
   (`direction = ONE_TEXT_TO_MULTIPLE_GRAPHIC`, 8 graphic ids).
5. Scope: TEXT в `scope_f1da8b0875a7c73e93ae` (UNRESOLVED), GRAPHIC в `SCOPE-IOS` → **не совпадает**.
6. Coverage `MSB_TП`: `NOT_CHECKED` по всем наблюдаемым осям на обеих сторонах.
7. **Вердикт: UNRELATED.**
8. Обоснование: имя конфликтует, кардинальность 1→8, области сравнения разные.
   Ни один из восьми кандидатов не может быть выбран без догадки.
9. Решающий gate: **M3** (для ENTITY допустимы только `SAME_ENTITY`/HIGH), с усилением M1 и M8.

---

**A8. TEXT MATERIAL + GRAPHIC DETAIL_LEVEL_INCREASED (обязательный случай — частично)**

1. TEXT: `hlc_434f8823f7f0b747` `UNRESOLVED_HIGH_LEVEL_CHANGE` / `ev_6ff5660f5421faad`
   (sheet group `link_8369ee399ae9`, LEFT p.31 → RIGHT p.29).
   GRAPHIC: `chg_5d6e7ac75c0d` `DETAIL_LEVEL_INCREASED` (HIGH 0.94).
2. TEXT: «Все перечисленные фрагменты присутствуют только в правой документации»
   (`after`: таблица «Автостоянка / Наименование потребителей…») — материальное добавление
   потребителей, но статус `REVIEW_REQUIRED` по `SOURCE_LINK_UNCERTAIN`.
3. GRAPHIC: «Источник показан подробнее без смены функционального пути:
   ТП1 (UPSTREAM_TP_CONNECTION) → Т1 (TRANSFORMER_EXPLICIT)», `equivalence = representation_expansion`.
4. Entity link: ABSENT между этим TEXT-атомом и узлами `SOURCE1/SOURCE1:PATH1/INPUT1`.
5. Scope: **не совпадает** — TEXT-scope `scope_0ed97f422ced03660181` = `UNRESOLVED_SCOPE`,
   GRAPHIC в `SCOPE-IOS`.
6. Coverage: TEXT-сущности этого листа (`MSB`, `VRU_A`) — `NOT_CHECKED / scope_join_unresolved`;
   GRAPHIC-сущности `SOURCE_PATH_BUS_1/2` — `CHECKED` по CONNECTION/STRUCTURE/TYPE на обеих сторонах.
7. **Вердикт: UNRELATED.**
8. Обоснование: это два разных листа. Пара «MATERIAL vs DETAIL» здесь не является конфликтом,
   потому что до M6 дело не доходит — M1 уже провален.
9. Решающий gate: **M1** (scope compatibility).

> Пары «TEXT MATERIAL и GRAPHIC DETAIL_LEVEL_INCREASED **в одном scope, про один субъект**»
> в реальных данных **NOT PRESENT IN REAL DATA**: единственный TEXT `DETAIL_LEVEL_INCREASED`
> (`hlc_78243ba30fcf16c1`) — в АР-1, где графики нет вовсе; а два GRAPHIC
> `DETAIL_LEVEL_INCREASED` — в ИОС, где TEXT про источники ничего не говорит.

---

**A9. Кандидат на конфликт, который конфликтом не является**

1. TEXT: `hlc_d3913c0ab7b930d2` / `ev_f9464ad3b067a5ba` (service-бакет).
   GRAPHIC: `chg_4aafe649bd59` `GROUP_COUNT_CHANGED` 30 → 27.
2. TEXT: «Схемы ЩР-1–ЩР-5 отсутствуют справа» (`source_status = REMOVED`).
3. GRAPHIC: число отходящих на щите уменьшилось на 3.
4. Entity link: для `PANEL_1..PANEL_5` — **ноль** кандидатов (`match = NOT_MATCHED` на обеих сторонах;
   `presence` LEFT = `PRESENT`, RIGHT = `ABSENT`).
5. Scope: TEXT-сущности `PANEL_*` входят в 5 sheet-групп, включая `SCOPE-IOS` → формально совпадает.
6. Coverage `PANEL_1` в `SCOPE-IOS`: LEFT/RIGHT `NOT_CHECKED / no_high_side_entity_link`,
   BOTH `NOT_CHECKED / subject_not_reliably_checked_on_both_sides`.
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: «−5 схем ЩР» и «−3 отходящих» соблазнительно связать, но чисел 5 и 3 не свести
   без домысла, а связи сущностей нет вовсе. Fail-closed → человек.
9. Решающий gate: **M2** (subject identity не доказана), затем M8.

---

**A10. Служебное изменение против графического — не связаны**

1. TEXT: `hlc_4ad3860c9d2504ea` `NO_HIGH_LEVEL_CHANGE` / `ev_8bd289a59355afae`
   («Отдельно добавлено указание города Москвы», группа `link_3b1c7c47e1ab` = `SCOPE-IOS`).
   GRAPHIC: `chg_1b601fa171f2` `NODE_TYPE_CHANGED`.
2. TEXT: в штамп добавлено «г. Москва».
3. GRAPHIC: секционный аппарат сменил тип.
4. Entity link: ABSENT.
5. Scope: **совпадает** (обе записи в `SCOPE-IOS`).
6. Coverage: у TEXT-субъекта нет — сущность из этого атома не порождена; GRAPHIC `CHECKED` по TYPE.
7. **Вердикт: UNRELATED.**
8. Обоснование: совпадение scope не создаёт связи. Это контрпример против «merge по scope».
9. Решающий gate: **M2**.

---

**A11. Неопределённое графическое, TEXT молчит**

1. GRAPHIC: `chg_66542577620e` `UNCERTAIN_STRUCTURAL_CHANGE`, subject `reserve_function`,
   `left_count = 2`, `right_count = 0`, LOW / 0.35,
   `absence_is_bounded_by_identity_coverage = 0.867`.
2. TEXT: ABSENT (о резервных отходящих ничего).
3. GRAPHIC: «Число распознанных резервных отходящих различается, но уверенности идентификации
   недостаточно для утверждения об изменении: 2 → 0».
4. Entity link: ABSENT.
5. Scope: `SCOPE-IOS`.
6. Coverage: узлы `OUT:1QF11@1294`, `OUT:2QF11@2597` входят в сущности с
   `NOT_CHECKED / comparator_identity_ambiguous_for_graph_entity`.
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: комаратор сам сказал «не утверждаю». Продвигать это в вывод нельзя ни как факт,
   ни как отсутствие факта.
9. Решающий gate: **M7**.

---

**A12. Неразрешённое соответствие узлов**

1. GRAPHIC: `chg_84afcbe773c3` `UNCERTAIN_STRUCTURAL_CHANGE`, subject `unresolved_correspondence`,
   LOW / 0.49, `relation.ambiguous_pairs` содержит `COMPENSATION_GROUP:BUS1` с кандидатом
   `MEDIUM_MATCH` (score 0.98, confidence 0.6).
2. TEXT: ABSENT.
3. GRAPHIC: «Для части узлов соответствие недостаточно надёжно; удаление или добавление не утверждается».
4. Entity link: ABSENT.
5. Scope: `SCOPE-IOS`.
6. Coverage: соответствующие сущности `NOT_CHECKED / comparator_identity_ambiguous_for_graph_entity`
   (162 записи в ИОС).
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: `MEDIUM_MATCH` по политике комаратора — `uncertainty_only`, он не используется
   для структурного сравнения. Синтезатор не имеет права поднять его до факта.
9. Решающий gate: **M7** (и M3, если бы связь TEXT существовала).

---

**A13. Идеально проверенный субъект без TEXT-события**

1. GRAPHIC-subject: `ХМ_1` — LEFT `gfx_ent_b76f52b70ae81b8e5bda` (`LOAD:1QF12@1388`, `OUT:1QF12@1388`),
   RIGHT `gfx_ent_c643a053f3b1bd342814` (`LOAD:1QF2@513`, `OUT:1QF2@513`).
2. TEXT: ABSENT (в `text_entities.json` для ИОС всего 19 сущностей: 17 PANEL/ВРУ + 2 MAIN_SWITCHBOARD; «ХМ» нет).
3. GRAPHIC: ни одно из 6 изменений не адресует эту сущность.
4. Entity link: ABSENT.
5. Scope: `SCOPE-IOS`.
6. Coverage: `CHECKED` по CONNECTION, STRUCTURE, TYPE на **обеих** сторонах;
   QUANTITY = `NOT_APPLICABLE / quantity_not_observable_for_individual_entity`.
7. **Вердикт: SINGLE_SOURCE** (с полезной нагрузкой «проверено, изменений нет»).
8. Обоснование: единственный класс случаев, где `CHECKED` + отсутствие change даёт настоящее
   «изменений не найдено». Их в ИОС 12 сущностей на сторону по CONNECTION/STRUCTURE.
9. Решающий gate: **M7** (только `CHECKED` позволяет утверждать отсутствие).

---

**A14. QUANTITY у индивидуальной сущности — навсегда N/A**

1. GRAPHIC: `chg_4aafe649bd59` `GROUP_COUNT_CHANGED` (QUANTITY, subject = группа).
2. TEXT: ABSENT для конкретной сущности.
3. GRAPHIC: количество группы изменилось.
4. Entity link: ABSENT.
5. Scope: `SCOPE-IOS`.
6. Coverage по QUANTITY для **любой** индивидуальной сущности (и TEXT, и GRAPH):
   `NOT_APPLICABLE / quantity_not_observable_for_individual_entity`
   — 108 GRAPH-записей + 249 TEXT-записей в ИОС.
7. **Вердикт: UNRELATED** (для пары «событие группы» ↔ «индивидуальная сущность»).
8. Обоснование: субъект `repeated_node_group` и субъект `individual entity` — разные объекты.
   G2.4.4.1 специально закрыл возможность приписать групповое количество отдельному элементу.
9. Решающий gate: **M2**.

---

**A15. `MSB` («ГРЩ») — TEXT видит три разных факта, GRAPHIC не связан**

1. TEXT: `txt_ent_8711b577ec2dd2cb8d14` (`MSB`), 3 атома:
   `ev_6b149f847c6443f7` («Добавлена однолинейная схема ГРЩ, лист 1», ADDED),
   `ev_af2f35ca1df8b79a` («Принципиальная схема ГРЩ отсутствует справа в таком виде», REMOVED),
   `ev_6ff5660f5421faad` (из `hlc_434f8823f7f0b747`).
2. TEXT: состав графических листов по ГРЩ переработан.
3. GRAPHIC: 6 изменений внутри схемы ГРЩ, но ни одно не про «наличие листа».
4. Entity link: ABSENT (`match = NOT_MATCHED` на обеих сторонах, `presence = PRESENT/PRESENT`).
5. Scope: TEXT принадлежит 5 группам, включая `SCOPE-IOS`.
6. Coverage `MSB`: LEFT/RIGHT `NOT_CHECKED / no_high_side_entity_link` по CONNECTION/STRUCTURE/TYPE.
7. **Вердикт: COMPLEMENTARY.**
8. Обоснование: TEXT описывает **комплектность документации**, GRAPHIC — **содержимое схемы**.
   Оба верны, слить нельзя.
9. Решающий gate: **M4**.

---

**A16. Кандидат на CONTRADICTORY — в реальных данных не нашёлся**

Проверено: чтобы получить `CONTRADICTORY`, нужны (а) один субъект, (б) одна ось,
(в) `CHECKED` покрытие, (г) противоположные направления. В ИОС пересечение множеств
«TEXT-сущность с `SAME_ENTITY/HIGH` на обеих сторонах» и «GRAPH-сущность с `CHECKED`»
**пусто**: единственная HIGH-связь односторонняя (RIGHT), а её граф-сущность
(`gfx_ent_77ec87caaebd3911498f`, `VRU_1`) не входит в 28 сущностей с `CHECKED`.

**CONTRADICTORY: NOT PRESENT IN REAL DATA.**

---

**A17. MERGE — в реальных данных не нашёлся**

Для `MERGE` нужны все M1–M8 разом, в том числе M3 (`SAME_ENTITY/HIGH` **на обеих** сторонах)
и M7 (`CHECKED` по общей оси). Единственная HIGH-связь — односторонняя, а TEXT-`CHECKED` = 0
во всём корпусе. Следовательно ни одна пара TEXT×GRAPHIC не проходит M3+M7 одновременно.

**MERGE: NOT PRESENT IN REAL DATA.**

> Это главный практический вывод корпуса: **на текущем состоянии конвейера G2.4.5 не сможет
> выполнить ни одного слияния.** Его первым реальным поведением будет
> COMPLEMENTARY / SINGLE_SOURCE / REVIEW_REQUIRED / UNRELATED.

---

**A18. 1 TEXT → N GRAPHIC (обязательный случай ✔)**

1. TEXT: `hlc_d3913c0ab7b930d2` (74 атома, service-бакет, охватывает 5 sheet-групп).
   GRAPHIC: все 6 изменений `chg_*` лежат в единственном `SCOPE-IOS`, который эта `hlc` покрывает.
2. TEXT: «Изменение не влияет на проектное решение» — 74 разнородных атома.
3. GRAPHIC: DETAIL ×2, UNCERTAIN ×2, GROUP_COUNT ×1, NODE_TYPE ×1.
4. Entity link: 28 связей на все стороны, из них 1 `SAME_ENTITY/HIGH`.
5. Scope: совпадает частично (1 из 5 групп).
6. Coverage: у сущностей этой `hlc` — `NOT_CHECKED` (все), у графических субъектов — 76 `CHECKED`.
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: одна TEXT-запись «накрывает» весь лист и все 6 графических событий.
   Слияние 1→6 без идентичности субъектов дало бы гарантированно ложный вывод.
9. Решающий gate: **M8**.

---

**A19. N GRAPHIC → 1 GRAPH-субъект (внутриграфическая кардинальность)**

1. GRAPHIC: `chg_5d6e7ac75c0d` (`DETAIL_LEVEL_INCREASED`, SECTION#1) и `chg_da7da89fba3c`
   (`DETAIL_LEVEL_INCREASED`, SECTION#2) — оба ссылаются на пересекающиеся наборы узлов
   (`SOURCE1`, `SOURCE2`, `SOURCE1:PATH1`, `SOURCE2:PATH1`, `INPUT1`, `INPUT2`, `BUS1`, `BUS2`).
2. TEXT: ABSENT.
3. GRAPHIC: оба ввода показаны подробнее.
4. Entity link: ABSENT.
5. Scope: `SCOPE-IOS` для обоих.
6. Coverage: `SOURCE_PATH_BUS_1`, `SOURCE_PATH_BUS_2`, `SOURCE_PATH_ELEMENT_BUS_1/2`,
   `INPUT_BUS_1/2` — все `CHECKED` по CONNECTION/STRUCTURE/TYPE.
7. **Вердикт: COMPLEMENTARY.**
8. Обоснование: это два **разных** ввода (секция 1 и секция 2), а не дубль одного события,
   несмотря на пересечение узлов `BUS1/BUS2`. Дедупликацию по узлам делать нельзя.
9. Решающий gate: **M2**.

### Блок B. АР — SYSTEM_GRAPH отсутствует, графика не участвует

---

**B1. АР-1: подтверждённое TEXT-изменение без графики (обязательный случай ✔)**

1. TEXT: `hlc_2cece3c61281e7f4` `PARAMETER_SET_CHANGED`, `CONFIRMED`, confidence `medium`,
   `decision_source = MIXED`, 12 атомов, группы `link_5bac0a5098c7`, `link_8b7b6a4bcb55`
   (`pairs/p570d156f57/high_level_project_changes.json`).
2. TEXT: «Скорректирован набор проектных параметров» — площади кладовых 7.К.4/7.К.5/7.К.6
   и «Блок кладовых: 28 / 150,80 м² против 12 / 64,60 м²».
3. GRAPHIC: ABSENT — `scope_join.json` АР: `graphic_scope_groups = 0`, `resolved_scopes = 0`,
   `unresolved_scopes = 8`; `graphic_coverage.json` АР: `by_subject_kind.GRAPH_ENTITY = 0`.
4. Entity link: ABSENT (обе стороны: `links = 0`, `graphic_entity_count = 0`).
5. Scope: `UNRESOLVED_SCOPE` для всех 8 листов (`no_graphic_scope_group_on_canonical_pages`).
6. Coverage: LEFT/RIGHT/BOTH `NOT_CHECKED / scope_join_unresolved` по CONNECTION/STRUCTURE/TYPE;
   PARAMETER/SPACE/METHOD/PRINCIPLE/QUANTITY — `NOT_APPLICABLE`.
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: графика физически не участвовала. Понижать уверенность TEXT из-за этого нельзя.
9. Решающий gate: **M7**.

---

**B2. АР-1: TEXT говорит «изменена программа помещений», подтверждён только текстом**

1. TEXT: `hlc_81baf0fc55420526` `SPACE_PROGRAM_CHANGED`, `CONFIRMED`, confidence **`high`**,
   `decision_source = DETERMINISTIC`, reason `TWO_SIDED_TEXT_PROOF`, subject `room_composition`.
2. TEXT: состав помещений изменён, доказано двусторонне.
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: `scope_9fac3bc12143beb17223` = `UNRESOLVED_SCOPE`.
6. Coverage: `SPACE` = `NOT_APPLICABLE` (ось вне возможностей MODE_2), остальные `NOT_CHECKED`.
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: единственная запись во всём корпусе с `confidence = high` **и**
   `TWO_SIDED_TEXT_PROOF`. Это эталон «текст сам себя доказал».
9. Решающий gate: **M7**.

---

**B3. АР-1: DETAIL на стороне TEXT, графики нет (обязательный случай — TEXT-половина)**

1. TEXT: `hlc_78243ba30fcf16c1` `DETAIL_LEVEL_INCREASED`, `CONFIRMED`, `decision_source = AI`,
   subject `equipment_material`, 1 атом `ev_2988677dcc6f4049`, группа `link_ba08ed80436a`.
2. TEXT: «Добавлена ведомость материалов кладочных стен»; reason прямо говорит:
   «подтверждено добавление ведомости, но не изменение состава материалов или проектного решения».
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: `scope_d533f766e8c12cba0265` = `UNRESOLVED_SCOPE`.
6. Coverage: все наблюдаемые оси `NOT_CHECKED / scope_join_unresolved`.
7. **Вердикт: SINGLE_SOURCE** с outcome `DETAIL`.
8. Обоснование: единственный реальный TEXT `DETAIL_LEVEL_INCREASED`. Он не должен смешиваться
   с 43 `MATERIAL`-атомами того же листа (`hlc_6bea8ebfc130d85c`).
9. Решающий gate: **M6** (outcome compatibility) при попытке слияния внутри листа.

---

**B4. АР-1: fail-closed из-за несовместимого типа от AI**

1. TEXT: `hlc_150a5d10541e7822` `UNRESOLVED_HIGH_LEVEL_CHANGE`, `decision_source = FAIL_CLOSED`,
   reason `AI_UNAVAILABLE:validation_failed:incompatible_high_level_type`, subject `principle`.
2. TEXT: модель предложила тип, несовместимый с кандидатным; система деградировала до `UNRESOLVED`.
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: `scope_9fac3bc12143beb17223` (UNRESOLVED).
6. Coverage: `NOT_CHECKED`.
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: это не изменение и не отсутствие изменения — это отказ классификатора.
   G2.4.5 обязан переносить такой статус наружу, а не гасить.
9. Решающий gate: **M7** (валидность источника: источник сам себя признал невалидным).

---

**B5. АР-1: AI отказался подтвердить удаление материала**

1. TEXT: `hlc_d817e57d3fba02ba` `UNRESOLVED_HIGH_LEVEL_CHANGE`, `decision_source = AI`,
   subject `минеральн`, группа `link_6976a3842ce3`.
2. TEXT: «Фрагмент о минеральной вате отсутствует справа, но одного отсутствия в представленном
   фрагменте недостаточно, чтобы доказать удаление материала».
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: `scope_2dbfabd7ebd705398b29` (UNRESOLVED).
6. Coverage: `NOT_CHECKED` / `NOT_APPLICABLE`.
7. **Вердикт: REVIEW_REQUIRED.**
8. Обоснование: образец правильного fail-closed на стороне TEXT — тот же принцип,
   что `NOT_CHECKED` на стороне GRAPHIC.
9. Решающий gate: **M7**.

---

**B6. АР-1: сущность существует, но с одной стороны её нет**

1. TEXT: `txt_ent_35c436241d8511bfa3d2` (`ROOM_7_K_4`, «Помещение 7.К.4»), источник `hlc_2cece3c61281e7f4`.
2. TEXT: единственный атом `ev_2168b0cfc9061d3b`, `source_status = REMOVED`,
   «Помещение 7.К.4 «Кладовая» площадью 6,50 отсутствует справа».
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: `UNRESOLVED_SCOPE`.
6. `query_text_entity_side`: LEFT `presence = PRESENT`, RIGHT `presence = ABSENT`;
   coverage — `NOT_CHECKED` по всем наблюдаемым осям.
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: важный контрпример к заголовку `hlc` — заголовок говорит про «разные значения
   площадей», а атом про удаление помещения. **Ось агрегата ≠ ось атома.**
9. Решающий gate: **M4** (при агрегации), M7 (при итоговом выводе).

---

**B7. АР-1: 39 атомов свёрнуты в один `NO_HIGH_LEVEL_CHANGE`**

1. TEXT: `hlc_a95030b3a8533021`, `NO_HIGH_LEVEL_CHANGE`, count = 39, reason `NO_PROJECT_DECISION_SIGNAL`,
   три группы (`link_5bac0a5098c7`, `link_8b7b6a4bcb55`, `link_ba08ed80436a`).
   Из него порождены 17 TEXT-сущностей `ROOM_3_K_1..ROOM_3_K_16` + `ROOM_3_K_1`.
2. TEXT: «Изменение не влияет на проектное решение».
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: три разных `UNRESOLVED_SCOPE`.
6. Coverage: `NOT_CHECKED`.
7. **Вердикт: SINGLE_SOURCE** (отрицательное утверждение TEXT).
8. Обоснование: это N-сущностей→1-запись в обратную сторону. При построении evidence-цепочек
   G2.4.5 получит 17 субъектов с одним и тем же `source_change_id` — дедуплицировать по нему нельзя.
9. Решающий gate: **M2**.

---

**B8. АР-2: полностью изолированная пара**

1. TEXT: `hlc_2b9706e6a87dd73e` `PARAMETER_SET_CHANGED`, `CONFIRMED`, confidence **`high`**,
   `DETERMINISTIC`, `TWO_SIDED_TEXT_PROOF`, 43 атома, группа `link_c2c978d0b15e`
   (`pairs/p16b108b9f5/high_level_project_changes.json`).
2. TEXT: скорректирован набор проектных параметров (площади).
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT — **для этой пары `text_entities.json` вообще не строился**.
5. Scope: ABSENT (`scope_join` не строился).
6. Coverage: ABSENT (артефакта нет).
7. **Вердикт: SINGLE_SOURCE.**
8. Обоснование: пара показывает нижнюю границу — G2.4.5 обязан корректно работать, когда
   из восьми входов есть только два (Stage 5 и Stage 5.3).
9. Решающий gate: **M7**.

---

**B9. АР-2: 15 `UNRESOLVED` при 1 `CONFIRMED`**

1. TEXT: `hlc_1111f3fb655e9c76` (count 22, `SOURCE_LINK_UNCERTAIN`),
   `hlc_49841d0b47bec5f3` (count 6), `hlc_62e0b5f756c8d97f`, `hlc_f85646c019ce2db0`
   (`INSUFFICIENT_CONTEXT`) — и ещё 11 таких же.
2. TEXT: «Возможное изменение проектного решения требует проверки».
3. GRAPHIC: ABSENT.
4. Entity link: ABSENT.
5. Scope: ABSENT.
6. Coverage: ABSENT.
7. **Вердикт: REVIEW_REQUIRED** (для всех 15).
8. Обоснование: в АР-2 отношение «требует проверки : подтверждено» = 15 : 1.
   Любая политика уверенности G2.4.5 будет работать в основном именно с этим классом.
9. Решающий gate: **M7**.

---

### Сводка вердиктов корпуса

| Вердикт | Кол-во кейсов | Кейсы |
|---|---:|---|
| `MERGE` | **0** | NOT PRESENT IN REAL DATA (A17) |
| `COMPLEMENTARY` | 3 | A1, A15, A19 |
| `CONTRADICTORY` | **0** | NOT PRESENT IN REAL DATA (A16) |
| `REVIEW_REQUIRED` | 9 | A5, A6, A9, A11, A12, A18, B4, B5, B9 |
| `UNRELATED` | 4 | A7, A8, A10, A14 |
| `SINGLE_SOURCE` | 10 | A2, A3, A4, A13, B1, B2, B3, B6, B7, B8 |

(Всего описано 28 кейсов: A1–A19 + B1–B9; из них A16 и A17 — отрицательные результаты, остальные 26 распределены по вердиктам выше.)

### Обязательные случаи из ТЗ — статус

| Требуемый случай | Статус | Кейс |
|---|---|---|
| Один объект, разные аспекты | ✔ есть | A1, A15 |
| GRAPHIC = NOT_CHECKED при TEXT-утверждении | ✔ есть | A3 |
| GRAPHIC = NOT_APPLICABLE | ✔ есть | A4, A14 |
| CHECKED только с одной стороны | ⚠ частично | A5 (по оси, не по стороне); «только LEFT» или «только RIGHT» — **NOT PRESENT IN REAL DATA** |
| TEXT MATERIAL + GRAPHIC DETAIL_LEVEL_INCREASED | ⚠ частично | A8 (в разных scope); в одном scope — **NOT PRESENT IN REAL DATA** |
| 1 TEXT → N GRAPHIC | ✔ есть | A18 (1 → 6), A1 |
| АР без SYSTEM_GRAPH | ✔ есть | B1–B9 |
| entity link = UNKNOWN из-за нескольких кандидатов | ✔ есть | A7 (1→8), A6 (1→2) |

---

## ЧАСТЬ 5. Расхождения

### 5.1 У GRAPH_ENTITY нет BOTH-агрегата — **ПОДТВЕРЖДЕНО**

**Код.** [graphic_coverage.py:234-244](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L234):
`_graph_entity_records(...)` содержит `for side in SIDES:` (`SIDES = ("LEFT","RIGHT")`) и
возвращает записи только для этих двух сторон. Результат кладётся как есть:
[строка 576](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L576)
`records.extend(graph_for_scope)` — без агрегации.

Для TEXT_ENTITY, напротив, есть третья запись:
[строки 599-614](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L599)
строят `per_side` и затем `_combine_text_sides(...)`
([строка 477](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L477))
с `side="BOTH"`.

**Данные.** ИОС: 864 GRAPH_ENTITY-записи = (56 LEFT + 52 RIGHT) × 8 осей — ни одной со `side="BOTH"`;
1992 TEXT_ENTITY-записи = 83 × 8 × 3 стороны (LEFT/RIGHT/**BOTH**).

**Последствие для G2.4.5:** «проверена ли графическая сущность на обеих сторонах» придётся
считать самому. Правило `_combine_text_sides` — не универсальное: оно склеивает
`NOT_APPLICABLE` + `NOT_CHECKED` → `NOT_CHECKED`, теряя различие «не умеем смотреть» и «не смотрели».

### 5.2 Порог 0.85 в `_match_sets` vs 0.68 в comparison policy — **ПОДТВЕРЖДЕНО, но не так, как сформулировано**

* Хардкод: [graphic_coverage.py:227](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L227)
  ```
  if isinstance(item, dict) and float(item.get("match_confidence") or 0.0) >= 0.85:
  ```
* Политика: [system_graph_comparison_policy.py:38](backend/app/pipeline/stages/block_grounding/system_graph_comparison_policy.py#L38)
  `high_match_threshold: float = 0.68` (и `medium_match_threshold = 0.38`, строка 39).
  В данных это видно в `comparison_result.json → matching.policy.high_match_threshold = 0.68`.

**Уточнение (важно):** порог 0.85 применяется **только к `matching.detail_matches`**.
Основной набор `matching.matches` фильтруется по `item["decision"] == "HIGH_MATCH"`
([graphic_coverage.py:218-224](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L218)),
то есть по решению комаратора, принятому на пороге 0.68. Итог: **в одном множестве `high`
смешаны узлы, прошедшие 0.68, и узлы, прошедшие 0.85.**

Дополнительно: 0.85 совпадает по числу с `MODE2_CONFIDENCE_POLICY_V1.high_minimum = 0.85`
([confidence_policy.py](backend/app/services/stage_comparison/graphic_comparison/confidence_policy.py)),
но это **другая шкала** — там это порог уверенности изменения, здесь — порог идентичности.
Совпадение числа выглядит как случайное переиспользование константы.

**Не исправлялось** (read-only режим).

### 5.3 `saved_coverage_bundle_is_stale(...)` vs `graphic_coverage_is_stale(...)` — **ПОДТВЕРЖДЕНО, проверяют разное**

[graphic_coverage.py:799](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L799)
и [:825](backend/app/services/stage_comparison/unified_entity_bridge/graphic_coverage.py#L825).

* `graphic_coverage_is_stale(artifact, stage53, text, side_graphs, side_links, scope_join, graphic_scope_groups)` —
  **7 входов**, включая «живой» Stage 5.3 и **текущий** набор `graphic_scope_groups`.
  Он вызывает `scope_join_is_stale(...)` (полная сверка scope против stage53 и групп) и
  пересчитывает `_coverage_signature` от **переданных** источников.
* `saved_coverage_bundle_is_stale(artifact, text, side_graphs, side_links, scope_join)` —
  **5 входов**: ни `stage53`, ни `graphic_scope_groups` не передаются. Вместо
  `scope_join_is_stale` он делает ручное сравнение полей `scopes["source_artifacts"]`
  (schema_version + source_signature для text/side-graphs, pair_id + artifact_digest для stage53),
  а группы берёт **из самого проверяемого артефакта** (`validated["source_artifacts"]["graphic_scope_groups"]`).

**Разница в одной фразе:** первый отвечает «согласован ли coverage с текущим состоянием мира»,
второй — «внутренне ли согласован сохранённый комплект из пяти файлов». Второй **не заметит**,
что изменились исходные SYSTEM_GRAPH/ledger, если сохранённый scope_join остался прежним.

### 5.4 Прочие расхождения между кодом/данными и формулировками ТЗ

1. **`graphic-coverage.v1` → фактически `v2`.** В ТЗ артефакт назван `graphic-coverage.v1`.
   В коде и данных — `graphic-coverage.v2` / `graphic-coverage-builder-v2` /
   `graphic-coverage-policy-v2` (переименовано коммитом `61553e16`). `v1` в репозитории не существует.

2. **`DETAIL_LEVEL_INCREASED` — тип и в TEXT, и в GRAPHIC.** Одна и та же строка присутствует
   в `HIGH_LEVEL_TYPES` (TEXT) и в `STRUCTURAL_CHANGE_TYPES` (GRAPHIC), но семантика разная:
   у TEXT это «добавлена ведомость/таблица», у GRAPHIC — `representation_expansion` узла.
   Совпадение имён создаёт ловушку ложного merge.

3. **У ledger v2 нет поля `kind`, а unified-контракт его требует.**
   [unified_evidence_contract.py:22](backend/app/services/stage_comparison/unified_evidence_contract.py#L22)
   объявляет `GRAPHIC_ARTIFACT_KIND = "graphic_change_ledger"` и валидирует
   `evidence[].source_artifact.kind`. Но сам ledger
   (`graphic_change_ledger_v2.schema.json`, `additionalProperties: false`) поля `kind` **не имеет** —
   его придётся синтезировать при построении unified-envelope.

4. **`project_change_summary.json` без `schema_version`.** Все остальные артефакты цепочки
   имеют версию схемы; Stage 5 — только `version: 1`. Для G2.4.5 это единственный вход,
   который нельзя проверить по строке версии.

5. **`unresolved[]` дублирует `material_review[]`** во всех трёх парах. Наивный обход всех
   бакетов даёт +22 фантомных изменения (22 из 91).

6. **`service_structure_summary.items[]` — скрытый шестой бакет с полноценными изменениями.**
   Именно оттуда происходят все TEXT-сущности `VRU_*` и `PANEL_*` ИОС (74 атома в
   `hlc_d3913c0ab7b930d2`). Обход только пяти «явных» списков потеряет 12 из 19 сущностей ИОС.

7. **`graph_entities_path()` и `entity_links_path()` объявлены, но не используются нигде.**
   `grep -rn "graph_entities_path\|entity_links_path" backend/ tests/ scripts/` → только
   объявление и `__all__` в `paths.py`. GET-эндпоинт есть только у `text-entities`.

8. **G2.4.4-артефакты не имеют продовой обвязки вообще.** Ни `paths.py`, ни `store.py`,
   ни роутер не знают про `side_graph_entities.json`, `side_entity_links.json`,
   `scope_join.json`, `graphic_coverage.json`. Единственный производитель —
   `scripts/run_g2_4_4_scope_side_coverage.py`.

9. **`OPERATION` отсутствует среди осей текущей политики.**
   `graphic_coverage_policy.DIMENSIONS` = 8 осей без `OPERATION`, хотя TEXT-тип
   `SYSTEM_OPERATION_CHANGED` объявлен.

10. **Нет доказательства, что SYSTEM_GRAPH-блоки принадлежат тем же PDF, что и Stage 5.3-пара.**
    `scope_join.json → source_artifacts` содержит `stage53.pair_id` и `block_id`/`page_index`
    графических блоков, но ни `document_code`, ни путь к PDF. В `left/right_system_graph.json`
    поле `provenance` тоже без идентификатора документа. Связка ИОС ↔ `blk_039909ec…`/`blk_2d72a670…`
    держится на аргументах CLI, а не на данных. Для G2.4.5 это M1 на уровне документа —
    сейчас недоказуем.

11. **Ни одного продового экземпляра артефактов на диске.**
    `find comparison -name "text_entities.json" -o -name "graph_entities.json" -o
    -name "entity_links.json" -o -name "graphic_change_ledger.json" -o -name "graphic_coverage.json"`
    → пусто. Весь корпус — только `experiments/`.

12. **Несогласованность регистра reason_codes.** `NEIGHBOUR_IDENTITY_UNRESOLVED` — UPPER_SNAKE,
    все прочие 12 кодов — lower_snake, в одном и том же поле `reason_codes[]`.

13. **`confidence` у TEXT — строчными (`medium`, `high`), у GRAPHIC и bridge — прописными
    (`HIGH`, `MEDIUM`, `LOW`, `UNKNOWN`).** Прямое сравнение строк даст ложный «не равно».

14. **Легаси-артефакт с невалидным контрактом.**
    `experiments/g2_vectograf_system_graph_research/artifacts/grsh_mode2_ledger.json`
    объявляет `graphic-change-ledger.v1` при `mode: "MODE_2"`;
    `validate_ledger(...)` → `LedgerValidationError: invalid mode`. Это единственный источник
    типов `FUNCTIONAL_GROUP_CHANGED` и `CONNECTION_CHANGED` в репозитории — использовать его
    как корпус нельзя.

---

## ЧАСТЬ 6. Открытые вопросы к дизайну G2.4.5

**Q1. Откуда брать dimension — из типа или из содержимого?**
Варианты: (а) детерминированная таблица `type → dimension` из Части 3;
(б) вывод оси из `details[].before/after` детерминированными правилами;
(в) обязательное `UNKNOWN_DIMENSION` для всех многоосевых типов.
Риски: (а) — `PARAMETER_SET_CHANGED` доказанно многоосевой (`hlc_2cece3c61281e7f4`),
таблица будет систематически врать; (б) — правила по тексту это скрытый парсер,
он даст недетерминированный дрейф при смене формулировок модели;
(в) — оси не будет почти нигде, M4 выродится в «всегда UNKNOWN», merge станет невозможен даже теоретически.

**Q2. Что делать с `UNRESOLVED_HIGH_LEVEL_CHANGE` (22 из 69 записей, в АР-2 — 15 из 16)?**
Варианты: (а) не пускать в синтез вообще; (б) пускать как `REVIEW_REQUIRED`-носитель без оси и направления;
(в) пускать как полноценный TEXT-факт с низкой уверенностью.
Риски: (а) — теряется бо́льшая часть реального сигнала АР-2; (в) — прямое нарушение fail-closed,
такие записи по построению не доказаны; (б) — придётся определить, может ли `REVIEW_REQUIRED`
участвовать в M2/M8 (сопоставляться с графикой) или только выводиться отдельным списком.

**Q3. Считать ли `NO_HIGH_LEVEL_CHANGE` (42 записи) утверждением об отсутствии изменения?**
Варианты: (а) да, это TEXT-доказательство отсутствия; (б) нет, это только «не поднято до
верхнего уровня», факт изменения при этом может существовать; (в) различать по `route_reason`
(`NO_SEMANTIC_CHANGE` → отсутствие; `SERVICE_ONLY_REVIEW`/`LOW_VALUE_UNCERTAIN` → не отсутствие).
Риски: (а) — `hlc_d3913c0ab7b930d2` помечен `NO_HIGH_LEVEL_CHANGE`, но содержит
«Схема ВРУ-1 разделена…» и «Схемы ЩР-1–ЩР-5 отсутствуют справа», то есть отсутствием
изменения не является; (б) — теряется единственный TEXT-сигнал «здесь чисто»;
(в) — привязка к 11 недокументированным строкам `route_reason`, которые может поменять
следующая версия Stage 5.3.

**Q4. Что означает `NOT_APPLICABLE` в BOTH-агрегате?**
Сейчас `_combine_text_sides` даёт `NOT_APPLICABLE` только если N/A **обе** стороны,
а `N/A + NOT_CHECKED` → `NOT_CHECKED`. Варианты: (а) оставить как есть;
(б) ввести четвёртое состояние `PARTIALLY_APPLICABLE`; (в) считать `N/A` доминирующим.
Риски: (а) — «графика не умеет это видеть» маскируется под «графика не посмотрела»,
и G2.4.5 будет предлагать «дообследовать» то, что дообследовать нечем;
(в) — реальные `NOT_CHECKED` спрячутся за `N/A` и покрытие будет выглядеть лучше, чем есть.

**Q5. Какова минимальная планка для `MERGE`?**
Варианты: (а) `SAME_ENTITY/HIGH` на **обеих** сторонах + `CHECKED` по общей оси на обеих сторонах
(тогда, по корпусу, merge = 0 навсегда до улучшения bridge);
(б) `SAME_ENTITY/HIGH` хотя бы на одной стороне + `CHECKED` на той же стороне;
(в) допускать `POSSIBLE_ENTITY` при совпадении оси и направления.
Риски: (а) — G2.4.5 никогда не сработает на текущих данных, ценность фичи под вопросом;
(б) — воспроизводит именно A6, где RIGHT HIGH при LEFT ambiguous, то есть слияние без
доказанного левого прообраза; (в) — `POSSIBLE_ENTITY` в реальных данных не встречается вообще,
правило будет непроверяемым.

**Q6. Как считать уверенность объединённого изменения при двух несовместимых шкалах?**
TEXT даёт `high/medium` (строчными, без числа); GRAPHIC даёт `raw_confidence` (0..1) **и**
`HIGH/MEDIUM/LOW` по порогам 0.85/0.60; bridge даёт `HIGH/MEDIUM/LOW/UNKNOWN`;
coverage даёт не уверенность, а состояние проверки.
Варианты: (а) min по решётке `UNKNOWN < LOW < MEDIUM < HIGH`; (б) числовая свёртка через
восстановление raw (у TEXT raw нет — придётся выдумать); (в) не считать одно число,
а возвращать вектор `(text_confidence, graphic_confidence, link_strength, coverage_state)`.
Риски: (а) — всё схлопнется в `LOW/UNKNOWN`, различать нечего;
(б) — прямое нарушение «не выдумывать данные»; (в) — потребитель (UI/отчёт) должен уметь
показывать вектор, чего сейчас нет.

**Q7. Кто владеет группировкой графических блоков в scope-группы в проде?**
Сейчас `produce_graphic_scope_groups(...)` вызывается только из CLI и на реальных данных
всегда даёт ровно `1 группа × 1 пара`. В проде подача полного плоского набора пар не реализована.
Варианты: (а) G2.4.5 сам собирает набор пар; (б) отдельный продовый producer до G2.4.5;
(в) оставить CLI-only и не запускать G2.4.5 в проде.
Риски: (а) — синтезатор берёт на себя ответственность за scope, что противоречит разделению
G2.4.4/G2.4.5; (б) — новый этап конвейера и новые артефакты на диске; (в) — фича не доедет до пользователя.

**Q8. Как доказывать, что графические блоки принадлежат тем же документам, что и пара?**
Сегодня связки нет (расхождение 5.4.10). Варианты: (а) добавить `document_code`/`pdf_path`
в `graphic_scope_group`; (б) сверять по `block_id` через `document_graph.json`;
(в) считать доказанным по факту вызова.
Риски: (в) — молчаливая подстановка чужой схемы под чужую пару, худший из возможных ложных merge;
(а)/(б) — требуют менять артефакты G2.4.4 или тянуть внешний источник, то есть выходят
за «additive»-принцип, которого держались G2.4.1–G2.4.4.

**Q9. Что делать при `CHECK_BLOCKED` — состояние, которого нет в реальных данных?**
Варианты: (а) трактовать как `REVIEW_REQUIRED`; (б) как `NOT_CHECKED`; (в) как отдельный
терминальный вердикт «сравнение заблокировано качеством».
Риски: (б) — потеря различия «нечего смотреть» и «смотреть нельзя из-за качества»;
(а) — очередь ревью может распухнуть на порядки, если quality gate начнёт срабатывать;
(в) — правило проверяется только синтетикой, на проде поведение неизвестно.

**Q10. Нужен ли `MERGE` вообще на первом релизе?**
Корпус даёт 0 merge-кандидатов (A17). Варианты: (а) реализовать merge сразу
(рискуя, что он никогда не сработает и не будет протестирован на реальных данных);
(б) первый релиз без merge — только `COMPLEMENTARY / SINGLE_SOURCE / REVIEW_REQUIRED / UNRELATED`;
(в) merge только внутри одного источника (GRAPHIC×GRAPHIC — A19 показывает, что и там
объединять нельзя).
Риски: (а) — непроверяемый код в проде; (б) — потребитель не получает обещанной «единой картины»;
(в) — A19 доказывает, что даже внутриграфическое объединение по пересечению узлов даёт ложное слияние.

---

## Приложение A. Команды воспроизведения чисел

**A.1 — типы TEXT-изменений по бакетам (Часть 2A):**
```bash
cd /home/coder/projects/PDF-proverka/comparison/sessions/121d764109184c13/pairs
python3 -c '
import json, collections
for p in ["p16b108b9f5","p26c08b83a6","p570d156f57"]:
    d=json.load(open(p+"/high_level_project_changes.json")); c=collections.Counter()
    for b in ["high_level_changes","detail_level_increased","material_review","non_material_review"]:
        for it in d[b]: c[(b,it["type"])]+=1
    for it in d["service_structure_summary"]["items"]: c[("service",it["type"])]+=1
    print(p, dict(c))'
```

**A.2 — типы GRAPHIC-изменений (Часть 2B):**
```bash
python3 -c '
import json, collections
d=json.load(open("experiments/g2_4_4_scope_side_coverage/ios/graphic_change_ledger.json"))
print(dict(collections.Counter(c["type"] for c in d["changes"])))
print(dict(collections.Counter(c["confidence"] for c in d["changes"])))'
```

**A.3 — coverage по kind/dimension/side/state (Часть 2C):**
```bash
python3 -c '
import json, collections
for n in ["ios","ar"]:
    d=json.load(open(f"experiments/g2_4_4_scope_side_coverage/{n}/graphic_coverage.json"))
    print(n, d["summary"])
    c=collections.Counter((r["subject"]["kind"],r["dimension"],r["side"],r["state"]) for r in d["coverage"])
    for k in sorted(c): print("  ",k,c[k])'
```

**A.4 — сила связей entity bridge (Часть 2D):**
```bash
python3 -c '
import json, collections
for n in ["ios","ar"]:
    d=json.load(open(f"experiments/g2_4_4_scope_side_coverage/{n}/side_entity_links.json"))
    for s in ["LEFT","RIGHT"]:
        L=d["sides"][s]["links"]
        print(n,s,len(L),dict(collections.Counter(l["relation"] for l in L)),
              dict(collections.Counter(e["rule"] for l in L for e in l["evidence"])))'
```

**A.5 — presence/match по сторонам (Часть 2D, побочный контракт):**
```bash
python3 -c '
import json,sys; sys.path.insert(0,".")
from backend.app.services.stage_comparison.unified_entity_bridge import query_text_entity_side
b="comparison/sessions/121d764109184c13/pairs/"
for n,pid in [("ios","p26c08b83a6"),("ar","p570d156f57")]:
    st=json.load(open(b+pid+"/high_level_project_changes.json"))
    te=json.load(open(f"experiments/g2_4_4_scope_side_coverage/{n}/text_entities.json"))
    sl=json.load(open(f"experiments/g2_4_4_scope_side_coverage/{n}/side_entity_links.json"))
    for e in te["entities"]:
        r={s:query_text_entity_side(st,te,sl,e["entity_id"],s) for s in ("LEFT","RIGHT")}
        print(n,e["canonical_name"],{s:(r[s]["presence"],r[s]["match"]) for s in r})'
```

**A.6 — проверка легаси-ledger (расхождение 5.4.14):**
```bash
python3 -c '
import json,sys; sys.path.insert(0,".")
from backend.app.services.stage_comparison.graphic_comparison.contract import validate_ledger
for f in ["experiments/g2_vectograf_system_graph_research/artifacts/grsh_mode2_ledger.json",
          "experiments/g2_4_4_scope_side_coverage/ios/graphic_change_ledger.json"]:
    try: validate_ledger(json.load(open(f))); print("OK  ",f)
    except Exception as e: print("FAIL",f,e)'
```

**A.7 — отсутствие продовой обвязки G2.4.4 (расхождения 5.4.7, 5.4.8, 5.4.11):**
```bash
grep -rn "graphic_coverage\|scope_join\|side_graph_entities\|side_entity_links" \
  backend/app --include=*.py | grep -v "unified_entity_bridge/"      # → пусто
grep -rn "graph_entities_path\|entity_links_path" backend/ tests/ scripts/  # → только paths.py
find comparison -name "text_entities.json" -o -name "graphic_coverage.json"  # → пусто
```

---

*Отчёт подготовлен в read-only режиме. Ни один файл вне `docs/research/` не создавался и не изменялся; коммитов нет.*
