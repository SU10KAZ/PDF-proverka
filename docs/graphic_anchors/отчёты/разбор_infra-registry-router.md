# ОТЧЁТ: инфраструктура профилей/роутинга блоков + незакоммиченная работа `feature/block-vector-graphs`

## 1. РЕЕСТР ПРОФИЛЕЙ — `block_profile_registry.py`

Реестр — это НЕ реестр матчеров, а (а) реестр **эталонных блоков** проверенного корпуса и (б) сборщик **канонического пакета** блока. Матчеры-классификаторы живут в per-discipline geometry-модулях (см. раздел 2).

**Константы и схема:**
- `SCHEMA_VERSION = 6` (block_profile_registry.py:32) — v4: эталон по смыслу/подтипу/сигнатуре; v5: строгий клип текста по полигону блока; v6: эталоны во встроенном каталоге `block_context/reference_catalog` (импорты :18-23).
- `ARTIFACT_DIRNAME = "block_vector_graphs"` (:33) — пакеты хранятся в `_output/block_vector_graphs/<block_id>.json` (`artifact_path` :437, `artifact_filename` :432).
- `DISCIPLINE_TITLES` (:35) — 9 дисциплин: ЭОМ, ГП, АР, КЖ, КМ, ТХ, ОВ, ВК, СС.
- `SOURCE_DISCIPLINES` (:47) — маппинг `source_kind → дисциплина`: `structured_singleline`/`structured_electrical`→ЭОМ, `structured_general_plan`→ГП, `structured_architecture`→АР, `structured_structure`→КЖ, `structured_technology`→ТХ, `structured_hvac`→ОВ, `structured_water`→ВК, `structured_alia_scheme`→СС.

**Выбор эталона (`select_reference` :238):** внутри пары (дисциплина, profile_id) кандидаты из `_reference_candidates()` (:118, lru_cache по `load_reference_records()`), ранжирование по 4 факторам:
- `semantic` — IDF-косинус токенов описания (`_reference_tokens` :71 — самописный RU-стеммер с отсечением окончаний; `_semantic_similarities` :202);
- `subtype` — `_subtype_similarity` (:189, generic-подтипы обнуляются);
- `structure` — `_graph_signature` (:149 — Counter node_types + counts nodes/containers/networks/edges/`*_total` из validation) и `_structure_similarity` (:171 — 0.65·cosine типов + 0.35·log-совпадение счётчиков);
- `quality` — кортеж (complete, covered_facts, evidence) с весами из reference rules.

Веса по умолчанию: semantic 0.35 / subtype 0.40 / structure 0.15 / quality 0.10 (:298-303), конфигурируются `_SELECTION_RULES` (reference_rules). Режимы результата: `dynamic_similarity` (порог `strongest_match_min` 0.08, confidence high/medium/low, top-3 альтернатив), `canonical_profile_fallback` (:357), «встроенная грамматика профиля» без корпусного блока (:366). Текущий блок исключается из кандидатов (`exclude_current_block` :249).

**Контракт пакета (`make_package` :375):** `{schema_version, block_id, page, source_kind, discipline, discipline_title, profile_id, classification, reference_catalog, reference, gate, readiness, validation, graph, markdown, user_text, error}`. `select_reference` fail-soft (:399-408): при ошибке — канонический эталон профиля, граф не теряется.

**`load_prepared_package` (:441):** валидирует schema_version/block_id/source_kind; НОВОЕ (незакоммичено) — поддержка `graph_artifact` sidecar (см. раздел 5).

**Полный список profile_id** (канонический перечень = `PROFILE_LABELS` в profiled_graph_localization.py:31-140):
- **ЭОМ:** `electrical_singleline`, `panel_circuit_scheme`, `electrical_distribution_plan`, `lighting_plan`, `cable_route_plan`, `lightning_grounding_plan`, `equipotential_scheme`, `ozds_topology`, `electrical_installation_detail`, `electrical_equipment_drawing`, `illuminance_calculation`;
- **ГП:** `gp_axis_plan`, `gp_general_plan`, `gp_stakeout_plan`, `gp_grading_plan`, `gp_grading_detail`, `gp_earthwork_plan`, `gp_pavement_plan`, `gp_road_structure`, `gp_surface_layout`, `gp_small_forms_plan`, `gp_drainage_plan`, `gp_drainage_profile`, `gp_drainage_detail`;
- **АР (22):** `ar_masonry_plan`, `ar_masonry_detail`, `ar_stair_drawing`, `ar_railing_detail`, `ar_section`, `ar_opening_plan`, `ar_facade`, `ar_detail`, `ar_marking_plan`, `ar_opening_drawing`, `ar_floor_plan`, `ar_roof_plan`, `ar_roof_detail`, `ar_finish_plan`, `ar_equipment_foundation`, `ar_wall_elevation`, `ar_ceiling_plan`, `ar_floor_finish_plan`, `ar_interior_electrical_plan`, `ar_furniture_plan`, `ar_floor_wall_junction_detail`;
- **КЖ:** `kj_formwork_plan`, `kj_reinforcement_plan`, `kj_reinforcement_section`, `kj_marking_plan`, `kj_embedded_parts`, `kj_structural_detail`;
- **КМ:** `km_layout_plan`, `km_member_drawing`, `km_connection_detail`, `km_ladder_drawing`, `km_facade_layout`, `km_facade_detail`, `km_mockup`;
- **ТХ:** `tx_parking_plan`, `tx_parking_detail`, `tx_lift_plan`, `tx_lift_section`, `tx_lift_equipment`, `tx_lift_assignment`, `tx_waste_plan`, `tx_waste_detail`;
- **ОВ:** `hvac_floor_plan`, `heating_axonometry`, `ventilation_axonometry`, `hydronic_principle`, `hvac_installation_detail`, `hvac_section_layout`, `hvac_equipment_drawing`, `hvac_performance_chart`, `hvac_site_overview`;
- **ВК:** `vk_floor_plan`, `water_supply_axonometry`, `sewer_axonometry`, `fire_water_axonometry`, `vk_principle_scheme`, `control_circuit_graph`, `vk_installation_detail`, `external_network_profile`, `pump_equipment_drawing`;
- **СС (~20):** `cctv_floor_network`, `voice_alarm_line_topology`, `fiber_ring_backbone`, `metering_floor_bus_water`/`_heat`, `automation_control_hierarchy`, `dispatch_integration_backbone`, `lift_dispatch_floor_topology`, `mgn_intercom_floor_bus`, `cabinet_commutation_graph`, `cabinet_rack_layout`, `functional_process_io`, `external_terminal_wiring`, `multidiscipline_niche_layout`, `discipline_floor_plan`, `device_terminal_wiring`, `installation_assembly`, `physical_panel_layout`, `access_point_assembly` + НОВЫЕ (незакоммичено): `fire_alarm_loop_topology`, `cable_tray_axonometry`, `low_voltage_terminal_wiring`;
- служебный `raw_vector`.

**Контракт extractor'а** (единый для всех дисциплинарных модулей, видно по вызовам роутера): `classify_<disc>_profile(context_text[, block_id, prefer_block_hint]) → profile_id|None`; `build_<disc>_graph_from_source(pdf, page_index, bbox_norm, polygon_norm, block_id, profile_hint[, subtype_hint]) → graph dict | None`; `evaluate_<disc>_gate(graph) → {"use": bool, "reasons", "warnings", "metrics"[, "mode"]}`; `render_<disc>_markdown(graph) → str`. Граф содержит `profile_id`, `subtype`, `containers/nodes/networks/edges`, `validation`, `readiness`, `warnings`, `status`.

## 2. РОУТЕР — `block_source_router.py`

Идея (docstring :1-26, решение Андрея 07-07): «вместо Gemma везде — сырые вектор-данные, для известных типов — структурированный профиль». Развилка на блок: (1) однолинейка + гейт Вектографа → полный рендер; (2) известная схема дисциплины + профильный гейт → структурированный Markdown; (3) есть вектор-слой → сырой вектор-текст полигон-клипа; (4) нет слоя → `image_only` (Stage 01 анализирует PNG). Gemma удалена — «gemma_fallback» существует только как legacy-строка, мапится в `image_only`.

**Ключевые механики:**
- `_MIN_VECTOR_CHARS = 40` (:46) — порог «вектор-слой есть».
- `_locate` (:111) — PDF + `document_graph.json` вверх по родителям до 4 уровней (legacy `_output`, V2 `02_work/document.pdf`, v2-primary `03_analysis/runs/<id>`).
- `_extract_block` (:522) — один `fitz.open`; клип слов страницы по `polygon_points_norm` (`_clip_words_to_polygon`) либо `coords_norm` (`_clip_words_to_bbox`), реконструкция текста `_block_text`; возвращает `(page_text, block_text, bbox, poly, page_pdf)`.
- Классификация типа: `_load_chandra_description` (:162) → `_classification_context` (:177) — профиль выбирается по **описанию Chandra**, НЕ по тексту чертежа (примечания «см. кладочные планы» не должны менять тип); `_classification_metadata` (:187) — аудируемый `source`: `chandra_md` / `vector_block_fallback` / `vector_page_fallback` / `vector_pdf_fallback` / `vectograph_pdf` / `per_block_*`.
- `_discipline_hint` (:223) — дисциплина версии из пути `disciplines/<code>`, `project_info.json → section`, legacy-частей пути; `_PATH_DISCIPLINES` (:92) включает alias АИ→АР. Предикат `allows(code) = hint in (None, code)` (:707) гейтит каждый дисциплинарный построитель — защита от междисциплинарных ложных профилей.

**Каскад `resolve_block_package` (:669)**, первый прошедший гейт побеждает:
1. prepared-пакет из `block_vector_graphs/` (`prefer_prepared=True`, с проверкой A/B-совместимости `profile_routing` :683-695);
2. **(ЭОМ) Вектограф-однолинейка** :801-819: `build_singleline_graph` + `evaluate_vectograf_gate` + `render_graph_etalon_markdown` (>200 симв.) → `structured_singleline`, profile `electrical_singleline`;
3. **(ЭОМ) остальное** :824-852: `electrical_geometry` (classify/build/gate/render), демот SINGLELINE→PANEL при ≥3 «QF n» (:835-837) → `structured_electrical`;
4. **ГП** :854-877 (`general_plan_geometry`) → `structured_general_plan`;
5. **АР** :879-919 (`architecture_geometry`; маркеры «КЛАДОЧ», «ФАСАД», «РАЗВЕРТК»…; учитывает per-block routing decision) → `structured_architecture` (+ в `package()` :786-796 к user_text добавляется «Полный точный текст полигона» — профиль это индекс, а не замена первичного источника);
6. **КЖ** :921-945 и **КМ** :949-972 (общий `structural_geometry`, разные classify) → `structured_structure`;
7. **ТХ** :974-998 (`technology_geometry`) → `structured_technology`;
8. **ВК раньше ОВ** :1000-1034 (водомерный узел лексически похож на гидравлику ОВ) → `structured_water`; затем **ОВ** :1036-1068 → `structured_hvac`;
9. **[НОВОЕ, незакоммичено] СС low_voltage** :1070-1129 (см. раздел 5) → `structured_alia_scheme`;
10. `block_text < 40` → `image_only` (:1131-1133);
11. **СС alia_scheme** :1135-1163 и **СС alia_remaining** :1165-1207 → `structured_alia_scheme`;
12. финальный **`raw_vector`** (:1209-1216): «Точный текст блока из вектор-слоя PDF» + `_TASK` (:103).

Каждая ветка обёрнута в try/except → None; ошибка чего угодно → `make_package(source_kind="error")` (:1217-1219). `resolve_block_source` (:1222) — совместимая обёртка `(user_text, source_kind)`.

**Per-block роутинг (A/B, только АИ):** флаг `STAGE01_PER_BLOCK_PROFILE_ROUTING_ENABLED` (:49, default OFF, читается per-call :81-90); применяется только когда `_storage_discipline_code` ∈ {AI, АИ} (:296-301). `_per_block_profile_route` (:412): приоритет штамп листа (`_sheet_title` :321) → лист 0.1 → block_type (`_canonical_graphic_block_type` :339 + `classify_graphic_profile` из stage_comparison/graphic_profiles) → контент → «≥2 десятичных угла = рампа» (:502-509). Маркеры в `_route_from_semantic_text` (:363): паркинг/рампа/уклон → `raw_vector`; отделка/развёртки/двери → `structured_architecture`; ведомости ссылочных/титульный → `raw_vector`. Решение кладётся в `classification.profile_routing` c `applied`.

**`vector_text_block_index` (:554)** — точный вектор-текст ВСЕХ image- и text-блоков за один проход PDF: `{block_id: {text, page, page_index, block_kind, source: "pdf_vector_text", source_file, router_eligible}}`; `router_eligible` = image-блок с текстом ≥40 симв. = ровно предикат роутера. Нужен детерминированным evidence-проверкам (text-блоки в document_graph — OCR-производные, не истина). Потребители: symbol-evidence `gemma_findings_only.py:1743-1745`, `codex_targeted_findings.py:792-796`. **`vector_covered_block_ids` (:645)** — подмножество eligible → гейт пропуска Gemma-стадии + страховочный placeholder-enrichment + карта соседей (`gemma_findings_only.py:1759-1761`).

## 3. ЛОКАЛИЗАЦИЯ — `profiled_graph_localization.py`

Это **русификация машинных кодов графа для Markdown/UI** (не координаты). JSON намеренно хранит стабильные машинные коды — контракт построителей и тестов; переводятся только отображения (docstring :1-6). Словари: `SOURCE_LABELS` (:12), `PROFILE_LABELS` (:31), `CONTAINER_TYPE_LABELS` (:143), `NODE_TYPE_LABELS` (:172, ~150 типов узлов), `NETWORK_TYPE_LABELS` (:259), `STATE_LABELS` (:287, evidence-состояния: `path_confirmed`, `spatial_inventory`, `legend_only`…), `EDGE_TYPE_LABELS` (:329), `SUBTYPE_LABELS` (:360).

Контракт: `_label` (:380) — если значение уже русское, вернуть как есть; незнакомый код → нейтральный fallback (не выдумывает). Публичные `ru_source/ru_profile/ru_container_type/ru_node_type/ru_network_type/ru_state/ru_edge_type/ru_subtype` (:392-435); `node_state_needs_review` (:425) — скрывает рутинные состояния (`present`, `engineering_annotation`, `confirmed_by_floor_count_position`); `package_display` (:438) — компактная русская проекция пакета для таблиц UI (containers/nodes/networks/edges + `nodes_review_total`), большой машинный граф в HTTP-ответ не дублируется.

## 4. ШАДОУ/ГЕЙТ

**`vectograf_shadow.py`** — observe-only (:1-11): `write_vectograf_shadow(version_dir)` (:27) проходит по image-блокам `02_work/result.json`; `pdfplumber_text < 200` (`_MIN_VECTOR_LEN` :24) → сразу «вектор-слой мал»; иначе `vectograf_gate_for_block`. Пишет `_output/vectograf_shadow.json` (schema 1): `blocks_total`, `would_use_vectograf`, `would_use_block_ids`, per-block `{use, reasons, warnings, metrics}`. Атомарная запись tmp→replace (:64-66), fail-soft → None. Ничего в пайплайне не меняет — оценка точности гейта ПЕРЕД реальной заменой описаний.

**`evaluate_vectograf_gate`** (`singleline_graph_geometry.py:2499`) — «годится ли граф как замена описания блока», подобран по 15 боевым однолинейкам: блокирующие критерии — линий ≥5; физика P ≥0.8 (пересчёт P=√3·U·I·cosφ); честная привязка active/(active+ambiguous) ≥0.85; геом. конфликтов ≤15%. coverage <60% и `needs_review` — только warnings (не блокируют: дубли токенов/коды чужих панелей). `vectograf_gate_for_block` (:2544) — вход по result.json (`pdfplumber_text` + bbox/polygon клип, кэш `_result_blocks_vector_index` :2477).

## 5. НЕЗАКОММИЧЕННОЕ (diff HEAD: +444/−1 в 7 файлах)

Тема ветки: **специализированные СС-грамматики (АПС/лотки/клеммы) как полноправные CTX-профили + sidecar-графы для галерей ревью.**

1. **`low_voltage_geometry.py` +219** (файл существовал: коммит d364cf2e «feat(block-graphs)»):
   - `PROFILE_IDS_BY_SUBTYPE` (:24) + `profile_id_for_subtype` (:32): subtype→профиль: `aps_structural`/`aps_fragment`→`fire_alarm_loop_topology`, `tray_axonometry`→`cable_tray_axonometry`, `terminal_wiring`→`low_voltage_terminal_wiring`; иначе базовый `low_voltage_scheme`.
   - **`normalize_low_voltage_graph` (:632-835)** — главное добавление: универсальная проекция `containers/nodes/networks/edges` ПОВЕРХ специализированных полей (`loops/devices/elements/components` остаются источником истины; связность, которой нет в PDF, не достраивается). Для АПС: узлы root (`fire_alarm_control_panel`) + шлейфы + этажи + адресные устройства; контейнеры-шлейфы и контейнеры-этажи с member_ids; networks по шлейфам (`path_state: semantic_code_confirmed`); рёбра `contains_loop` / `serves_floor` / `contains_device`; старые edges переносятся в `hierarchy_edges`; `readiness.complete` только для `aps_structural` со status ok. Для лотков: инвентарь-узлы (`tray_callout`/`conduit_group` с собранными подписями «лоток 200×100 мм, L=…»), edges=[], `topology_partial`. Для клемм: узлы-аппараты, edges=[], `topology_partial`. В конце — `validation.{containers,nodes,networks,edges}_total` и подмена `graph["profile_id"]` на подтиповый.
2. **`block_source_router.py` +61** — новая ветка каскада (:1070-1129) МЕЖДУ ОВ и image_only-проверкой, раньше общих ALIA-профилей (комментарий: «этот построитель точнее»). Особый случай **whole-page блока** (:1076-1090): bbox≈(0,0,1,1) без полигона → в построитель уходит `page_text` (сохраняет порядок многострочных выносок лучше реконструкции из words) и `bbox_norm=None`; реальные полигоны — только `block_text` (соседняя схема не протекает). Условие: `allows("СС")` и текст ≥40. Пайплайн: `build_low_voltage_graph` → `evaluate_low_voltage_gate` (режимы: `hierarchy` ≥10 адресов + этажная привязка ≥95% + типы ≥90%; `address_inventory`; `inventory_only` ≥3 элементов; `confirmed_connections_only` — low_voltage_geometry.py:896-956) → `normalize_low_voltage_graph` → `profile_id_for_subtype` → рендер >150 симв. → пакет `source_kind="structured_alia_scheme"`, discipline СС, source `vector_block_pdf`.
3. **`block_profile_registry.py` +16** — `load_prepared_package` (:453-468) понимает `graph_artifact`: пакет с `graph=None` подгружает граф из относительного sidecar под `block_vector_graphs/` (галереи ревью хранят иммутабельный граф hard-link'ом в `_graphs/` вместо дублирования). Защита от traversal: resolved-путь обязан лежать под каталогом артефактов, иначе None.
4. **`profiled_graph_localization.py` +11** — русские подписи для нового: 3 профиля (:133-135), контейнеры `fire_alarm_loop`/`floor` (:154-155), узлы `fire_alarm_control_panel`/`fire_alarm_loop`/`addressable_fire_alarm_device` (:253-255), рёбра `contains_loop`/`serves_floor`/`contains_device` (:351-353).
5. **Тесты:**
   - `tests/test_block_profile_registry.py` +45: round-trip sidecar (`test_prepared_package_loads_safe_graph_sidecar`) и отказ от sidecar вне каталога (`../outside.json` → None);
   - `tests/test_block_source_router_profile_routing.py` +69: `test_resolver_routes_specialized_low_voltage_graph_to_ctx_profile` — monkeypatch `_locate/_extract_block/build_low_voltage_graph`; проверяет `source_kind=structured_alia_scheme`, `profile_id=fire_alarm_loop_topology` (и в пакете, и в графе), непустые nodes/networks/edges, `classification.source=vector_block_pdf`, и что при whole-page bbox в билдер ушёл `page_text` с `bbox_norm=None`;
   - `tests/test_low_voltage_geometry.py` +24: маппинг подтип→профиль; на реальных PDF-корпусах (skipif) — после normalize: nodes = 1+loops+floors+devices, networks=loops, validation totals; для лотков nodes=elements=10, edges=[], readiness.complete=False.
6. **Смежное untracked:** `backend/scripts/build_vector_graph_gallery.py` — строит UI-галерею из проверенного корпуса `experiments/блоки разных дисциплин` в POS-документы объекта ALIA (`projects_v2/objects/214_Alia_ASTERUS/disciplines/POS`): WebP-превью + hard-link графов в `block_vector_graphs/_graphs/` и запись `package["graph_artifact"]` (строки 400, 433-436, 489-492) — это и есть потребитель нового кода `load_prepared_package`. Использует `resolve_block_package` (строки 191, 223).

## 6. ТОЧКИ ВСТРАИВАНИЯ

**Stage 02 (анализ блоков, «01 Блоки»):**
- Главный цикл `gemma_findings_only.py:1970-1977` — на каждый блок один `resolve_block_package(output_dir, block_id, page, prefer_prepared=False)` (refresh роутинга при смене маппингов); из пакета: `routed_context=(user_text, source_kind)` (:1978-1981), `retrieval_query_text` из `classification.block_title` (:1982-1987), пакет уходит в детерминированный детектор `run_protection_table_detector` (:1992-2000) и в `retrieve_document_context` (:2005-2013).
- Сборка промпта — `build_effective_block_context` (`gemma_findings_only.py:886`, тело :905-945): `routed_context` **затирает** базовый `build_block_user_text` (:683); поверх при `STAGE01_PAGE_CONTEXT_ENABLED` добавляются `_page_context_data_section` (текст листа + соседи) и анти-FP оговорка. Fallback-путь без routed_context вызывает `resolve_block_source` напрямую (:924-928). Legacy-инъекции `SINGLELINE_RICH_PROMPT_ENABLED` (:953-966) и `MIRROR_OCR_ENABLED` (:968+) работают только когда роутер НЕ применился.
- Symbol-evidence / gap-детекторы: `vector_text_block_index` в `gemma_findings_only.py:1743-1745` и `codex_targeted_findings.py:792-796`; карта соседей `vector_covered_block_ids` в :1759-1761.
- Сам текст графа в промпте = `user_text` пакета: `head("# Блок …") + structured markdown (render_*_markdown) + _TASK` (block_source_router.py:103-108, ветки каскада).

**Фронтенд (`/blocks/llm-text`):**
- `GET /{project_id}/blocks/llm-text/{block_id}` — `backend/app/api/routers/blocks.py:655` (`get_block_llm_text`): вызывает `resolve_block_package` (:811-813) и `package_display` (:814-817) → поля ответа `block_graph_package` / `profiled_graph_display`; `user_text` превью = роутерный текст (правдивый payload Stage 02, :818-827); prepared singleline-граф переиспользуется без rebuild (:831-838); плюс `structured_graph` (singleline_structurer), `singleline_graph` (геометрия), `singleline_graph_markdown` (:843-848), `text_groups` для оверлея областей (:780-794). На фронте это панель «🔌 Граф схемы» в окне «txt» (`frontend/index.html` + `frontend/static/js/app.js`, docs/vectograf.md; вкладка CTX-графа берёт `profiled_graph_display`).
- Прочие потребители пакетов: builder контекста `block_context/builder.py:19-26` (канонический resolve + shim), галерея `backend/scripts/build_vector_graph_gallery.py`.

**Наблюдение:** в текущем рабочем дереве ветка «СС low_voltage» стоит ДО проверки `image_only`, но сама требует текст ≥40 симв. — для whole-page блоков с пустым `block_text`, но непустым `page_text`, она может сработать там, где раньше блок ушёл бы в `image_only`; это осознанное изменение (тест фиксирует именно этот сценарий).