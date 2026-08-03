Отчёт собран. Ниже — итог инвентаризации по прочитанным файлам.

---

# Инвентаризация галереи эталонов: АР, КЖ, КМ, ГП, ТХ

## 0. Общая рамка галереи

- Папка `experiments/блоки разных дисциплин/` — **исследовательский архив, пайплайн её не читает** (README.md корня папки). Производственная копия правил/профилей/компакт-примеров перенесена в `backend/app/pipeline/stages/block_context/reference_catalog` (manifest `2026.07.13-1`); сама папка помечена в `DELETE_CANDIDATE.md` как кандидат на отложенное удаление (~2 ГБ, 3700 файлов), удалять нельзя без ручного решения.
- **Код-строители графов живут в проде**: `backend/app/pipeline/stages/block_grounding/architecture_geometry.py` (АР), `structural_geometry.py` (КЖ+КМ), `general_plan_geometry.py` (ГП), `technology_geometry.py` (ТХ); общий каркас — `hvac_geometry.py` (`_node`:129, `_unique`:136, `_components`:170, `_axes`:219, `_bind_nearest_axes`:237, `_base`+semantic_ledger:245–270, `_views`:464, `_assign_nodes_to_views`:473).
- `DISCIPLINE_COVERAGE.json` (schema_version 2): 9 дисциплин, 1133 блока, 983 полных эталона, 124 ограниченных, 26 кандидатов, 0 непокрытых каталогов. Запись блока: `block_id, profile_id, subtype, status (reference_complete|reference_limited|candidate), gate_use, complete, issues, text_characters, drawing_paths, embedded_images`.
- Критерий полного эталона (`DISCIPLINE_COVERAGE.md`): одностраничный векторный PDF с координатами исходного блока + ссылка на источник + предметный JSON-граф + русский Markdown + пройденный профильный гейт. Chandra-описание используется **только для выбора профиля**, извлечённый Chandra-текст игнорируется (AR_PROFILES.md:55–57); узлы/рёбра — только по вектор-слою PDF.

Сводка пяти строительных дисциплин:

| Дисц. | Блоков | Полных | Огранич. | Кандидатов | Комплектов | Профилей | Подтипов | Сем. пропуски |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| АР | 247 | 241 | 6 | 0 | 57/57 | 20 (21 в PROFILES.md) | 26–27 | 0 |
| КЖ | 110 | 110 | 0 | 0 | 25/25 | 6 | 14 | 0 |
| КМ | 71 | 68 | 3 | 0 | 17/17 | 7 | 11 | 0 |
| ГП | 47 | 47 | 0 | 0 | 2/2 | 13 | 16 | 0 |
| ТХ | 35 | 32 | 3 | 0 | 7/7 | 7–8 | 9 | 0 |

Расхождение АР: `AR_PROFILES.md`/`AR_GRAPH_AUDIT.md` заявляют 21 профиль/27 подтипов (добавлен `ar_floor_wall_junction_detail`, есть в `AR_DIVERSE_CORPUS.json:4335`), а `DISCIPLINE_COVERAGE.json` от 07-12 фиксирует 20/26 — coverage-файл на день старше.

---

## 1. АР — 247 эталонов

### 1.1 Таксономия (профиль / подтип: кол-во в корпусе)

Планы (PLAN_PROFILES, architecture_geometry.py:20–21): `ar_masonry_plan`/masonry_plan 12; `ar_marking_plan` 10; `ar_floor_plan`/architectural_plan 8; `ar_opening_plan`/wall_opening_plan 6; `ar_roof_plan` 8; `ar_finish_plan` (apartment_finish_fragment 12 + finish_plan_or_detail 12) 24; `ar_floor_finish_plan` 4; `ar_ceiling_plan` (ceiling_and_lighting 10 + ceiling_lighting_layout 8) 18; `ar_interior_electrical_plan`/socket_and_equipment_layout 10; `ar_furniture_plan` 8; `ar_equipment_foundation`/roof_equipment_foundation 8.
Виды (ELEVATION_PROFILES:22): `ar_facade`/facade_elevation 8; `ar_section` (building_section 8 + masonry_section 8) 16; `ar_wall_elevation` (wall_elevation 15 + wall_lighting_layout 6) 21.
Детали (DETAIL_PROFILES:23): `ar_detail` (architectural_detail 14, facade_detail 8, marking_detail 12) 34; `ar_masonry_detail` 12; `ar_roof_detail` 13; `ar_stair_drawing`/stair_plan_section 11; `ar_railing_detail` 8; `ar_opening_drawing`/door_window_sketch 8; `ar_floor_wall_junction_detail` (21-й профиль).
**Ведомостей/спецификаций как отдельного профиля НЕТ**: «ВЕДОМОСТЬ ДВЕР»/«ТАБЛИЦА ДВЕР» роутится в `ar_opening_drawing` (classify_ar_profile, architecture_geometry.py:36–40).

### 1.2 Якорные точки сейчас

12 типов узлов через регексы `PATTERNS` (architecture_geometry.py:76–93): `room` (квартира №N, санузел, коридор…), `opening` (Д/ДПМ/ОК/ЛЮК-N), `elevation` (`[+\-]N,NNN`:79), `dimension` (`AxB`, `R=N`, `N мм`:80; для развёрток/примыканий ещё голые числа 2–5 цифр:109–110), `material`, `construction_element`, `layer_count`, `installation_requirement`, `finish_mark` (СТ/ПЛ/ПТ-N), `furniture_or_equipment`+`furniture_mark` (М-N), `electrical_fixture`, `roof_element`, `stair_or_railing`, `fire_rating` (EI/REI N), `fastener`; плюс `raster_region` (крупные растры:124–131). Оси: `_axes` (hvac_geometry.py:219–234) — повторяющиеся короткие метки, выстроенные в линию → `graph["grid"].axes`; в примере 7DU7 детектор дал **ложную ось «1000»** (повторяющийся размер).

### 1.3 Что связывается / что плоское (главный раздел)

Факт по всем 247 structure.json: **рёбра есть только в 5 файлах, все 12 рёбер — «порядок слоёв» в узлах кровли** (сортировка material-узлов по y при |Δx|<0.4·ширины, architecture_geometry.py:144–147). Всё остальное — плоские типизированные узлы:
- **Размеры НЕ связаны с элементами**: 935 dimension-узлов корпуса — это текст+центр bbox; размерные/выносные линии и полочки не парсятся вообще, ребра размер→стена/проём нет нигде.
- **Выноски НЕ связаны**: механизм чтения выносок отсутствует в коде АР полностью.
- **Толщины** — просто dimension или кусок label материала (пример PDT9: «40мм» отдельный узел из строки «(керамзитовый гравий фр.20-40мм…)»), связь слой→толщина не создаётся.
- Что связывается: (а) узел→вид: ближайший евклидово anchor подписи вида (`_assign_nodes_to_views`, hvac_geometry.py:473–479 — не по рамке вида!); (б) `nearest_room_label`/`room_binding_state` на планах: ближайшая room-подпись при дистанции <20% габарита (`_nearest_room`, architecture_geometry.py:118–123) — явно помечено «пространственно ближайшая подпись», не полигон; (в) слоевые рёбра кровли. Оси в grid ни к чему не привязаны (`_bind_nearest_axes` в АР не вызывается).
- Дополнительно у каждого графа `semantic_ledger` — **полный плоский реестр всех текстовых строк с координатами** (hvac_geometry.py:253–261), заявленная гарантия полноты (AR_SEMANTIC_COVERAGE.md: 12 категорий, 2643 подписи, 0 пропусков: помещения 234, проёмы 843, отметки 715, размеры 528, материалы 169, огнестойкость 59 и т.д.).
- Качество распознавания узлов не идеально: в 7DU7 узел node-1 label «Данный» типа opening — ложное срабатывание регекса «Д…» на примечании «1. Данный лист см. совместно…».

### 1.4 Ограниченные эталоны

6 reference_limited, все по причине `readiness.reasons=["нет текстового слоя PDF"]` (evaluate_ar_gate, architecture_geometry.py:186–191): 3× ar_detail/architectural_detail (4YQN-UY9J-QJE, DE4M-VPVF-WXE, 7DCJ-DHFN-RWK), 2× ar_roof_detail (7ADT-Q73X-66Q, UFGL-X3FG-RGJ), 1× ar_roof_plan (6VCK-CHYD-L77). Кандидатов 0.

### 1.5 Формат structure.json — фактические примеры

Схема (все дисциплины, `_base` hvac_geometry.py:245–270): `schema_version:1, profile_id, source{pdf_file,page_index,block_id}, containers[], nodes[], networks[], edges[], semantic_ledger[], validation{...node_types, topology_state, description_depth, coordinate_text_*, source_layer_state}, warnings[], status, secondary_description{text, evidence_state:"без координат", warning:"не участвует в рёбрах"}, readiness{complete,reasons}`. Узел: `{id, label, node_type, x, y, bbox_page(норм.), container_ids[], field_state:"present", source_label(полная строка-источник)}`.

| Пример | Профиль | Узлы | Контейнеры | Рёбра | Ledger |
|---|---|---:|---:|---:|---:|
| 7DU7-346V-DN6 | ar_masonry_plan (план) | 72 (elevation 51, dimension 10, material 5, opening 4, room 2) | 0 | 0 | 534 |
| 49XE-EHU3-Y3V | ar_facade (вид) | 80 (все elevation!) | 2 (drawing_view+architectural_view) | 0 | 259 |
| PDT9-6WQK-3PR | ar_roof_detail (узел) | 34 (elevation 29, dimension 2, material 1, roof_element 1) | 1 | 0 | 196 |
| 67AU-N79T-VUA | ar_opening_drawing (эскиз/ведомость дверей) | 15 (opening 8 «Д-16», fire_rating 7 «EI30») | 1 | 0 | 87 |

Показательно: фасад = 80 отметок без единой связи; ведомость дверей = пары Д-N/EI30 рядом по координатам, но пара «дверь↔огнестойкость» ребром не оформлена. `.structure.md` (render_ar_markdown:205–230) — дерево счётчиков, состав по типам, список видов, secondary_description (Qwen-описание без координат, «не создаёт узлы и рёбра»), ограничения.

---

## 2. КЖ — 110 эталонов

### 2.1 Таксономия
6 профилей / 14 подтипов (structural_geometry.py:7): `kj_reinforcement_plan` — 8 подтипов (wall_reinforcement_plan 10, slab_lower/upper_reinforcement 8+8, beam_reinforcement 8, column_reinforcement 8, bent_bar_layout 6, starter_bars 6, opening_reinforcement 2) = 56; `kj_reinforcement_section` 12; `kj_formwork_plan` 10; `kj_marking_plan` 8; `kj_embedded_parts` (detail 8 + layout 8) 16; `kj_structural_detail` 8. Ведомостей расхода стали/спецификаций как профиля нет.

### 2.2 Якорные точки
`PATTERNS` (structural_geometry.py:12–27): `structural_mark` (Стм/Км/Пм/Бм/ЗД-N:13), `rebar` (Ø12 А500С:14), `position` (поз. N:15), `spacing` (шаг 200:16), `elevation` (±N,NNN:17), `dimension` (AxB[xC], N мм:18), `concrete` (В25 W6 F150:19), `cover` (защитный слой…:20), `embedded`, `section_mark` (разрез/сечение N-N:22). Оси считаются (`_axes`) и кладутся в `grid`, к узлам не привязываются.

### 2.3 Связи / плоское
**0 рёбер и 0 networks во всех 110 файлах.** Агрегат узлов: spacing 1205, elevation 1055, structural_mark 900, dimension 105, embedded 12, cover 4, concrete 3, rebar 2. Т.е. арматурный план = облако «шагов» и марок с координатами. Связывание: только узел→вид для сечений/деталей (containers `structural_view`, _dispatch:75–76). **Позиция↔стержень, шаг↔сетка, размер↔элемент — не связываются принципиально**; это зафиксировано и в KJ_PROFILES.md («связь "позиция—стержень" не создаётся без выноски или отдельного правила») и в warning каждого графа: «арматурные марки и позиции сохраняются по координатам; попарная связь не создаётся без выноски или отдельного правила» (structural_geometry.py:82) — **слово «выноска» названо как будущий механизм, но код выносок не читает**. Семантический аудит `KJ_SEMANTIC_COVERAGE.json`: 0 пропусков по 14 категориям (structural_mark 142, spacing 76, elevation 147, dimension 44…).

### 2.4 Ограничения
0 limited, 0 кандидатов — единственная «идеально чистая» дисциплина вместе с ГП.

---

## 3. КМ — 71 эталон

### 3.1 Таксономия
7 профилей / 11 подтипов (structural_geometry.py:9): `km_layout_plan`/balcony_or_frame_layout 8; `km_member_drawing`/support_or_equipment_frame 3; `km_connection_detail`/steel_connection 8; `km_ladder_drawing` 8; `km_facade_layout` (bracket 8, cladding 6, guide 4) 18; `km_facade_detail` (connection 8, insulation 2) 10; `km_mockup` (detail 8, layout 8) 16. «СПЕЦИФИКАЦ…ЭЛЕМЕНТ/ПРОФИЛ» роутится в `km_member_drawing` (classify_km_profile:46), отдельного профиля таблиц нет.

### 3.2 Якорные точки
Те же структурные PATTERNS + металл: `steel_profile` (L/HEA/HEB/IPE/Швеллер/Труба-N:23), `bolt` (болт/анкер МN:24), `weld` (катет N:25), `plate` (лист AxB:26). Агрегат: elevation 776, structural_mark 138, dimension 112, steel_profile 20, position 16, bolt 8, spacing 3, embedded 5.

### 3.3 Связи / плоское
**0 рёбер, 0 networks.** Только узел→вид (containers structural_view 15 + drawing_view 61). Болт↔пластина, шов↔соединение, размер↔профиль — не связываются. Особенности КМ: физические сегменты **не извлекаются вовсе** (`segments=[]` для km_, structural_geometry.py:74), `vector_geometry_state:"preserved_not_expanded"` — тяжёлые штриховки НВФ не разворачиваются (KM_PROFILES.md); при полном отсутствии подписей создаётся единственный узел `vector_geometry_region` «векторный PDF без распознанных подписей» (:71–73, 13 шт. в корпусе).

### 3.4 Ограничения
3 reference_limited (нет текст-слоя): 6UMT-JJNU-MKW (steel_connection), 6T6U-XCJF-QJG (support_or_equipment_frame), V9UQ-ELYF-PXV (mockup_detail). Кандидатов 0.

---

## 4. ГП — 47 эталонов (самая «связная» из пяти)

### 4.1 Таксономия
13 профилей / 16 подтипов (general_plan_geometry.py:11–21): планы — `gp_axis_plan` (building_axis_composition 1, site_axis_layout 1), `gp_general_plan`/master_plan 1, `gp_stakeout_plan`/stakeout_fragment 4, `gp_grading_plan` 1, `gp_earthwork_plan`/cut_fill_grid 1, `gp_pavement_plan`/surface_zoning 1, `gp_surface_layout` (tile 1, rubber 1), `gp_small_forms_plan` 2, `gp_drainage_plan` (location 1, network 1); физические — `gp_grading_detail` 1, `gp_road_structure`/road_pavement_section **24**, `gp_drainage_profile` 2, `gp_drainage_detail` 4. 24 дорожные одежды **вручную восстановлены** с листа, который детектор блоков целиком пропустил (GP_GRAPH_AUDIT.md «Особо важное восстановление»).

### 4.2 Якорные точки
`PATTERNS` (:46–59): `building` (корпус №N/К1-7), `floor_count` (N эт.), `elevation` (в т.ч. без знака NN,NN:49), `coordinate` (X=/Y=:50), `radius` (R=:51), `slope` (i=, ‰:52), `dimension`, `area_or_volume` (м²/м³), `surface` (асфальтобетон/брусчатка/газон/щебень…), `small_form` (МАФ-N, скамья…), `drainage` (DN N, лоток, пескоуловитель, CompoMax), `earthwork`. Плюс синтетические узлы: `building_footprint` «здание с подписью „N эт.“» по позиции этажности, номер не выдумывается (:124–126, field_state=confirmed_by_floor_count_position); `earthwork_cell` Δ=±N из знаковой отметки на плане земляных масс (:128–131) — 214 ячеек.

### 4.3 Связи / плоское
Единственная дисциплина пятёрки с реальной геометрической топологией:
- **networks**: 7445 `site_geometry_component` — непрерывные CAD-компоненты цветных линий (`_inside_segments`:82–103 → union-find по общим конечным точкам `_components` hvac_geometry.py:170–195), с length/branch_points/endpoint_count; узлы-подписи цепляются к компоненте, если до сегмента ≤30 pt (`_attach`:106–116, поля `route_id`,`route_distance`). Доказательная база: «пересечение без общей конечной точки не создаёт связь» (GP_PROFILES.md).
- **edges**: 160 рёбер «последовательность слоёв» в 24 дорожных одеждах — вертикальная сортировка surface-узлов (:165–169, edge_state=vertical_order_in_section). Штриховки (>120 тыс. путей) сознательно не разворачиваются (:155–159, vector_hatching_state).
- Уровни по gate (evaluate_gp_gate:218–226): engineering_graph если у сети есть подписи-endpoint'ы — итог 15 инженерных графов, 1 предметная иерархия, 31 физическая структура.
- **Но размеры/радиусы/уклоны/отметки — по-прежнему плоские узлы**: elevation 1743, radius 400, dimension 292 без связи с конкретной дорогой/бортом; связь «размер↔элемент» и здесь отсутствует; толщина слоя дорожной одежды остаётся отдельным dimension-узлом рядом с material-узлом (пример GP-ROAD-11: «15м»≈«0,15 м» — регекс даже съел запятую: source_label «,45м» → label «45м»).
- Семантика: 0 пропусков (этажность 73, отметки 792, координаты 68, радиусы 79, размеры 247, материалы 153, водоотвод 32); **uklony=0 и площади=0 в coverage-аудите** — категории объявлены, но в двух комплектах не встретились.

### 4.4 Ограничения
0 limited, 0 кандидатов; но исходников всего **2 уникальных комплекта** (4 result-файла = 2 пары дублей) — статистическая база ГП самая узкая.

---

## 5. ТХ — 35 эталонов

### 5.1 Таксономия
8 профилей объявлено, 7 представлено / 9 подтипов (technology_geometry.py:7–8): `tx_parking_plan`/parking_space_layout 4; `tx_parking_detail` (marking_and_safety 4, ramp_plan_section 2); `tx_lift_plan`/lift_shaft_plan 7; `tx_lift_section`/lift_vertical_section 7; `tx_lift_equipment` (shaft_equipment 4, door_drawing 1); `tx_lift_assignment`/lift_construction_assignment 4 (строительное задание); `tx_waste_detail` 2; `tx_waste_plan` объявлен, в корпусе отсутствует.

### 5.2 Якорные точки
`PATTERNS` (:9–14): `lift` (Л N.N[П/Г]), `parking_space` (м/м №N), `accessibility` (МГН), `parking_device` (колесоотбойник/демпфер/знак/разметка), `shaft`, `lift_device` (кабина/противовес/лебёдка/буфер), `capacity` (N кг/чел.), `speed` (N м/с), `elevation`, `dimension` (AxB), `fire_rating` (EI/EIS N), `waste_device` (мусоропровод/ствол/клапан/шибер).

### 5.3 Связи / плоское
**0 рёбер, 0 networks во всех 35.** Только узел→вид для не-плановых профилей (technology_view 17 + drawing_view 8). Warning каждого графа: «положение оборудования сохраняется по координатам; технологическая связь не создаётся только по соседству» (:48). Грузоподъёмность↔лифт, EI↔дверь шахты, размер↔машиноместо — не связываются. Агрегат: elevation 206, lift 51, fire_rating 36, parking_device 32, capacity 22, dimension 9. Coverage: 0 пропусков; `speed` source=0 (категория пустая).

### 5.4 Ограничения
3 reference_limited (нет текст-слоя): 7DU4-MNDM-DVN и 7LYE-PJ63-77W (lift_construction_assignment), JAMC-66UX-F6T (lift_door_drawing). Кандидатов 0.

---

## 6. Сквозной вывод по размерам/выноскам (ответ на главный вопрос)

1. **Ни в одной из пяти дисциплин размер не связан с измеряемым элементом.** Все dimension/spacing/elevation/radius/slope-узлы — регекс-совпадения строк текст-слоя с центром bbox (`_facts`: architecture_geometry.py:103, structural_geometry.py:58, general_plan_geometry.py:72, technology_geometry.py:36). Размерные линии, выносные линии, полочки-выноски, стрелки — **не детектируются нигде**; физические отрезки либо складываются нерасчленённой кучей `physical_line_segments_total`, либо (КМ, дорожные одежды ГП) не извлекаются вовсе.
2. Существующие механизмы связывания — только 5, все дешёвые: (а) node→view по ближайшему anchor подписи вида; (б) node→nearest_room (АР-планы, порог 20%); (в) node→CAD-компонента route_id (только ГП-планы, ≤30 pt); (г) синтетика ГП (building_footprint, earthwork_cell); (д) слоевые рёбра по вертикальному порядку (узлы кровли АР — 12 рёбер, дорожные одежды ГП — 160 рёбер). Итого на 510 графов пяти дисциплин — **172 ребра, из них 100% «порядок слоёв»**.
3. Компенсатор отсутствия связей — `semantic_ledger` (hvac_geometry.py:253–261): полный плоский реестр строк с координатами в каждом графе + семантические аудиты «0 пропусков». Полнота значений достигнута; **связность значений с элементами — нет, и это осознанная позиция** («неподтверждённые отношения не добавляются»), с прямым указателем на будущий механизм в warning КЖ: «…не создаётся без выноски или отдельного правила» (structural_geometry.py:82).
4. Оси как якорь недоиспользованы: `_axes` считается везде и кладётся в `grid`, но `_bind_nearest_axes` (hvac_geometry.py:237–242) в АР/КЖ/КМ/ТХ не вызывается; детектор осей даёт ложные срабатывания на повторяющихся размерах (ось «1000» в 7DU7-346V-DN6).
5. Профилей табличных блоков (ведомости, спецификации, экспликации) в этих пяти дисциплинах нет — табличные ключевые слова роутятся в графические профили (architecture_geometry.py:38, structural_geometry.py:46).

Ключевые файлы: `experiments/блоки разных дисциплин/{АР,КЖ,КМ,ГП,ТХ}/*_PROFILES.md, *_GRAPH_AUDIT.md, *_SEMANTIC_COVERAGE.{md,json}, README.md, DISCIPLINE_COVERAGE.{json,md}`; примеры: `АР/ar_out/{7DU7-346V-DN6,49XE-EHU3-Y3V,PDT9-6WQK-3PR,67AU-N79T-VUA}.structure.{json,md}`; код: `backend/app/pipeline/stages/block_grounding/{architecture_geometry.py,structural_geometry.py,general_plan_geometry.py,technology_geometry.py,hvac_geometry.py}`; прод-каталог: `backend/app/pipeline/stages/block_context/reference_catalog/`.