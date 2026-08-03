Отчёт собран. Ниже — инвентаризация галереи эталонов по четырём схемным дисциплинам.

# Инвентаризация галереи эталонов: СС, ОВ, ВК, ЭОМ

Корень: `/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/`. Статусы вычисляет `audit_all_disciplines.py:71-74`: `candidate` = профильный гейт не пройден (`gate_use=False`); `reference_complete` = гейт + `complete` + нет артефактных проблем; иначе `reference_limited`. Модель качества — `DISCIPLINE_COVERAGE.json → quality_model`. Итог (`DISCIPLINE_COVERAGE.md:5-10`): СС 144 (81/52/11), ОВ 154 (110/29/15), ВК 168 (141/27/0), ЭОМ 157 (153/4/0) — формат «полные/ограниченные/кандидаты»; семантических пропусков 0 во всех четырёх.

---

## 1. СС (слаботочные системы) — 144 блока из 34 комплектов

### 1.1 Таксономия (профиль → физический смысл → полн./огр./канд.)

**Корпус schemes (ALIA_SCHEME_PROFILES.md, 14 грамматик, реализация `backend/app/pipeline/stages/block_grounding/alia_scheme_geometry.py`):**

| Профиль | Что это физически | П/О/К |
|---|---|---|
| `cctv_floor_network` | структурная схема СОТ: корпус→этаж→ОСПД/камеры | 1/0/2 |
| `voice_alarm_line_topology` | структурная СОУЭ: шкаф→линии V→помещения/ST | 1/0/0 |
| `fiber_ring_backbone` | кольцо ВОК, сегменты L1–L9 между шкафами | 1/0/0 |
| `metering_floor_bus_water` / `_heat` | этажные шины АСКУВ/АСКУТ: ветвь→РИ/УСПД→счётчики | 2/0/0 + 2/0/0 |
| `automation_control_hierarchy` | иерархия автоматики (ШАУВ по системам) | 1/0/2 |
| `dispatch_integration_backbone` | backbone диспетчеризации: контроллер→ШАУВ→щит→ОСПД→АРМ | 1/0/1 |
| `lift_dispatch_floor_topology` | лифтовая диспетчеризация по шахтам | 1/0/0 |
| `mgn_intercom_floor_bus` | связь МГН: АПУ→РШ→щит→ОСПД→АРМ | 2/0/0 |
| `cabinet_commutation_graph` | коммутация шкафа (цветные сети между устройствами) | 4/0/0 |
| `cabinet_rack_layout` | физическая компоновка шкафа (порядок юнитов) | 4/0/0 |
| `functional_process_io` | функциональная схема техпроцесса (датчики/механизмы) | 1/0/0 |
| `external_terminal_wiring` | внешние подключения: полевое устройство→клеммник ХТ→ШАУВ | 3/0/5 |
| `multidiscipline_niche_layout` | организация ниш (гильзы, огнестойкость) | 1/0/0 |

**Корпус remaining (ALIA_REMAINING_PROFILES.md, 6 грамматик, `alia_remaining_geometry.py`):**

| Профиль | Что это | Подтипы | П/О/К |
|---|---|---|---|
| `discipline_floor_plan` | этажные планы СОТ/СОУЭ/лотков | cctv 3, voice_alarm 17, cable_tray 5 | 5/20/0 |
| `device_terminal_wiring` | схемы подключения устройств (клеммы/кабели) | device 17, aps/camera/metering/voice_alarm/access по 1 | 5/17/0 |
| `control_circuit_graph` | принципиальные схемы щитов (QF, клеммники, БП/UPS, контроллеры) | asud/control/metering_panel | 3/0/0 |
| `installation_assembly` | монтажные узлы (датчик, крепление, проходка, кабельный ввод) | assembly 34 + 4 спец. | 37/0/1 |
| `physical_panel_layout` | физические виды щитов/шкафов | cabinet 16 + 3 спец. | 4/15/0 |
| `access_point_assembly` | сборки точки доступа (калитка, врезной замок, 2 вида + выноски) | gate/mortise_lock/access_point | 3/0/0 |

**Отдельные исторические корпуса (не входят в 144):** `low_voltage_geometry.py` — диспетчер `low_voltage_scheme` с подпрофилями `aps_structural` (граф-иерархия АПС, валидация 16/16 + 27/27 `aps_fragment`, 9628 адресных точек — СС/README.md:79-95), `tray_axonometry` (аксонометрия лотков — инвентарь точный, топология `inventory_only`), `terminal_wiring` (клеммная СОВ: 111 клемм, 3 `confirmed_pair`, 2 `multi_terminal_review`); `structural_access_geometry.py` — `sov_structural_tower_pair` (2 корпуса, 7 этажных полос вкл. `shown_empty`), `sov_structural_multitower` (4 корпуса + control domains + external_references), `skud_structural_site` (участок: site→structures→access points; пожарные отсеки = overlays, не родители) — контракты в STRUCTURAL_PROFILES.md:26-259.

### 1.2 Якорные точки (regex/маркеры)

- АПС: адрес — главный якорь `_ADDRESS_RE` `low_voltage_geometry.py:35-38` (`9A2.144`, диапазоны `10SC1.53...56`, двойной адрес `1BTH1.165(107)`); тип устройства `_DEVICE_TYPE_RE`:41 (`МДУ-1С|АМ-4|ИЗ|РМ-1С|ШУН/В…`); шлейф выводится из самого адреса (`9A2.*`→`АЛС9.2`); этаж — ближайший Y-ряд; помещение — X-зона заголовка `пом. №…` (СС/README.md:99-115). Лотки: `_TRAY_RE`:50 (`Лестничный/Листовой лоток СБ|СПЗ|СЛЗ ШxВ, L=…м`), гильзы `_SLEEVE_RE`:57, кабели `_CABLE_RE`:76 (`КСПВПнг(А)-HF`, `U/UTP Cat5e…`).
- Структурные СОВ/СКУД: коды оборудования `structural_access_geometry.py:27-36` (`ОСПД\d+(\.\d+)+`, контроллер `К…`, `STR`, `DP`, `ТД`, `VP`, `UG`) + цветные сети (оранж. optical, пурпур RS-485 :38, синий Ethernet :42, зелёный периферия); СКУД :569-581 (`SW.\d`, `STR\d`, локации).
- ALIA schemes: СОТ `alia_scheme_geometry.py:323-327` (связь камера→шкаф по совпадению кодов `ВКb.d.* → ОСПДb.d`); АСКУВТ :423-430 (`РИ`, `УСПД "Пульсар"`, ветви `…-N.N (M счет.)` на CAD-шинах); АСУД :496-506 (`ШАУВ`, `ДР|LS|СУ|БД`, системы `Вж|Вкр|ПД…`); лифты/МГН :572-580; SONAR-позиции :623; техпроцесс :709-712 (`TE|TS|PDS|М|Y`); внешние подключения :759-761 (`ХТ\d+`, колоночная связь) ; ниши :800-801 (`\d шт. ВГП Ду=…мм`).
- Remaining: планы `alia_remaining_geometry.py:291-296` (оси + per-subtype: `ВК…/ОСПД`, `ST…/ШСОУЭ`, `СПЗ|СБ|ЛК|LOK`); клеммы :237, аппараты/кабели :363-365; щиты/цепи :145-148 (`QF\d`, `X(S)?\d`, `[AG]\d`, `UPS|ИБП|БП`).
- Общий принцип доказательности (ALIA_SCHEME_PROFILES.md:57-65): `semantic_confirmed`, `same_row_geometry`, `nearest_same_building_geometry`, `blue_path_component`/`colored_path_confirmed`, `multi_device_bus`, `physical_layout`, `x_order_geometry`/`column_alignment`. X-пересечение без path-конца НЕ ребро.

### 1.3 Ограниченные (52) — почему

Из `readiness.reasons` structure.json (ss_out): `device_terminal_wiring` ×17 — «менее 80% клемм привязано к проводам» + «нет подтверждённых пар» (не дотянута клеммно-проводная топология); `discipline_floor_plan` ×20 — «не построена координатная сетка» (14), «не найдены три фактических подключения шкафов к трассам» (16), «не построен граф трасс» (5), «камеры не связаны со шкафами по кабельным аннотациям» (2) — т.е. **топология трасс плана**; `physical_panel_layout` ×15 — «неполный состав щита». Текстовый слой у всех есть — ограничение именно топологическое.

### 1.4 Кандидаты (11) — чего не хватает

Все с нормальным текст-слоем; провал — **пороги профильного гейта, откалиброванные под эталонный вариант**: `external_terminal_wiring` ×5 (пример `4CYT-7XUU-P3J`: `field_devices_total: 3 < 10`, `connections_total: 3 < 10` — маленькие фрагменты той же грамматики; граф при этом `complete=True`); `cctv_floor_network` ×2 (<90% узлов привязано к корпусам, мало кодово подтверждённых связей камер с ОСПД); `automation_control_hierarchy` ×2 (<70% узлов в иерархии); `dispatch_integration_backbone` ×1 (не построен многоуровневый backbone; 141915 paths — очень плотный лист); `installation_assembly` ×1 (неполный состав сборки).

---

## 2. ОВ — 154 блока из 21 комплекта, 9 грамматик (`hvac_geometry.py`)

### 2.1 Таксономия

| Профиль | Физически | П/О/К | Ключевые подтипы |
|---|---|---|---|
| `hvac_floor_plan` | план этажа/кровли/паркинга | 46/0/0 | heating_floor 9, roof_ventilation 13, smoke_control_floor 9, ventilation_floor 8, ventilation_chamber 3 |
| `heating_axonometry` | аксонометрия отопления/теплоснабжения | 8/7/0 | heating_scheme 5, heat_supply_scheme 3, riser/manifold/parking по 1 |
| `ventilation_axonometry` | аксонометрия вентиляции/ДУ | 12/7/12 | smoke_control_scheme 15, ventilation_scheme 11 |
| `hydronic_principle` | принципиальная гидравлика (смесительные узлы) | 6/0/1 | hydronic_mixing_unit 4 |
| `hvac_installation_detail` | монтажный узел | 19/9/0 | fire_damper_detail 8, duct_penetration 5, insulation 4, pipe_support 3 |
| `hvac_section_layout` | разрез | 8/3/2 | duct_section 7, vent_chamber_section 2 |
| `hvac_equipment_drawing` | чертёж оборудования (AHU и т.п.) | 3/0/0 | ahu_exploded/section, heat_exchanger |
| `hvac_performance_chart` | рабочая характеристика | 7/3/0 | fan_curve 9, pump_curve 1 |
| `hvac_site_overview` | площадочная схема ИТП→корпуса | 1/0/0 | site_heat_source |

### 2.2 Якоря

`hvac_geometry.py`: планы :274-279 — системы `Т[12]x`/`В*|П*|ДУ*`, стояки `Ст.\d`, оборудование `КПУ|РКДМ|КДМ|КВК|КРК|SPL|VO-|KVR|AMP|CAV`, размеры `Ø\d{2,4}`/`ШxВ`, помещения по лексикону :279; отопление :314-319 — источник `ИТП`, приборы `РГ-|SPL|Kermi|КРК|ВТЗ`, арматура `BVR|MNF|RJIP|AB-QM|MSV|ASV|Danfoss`, отметка `[+\-]\d+.\d{3}`, этаж `L\d+_K\d+`; вентиляция :359-364 — `_VENT_SYSTEM_RE` (Вж/Ва/Пкр/ПД/ДУ…), огнестойкость `EI \d+`; гидравлика :409-413 — насос/теплообменник/коллектор/фильтр/приборы + позиционные выноски `^\d{1,2}$`; монтажный узел :447-458 — классы частей по лексике (труба/изоляция/крепёж/опора); разрезы :465, :502-504 (`Разрез|Вид|Узел|7-7`, отметки); оборудование :550-553 (модель, порты `F[1-4]|Вход|Выход|DN\d+`); график :571-573 (оси `Q|P|Pv|Ps|H|NPSH|η|КПД`); площадка :606-608 (`К\d`, `П.N-П.N`, ИТП). Связность плана — только по совпадающим концам CAD-отрезков (HVAC_PROFILES.md:31-33).

### 2.3 Ограниченные (29)

`heating_axonometry` ×7 — «не извлечены системы/стояки» + «не построена иерархия от источника» (марки скудны/нестандартны); `ventilation_axonometry` ×7 — «не извлечены вентсистемы»; `hvac_installation_detail` ×9 — «неполный состав монтажного узла»; `hvac_performance_chart` ×3 — «недостаточно числовых точек/параметров»; `hvac_section_layout` ×3 — «не найдены разрезы»/«неполная структура». Причина в основном — бедный или отсутствующий текст-слой (пример `9H9Y-AQHT-T6L`: text=0, 5578 paths).

### 2.4 Кандидаты (15)

12× `ventilation_axonometry` (6 smoke_control + 6 ventilation): «не извлечены вентсистемы» (8) и «не извлечена геометрия воздуховодов» (4); у 6 из 12 text_chars=0 (вектор без текст-слоя, напр. `CLLU-QK33-EC9` — 71926 paths, 0 текста), у остальных 135–503 символа — марок недостаточно для гейта. 2× `hvac_section_layout` (text=0 либо структура разреза не собралась), 1× `hydronic_principle` («менее трёх аппаратов» — слишком мелкий фрагмент).

---

## 3. ВК — 168 блоков из 33 комплектов, 11 грамматик (`water_geometry.py`)

### 3.1 Таксономия

| Профиль | Физически | П/О/К |
|---|---|---|
| `vk_floor_plan` | планы этажей/кровли/площадки | 27/6/0 |
| `water_supply_axonometry` | аксонометрия В1/В11/Т3/Т4 | 4/2/0 |
| `sewer_axonometry` | аксонометрия К1/К1н/К2/К2с/К13 | 19/4/0 |
| `fire_water_axonometry` | противопожарный водопровод/АПТ (В2.*, зоны) | 10/0/0 |
| `vk_principle_scheme` | принципиальные узлы (водомерные, редукционные, WDU) | 18/2/0 |
| `fire_control_scheme` | управление порошковым пожаротушением | 2/0/0 |
| `vk_installation_detail` | монтажные узлы (подвесы, проходки, гильзы, шкафы ШПК) | 32/8/0 |
| `roof_drain_detail` | узлы водосточных воронок/трапов | 8/2/0 |
| `external_network_profile` | продольные профили/камеры/разрезы наружных сетей | 9/2/0 |
| `pump_equipment_drawing` | чертежи насосного оборудования | 8/1/0 |
| `pump_performance_chart` | характеристики насосов | 4/0/0 |

Подтипов много уникальных (по одному блоку): напр. `storm_sewer_longitudinal_profile`, `hydrant_well_ladder`, `diaphragm_and_sprinkler`, «двухъярусный подвес трубопроводов» и т.д. (полный список — VK_DIVERSE_CORPUS.json → subtype).

### 3.2 Якоря

`water_geometry.py:94-120` — общий слой инженерных фактов: система `_SYSTEM_RE` (`В1|В2|В21|В11|Т3|Т4|К\d(н|с)?`), стояк `_RISER_RE` (`Ст.В/К/Т…`), отметка `_ELEV_RE`, диаметр `_DIAMETER_RE` (`Ø|Ду|DN`), уклон `_SLOPE_RE` (`i=0.0xx`), материал (`SML|ПЭ|ПНД|ВЧШГ|ПВХ|сталь|чугун`), продолжение `_CONTINUATION_RE` («далее см. лист/том»), лексиконы оборудования :110-120 (водомер, воронка/трап, арматура, насос `КНС|ДНС`, спринклер, изоляция, опоры, проходки, гидравлика `Q=|H=|м³/ч|МПа`). Планы :169-177 (`ШПК`, `Grundfos|Wilo`, `HL\d|PFO|VMCO`, помещения); узлы :268-274 (аппараты + позиция `^\d{1,2}$`); пожаротушение :311-316 (`МПТ|МПП|БКИ|С2000|Рубеж`, `ИПР`, клеммы `X|XT|КЛ`, кабели, питание 220/24/12В); профили :383-387 (колодец/камера `ВК-|ПГ-|ДК-`, пикет `ПК N+NN`, отметка земли). Правила: 2 аппарата на одной CAD-компоненте = `confirmed_pair`; многоточечная = общая цепь; факт только из OCR никогда не создаёт ребро (VK_PROFILES.md:18-21, 66-68).

### 3.3 Ограниченные (27) — одна причина

Все 27 — «**нет текстового слоя PDF: полнота марок, параметров и подписей не может быть подтверждена без OCR**» (вектор/растр без текста; часть — подписи вне кропа, README ВК:46-48). Геометрия трасс сохраняется полностью (`description_depth: geometry_inventory`), OCR-описание идёт во вторичный слой `secondary_facts` без координат. Кандидатов 0.

---

## 4. ЭОМ — 157 блоков из 22 комплектов, 12 грамматик

### 4.1 Таксономия

| Профиль | Физически | П/О/К | Подтипы |
|---|---|---|---|
| `electrical_singleline` | расчётная однолинейка / принципиальная ВРУ | 15/0/0 | calculation_singleline 13, principle_vru 2 — **строгий Вектограф** |
| `panel_circuit_scheme` | схемы щитов (этажные/квартирные, счётчики, диаграммы контактов) | 15/2/0 | floor_panel 8, apartment_panel 4 |
| `electrical_distribution_plan` | планы силовых сетей | 27/2/0 | building 8, parking/roof/common 4+4+4, розетки 3 |
| `lighting_plan` | планы освещения | 37/0/0 | building 8, координаты/маркировка/группы включения, кровля/паркинг/заградогни |
| `cable_route_plan` | планы кабельных трасс | 11/0/0 | лотки 5, огнезащитный короб 4 |
| `lightning_grounding_plan` | молниезащита и заземление | 11/0/0 | equipotential 4, roof 4, facade 2 |
| `equipotential_scheme` | СУП/ГЗШ/ДСУП | 1/0/0 | main_bonding |
| `ozds_topology` | ОЗДС (структура, подключения, планы) | 7/0/0 | device_wiring 4 |
| `electrical_installation_detail` | монтажные узлы | 11/0/0 | heating_cable 3, ozds_installation 3 |
| `electrical_equipment_drawing` | чертежи оборудования (УЭРВ, двери щитов, ВРУ) | 12/0/0 | floor_distribution_unit 5 |
| `illuminance_calculation` | светотехнический расчёт | 4/0/0 | calculation_map |
| `electrical_site_overview` | площадочная схема питания корпусов | 2/0/0 | building_power_context |

Однолинейки — `singleline_graph_geometry.py`+`singleline_structurer.py` (Вектограф), остальные 11 профилей — `electrical_geometry.py` (ЭОМ/README.md:14-15). Полный реестр объекта: 1745 блоков → корпус 157, 50 подтипов.

### 4.2 Якоря

`electrical_geometry.py:57-64` — базовые: щиты `_PANEL_RE` (`ВРУ|ГРЩ|ЩР|ЩО|ЩАО|ЩЭ|УЭРВ|ШР|ШУ|ЯУО|ГЗШ|ШДУ|ЩК|ЩМ`), аппараты `_DEVICE_RE` (`QF|QS|FU|KM|УЗО|АВДТ…`), цепи `_CIRCUIT_RE` (`Гр.N`, `К\d(.\d){1,3}`, `L1-3|N|PE|PEN`), кабели `_CABLE_RE` (`ВВГ|ППГ|NYM|N2XH|КВВГ|FRLS|нг(А)…`), отметки, сечения, мощности `кВт|А|В|кВА|лк`, огнестойкость `EI|FRLS|FRHF`. Профильные extra: освещение :136-140; щиты :166-168 (`Меркурий|ПСЧ`, шины); молниезащита :193-196 (молниеприёмник/токоотвод/заземлитель, `FeZn|Cu`); ОЗДС :217-219 (`ДР|ВУ|БП|БУ|ПУ`, электробарьер); светорасчёт :270-272 (`лк|lux`, модели `LED|ДПО|ДБО|ARLIGHT|VARTON|IEK`, `N шт.`); площадка :287-288. Вектограф: колонки фидеров по X-геометрии; `_QF_RE` `singleline_graph_geometry.py:712`, расчёты/КЗ/ТТ :756-769, классы аппаратов :1216-1220 (`QS|ВР|ВН`, `FU|ППН`, `УЗИП|ОП10x`, `РН|KV`, `HL`), учёт :1296 (`TA|Wh|НАРТИС|Меркурий|МТ-72`); формулы `Рр/Iр/cosф` — `singleline_structurer.py:36-71` (`BA_RE` ВА-серии, `KA_RE` «35кА 200А», `ROUTE_RE` «Лоток|Пг.|Каб.», `SECTION_RE` «L1,L2,L3»).

### 4.3 Ограниченные (4) и кандидаты (0)

Все 4 (`PVQW-637W-9L3`, `XN6M-XWHC-PRD`, `9WKW-LTVJ-4YU`, `FV7T-MNAR-ANC` — EOM_GRAPH_AUDIT.md:17-21) — «нет текстового слоя PDF: полнота подписей не подтверждена»; CAD-геометрия сохраняется, OCR-описание — в `secondary_description` без координат. Пограничные диалекты, зафиксированные отдельно: `4UJ9-3D93-W7A` (многоуровневая схема освещения — НЕ ослабляли гейт однолинейки), `GFEP-NT67-DEV` (план электрощитовой с упоминанием ВРУ — не выдаётся за однолинейку).

---

## 5. Фактический формат structure.json

### 5.1 Единый конверт (schema_version=1; ss_out/hvac_out/vk_out/eom_out)

```
{profile_id, source{pdf_file,page_index,block_id},
 containers[], nodes[], networks[], edges[], semantic_ledger[],
 validation{}, warnings[], status,
 readiness{complete, status, reasons[], description_depth, vectograph_level},
 + профильные секции: grid{axes}, route_segments[{id,color,p1,p2,length}],
   rooms, external_references, connections, secondary_description/secondary_facts}
```

- **Узел**: `{id, label, node_type, x, y, bbox_page[4 норм.], container_ids[], field_state, route_id, route_distance, nearest_vertical_axis, source_label}` (пример lighting_plan `4EMR-WNDK-X9J`: 261 узел, node_type = circuit/electrical_value/panel/size/elevation; привязка к трассе через `route_id` + дистанция).
- **Сеть (network)**: `{id, network_type, label, endpoint_ids[], path_state, segment_ids/segment_indexes, length, branch_points, endpoint_count, source_route_id}` — связная CAD-компонента по совпадающим концам отрезков.
- **Ребро**: `{id, network_id, from, to, edge_type, edge_state}` — создаётся ТОЛЬКО при доказательстве.
- **semantic_ledger**: полный координатный реестр строк текст-слоя `{id, text, x, y, bbox_page, evidence_state}` — факты вне графа, рёбер не создают.
- **field_state** (наблюдаемые по 4 корпусам): `present` (34402), `geometry_only` (6378), `engineering_annotation` (304), `raster_mosaic_only` (147), `raster_geometry_only` (121), `inferred_from_system_scheme` (12).
- **edge_state** (топ): `nearest_geometry` 3966, `same_cad_component` 909, `blue_path_component` 490, `semantic_system_source` 428, `nearest_system_geometry` 246, `nearest_same_building_geometry` 197, `path_confirmed` 172, `same_row_geometry` 164, `column_alignment` 81, `semantic_confirmed` 60, `colored_path_confirmed` 53 и др. — доказательство всегда именовано.
- **path_state сетей**: доминирует `cad_endpoint_component` (61428) + `endpoint_connected` 1115, `vector_curve_geometry` 777 (графики), `confirmed_pair` 172, `multi_device_bus`/`multi_apparatus_*`/`multi_terminal_review` (шины без выдуманных пар), `embedded_raster` 29, `spatial_inventory`, `uncalibrated_vector_field`.
- **readiness.description_depth**: engineering_graph / semantic_hierarchy / physical_hierarchy / geometry_inventory / analytic_geometry / raster_inventory (+ `vectograph_level: bool`).

### 5.2 Отдельная схема Вектографа однолинеек (eom_out для `electrical_singleline`, пример `4PAG-Y4EV-HDR`)

Не конверт, а предметная модель щита: `panel, feeders_total, incomers, power{inputs,avr,currents_table}, hierarchy{nodes,edges,tree_lines,feeds}, bus_sections[{x_range,incomer_qfs,feeder_qfs}], metering[{qf,ta,wh,meter}], input_devices, control_edges[{device,action,circuit_code}], panels, panel_calculations{ikz1,ikz3}, input_calculations{Pу,Pр,Iр,cosφ}, input_cables, tt_check_table, feeders_flat[84×{circuit_code, consumer, qf, breaker_*, cable, P_inst/P_calc, I_a, cosphi, voltage_drop_pct, bbox_page, binding_method, …}], semantic_facts[860×{fact_type,label,bbox_page,evidence_state,topology_state}], validation, confidence (0.905), status (needs_review), warnings`.

### 5.3 Прочие форматы СС

`structural_out/*.structure.json` — свой конверт с `buildings[{floor_bands{scope,display_state:shown_empty},control_zones}], control_domains, rooms, external_references` (STRUCTURAL_PROFILES.md:8-20). `low_voltage_out/` — только `graph.md` + `summary.json`. structure.md — русская «эталонная разметка»: назначение/источник → краткий результат → уровень доказательности → инженерное дерево → полный состав → связи/трассы → вторичное описание → ограничения.

### Ключевые файлы-реализации
`backend/app/pipeline/stages/block_grounding/`: `alia_scheme_geometry.py` (998 стр.), `alia_remaining_geometry.py` (618), `structural_access_geometry.py` (829), `low_voltage_geometry.py` (1032), `hvac_geometry.py` (1021), `water_geometry.py` (767), `electrical_geometry.py` (469), `singleline_graph_geometry.py` (2598), `singleline_structurer.py` (313). Тесты: `tests/test_alia_scheme_geometry.py`, `test_alia_remaining_geometry.py`, `test_low_voltage_geometry.py`, `test_hvac_geometry.py`, `test_water_geometry.py`, `test_electrical_geometry.py`. Все профили подключены к Stage 02 через `block_source_router` (ALIA_REMAINING_PROFILES.md:31).