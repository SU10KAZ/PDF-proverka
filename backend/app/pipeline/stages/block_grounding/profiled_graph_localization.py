"""Русские пользовательские подписи для профильных векторных графов.

В самом JSON намеренно сохраняются стабильные машинные коды: они являются
контрактом построителей и тестов. Этот модуль переводит их только для Markdown
и UI, поэтому инженер видит понятные названия, а алгоритмы не теряют совместимость.
"""
from __future__ import annotations

from typing import Any


SOURCE_LABELS = {
    "structured_legend": "структурированная расшифровка условных обозначений",
    "structured_singleline": "структурированный граф однолинейной схемы",
    "structured_electrical": "структурированный граф ЭОМ",
    "structured_general_plan": "структурированный граф генерального плана",
    "structured_architecture": "структурированный архитектурный граф",
    "structured_structure": "структурированный граф конструкций",
    "structured_technology": "структурированный технологический граф",
    "structured_hvac": "структурированный граф отопления и вентиляции",
    "structured_water": "структурированный граф водоснабжения и канализации",
    "structured_alia_scheme": "структурированный граф слаботочных систем",
    "raw_vector": "точный текст векторного слоя PDF",
    "image_only": "изображение блока без доступного текстового слоя",
    "missing": "исходные данные блока отсутствуют",
    "no_sources": "источники блока не найдены",
    "block_not_found": "блок не найден в графе документа",
    "error": "ошибка подготовки графа",
}


PROFILE_LABELS = {
    "raw_vector": "Векторный текст без предметной классификации",
    "legend": "Условные обозначения",
    "electrical_singleline": "Однолинейная электрическая схема",
    "panel_circuit_scheme": "Схема электрических цепей щита",
    "electrical_distribution_plan": "План силовых распределительных сетей",
    "lighting_plan": "План электроосвещения",
    "cable_route_plan": "План кабельных трасс",
    "lightning_grounding_plan": "Молниезащита и заземление",
    "equipotential_scheme": "Система уравнивания потенциалов",
    "ozds_topology": "Структурная схема ОЗДС",
    "electrical_installation_detail": "Монтажный узел ЭОМ",
    "electrical_equipment_drawing": "Чертёж электрооборудования",
    "illuminance_calculation": "Светотехнический расчёт",
    "gp_axis_plan": "План координационных осей",
    "gp_general_plan": "Генеральный план",
    "gp_stakeout_plan": "Разбивочный план",
    "gp_grading_plan": "План организации рельефа",
    "gp_grading_detail": "Узел вертикальной планировки",
    "gp_earthwork_plan": "План земляных масс",
    "gp_pavement_plan": "План дорожных покрытий",
    "gp_road_structure": "Конструкция дорожной одежды",
    "gp_surface_layout": "Раскладка покрытий",
    "gp_small_forms_plan": "План малых архитектурных форм",
    "gp_drainage_plan": "План поверхностного водоотвода",
    "gp_drainage_profile": "Профиль поверхностного водоотвода",
    "gp_drainage_detail": "Монтажный узел водоотвода",
    "ar_masonry_plan": "Кладочный план",
    "ar_masonry_detail": "Узел кладки",
    "ar_stair_drawing": "Чертёж лестницы",
    "ar_railing_detail": "Узел ограждения",
    "ar_section": "Архитектурный разрез",
    "ar_opening_plan": "План проёмов",
    "ar_facade": "Фасад",
    "ar_detail": "Архитектурный узел",
    "ar_marking_plan": "Маркировочный план",
    "ar_opening_drawing": "Эскиз заполнения проёма",
    "ar_floor_plan": "План этажа",
    "ar_roof_plan": "План кровли",
    "ar_roof_detail": "Узел кровли",
    "ar_finish_plan": "План отделки помещений",
    "ar_equipment_foundation": "Фундамент под оборудование",
    "ar_wall_elevation": "Развёртка стены",
    "ar_ceiling_plan": "План потолка и освещения",
    "ar_floor_finish_plan": "План полов",
    "ar_interior_electrical_plan": "Интерьерный план электрооборудования",
    "ar_furniture_plan": "План мебели и оборудования",
    "ar_floor_wall_junction_detail": "Узел примыкания пола и стены",
    "kj_formwork_plan": "Опалубочный план",
    "kj_reinforcement_plan": "План армирования",
    "kj_reinforcement_section": "Сечение армирования",
    "kj_marking_plan": "Маркировочная схема КЖ",
    "kj_embedded_parts": "Закладные детали",
    "kj_structural_detail": "Железобетонный узел",
    "km_layout_plan": "Монтажная схема металлоконструкций",
    "km_member_drawing": "Чертёж металлического элемента",
    "km_connection_detail": "Узел соединения металлоконструкций",
    "km_ladder_drawing": "Металлическая лестница или стремянка",
    "km_facade_layout": "Монтажная схема фасадной системы",
    "km_facade_detail": "Узел фасадной системы",
    "km_mockup": "Фрагмент макета фасада",
    "tx_parking_plan": "План автостоянки",
    "tx_parking_detail": "Узел автостоянки",
    "tx_lift_plan": "План лифтовых шахт",
    "tx_lift_section": "Разрез лифта",
    "tx_lift_equipment": "Оборудование лифта",
    "tx_lift_assignment": "Строительное задание лифта",
    "tx_waste_plan": "План мусороудаления",
    "tx_waste_detail": "Узел мусороудаления",
    "hvac_floor_plan": "План отопления и вентиляции",
    "heating_axonometry": "Аксонометрическая схема отопления",
    "ventilation_axonometry": "Аксонометрическая схема вентиляции",
    "hydronic_principle": "Принципиальная гидравлическая схема",
    "hvac_installation_detail": "Монтажный узел ОВ",
    "hvac_section_layout": "Разрез систем ОВ",
    "hvac_equipment_drawing": "Чертёж оборудования ОВ",
    "hvac_performance_chart": "Рабочая характеристика оборудования ОВ",
    "hvac_site_overview": "Площадочная схема теплоснабжения",
    "vk_floor_plan": "План систем ВК",
    "water_supply_axonometry": "Аксонометрическая схема водоснабжения",
    "sewer_axonometry": "Аксонометрическая схема канализации",
    "fire_water_axonometry": "Схема противопожарного водопровода",
    "vk_principle_scheme": "Принципиальная схема узла ВК",
    "control_circuit_graph": "Принципиальная схема управления",
    "vk_installation_detail": "Монтажный узел ВК",
    "external_network_profile": "Профиль или разрез наружных сетей",
    "pump_equipment_drawing": "Чертёж насосного оборудования",
    "cctv_floor_network": "Поэтажная сеть видеонаблюдения",
    "voice_alarm_line_topology": "Линии речевого оповещения",
    "fiber_ring_backbone": "Кольцевая волоконно-оптическая магистраль",
    "metering_floor_bus_water": "Поэтажная шина учёта воды",
    "metering_floor_bus_heat": "Поэтажная шина учёта тепла",
    "automation_control_hierarchy": "Иерархия управления автоматикой",
    "dispatch_integration_backbone": "Магистраль интеграции диспетчеризации",
    "lift_dispatch_floor_topology": "Поэтажная диспетчеризация лифтов",
    "mgn_intercom_floor_bus": "Поэтажная переговорная связь МГН",
    "cabinet_commutation_graph": "Схема коммутации шкафа",
    "cabinet_rack_layout": "Компоновка телекоммуникационного шкафа",
    "functional_process_io": "Функциональная схема входов и выходов",
    "external_terminal_wiring": "Схема внешних клеммных подключений",
    "multidiscipline_niche_layout": "Компоновка многодисциплинарной ниши",
    "discipline_floor_plan": "План размещения слаботочных систем",
    "device_terminal_wiring": "Клеммные подключения устройства",
    "fire_alarm_loop_topology": "Структурная схема АПС и АППЗ",
    "cable_tray_axonometry": "Аксонометрия кабельных лотков",
    "low_voltage_terminal_wiring": "Клеммные подключения слаботочных систем",
    "installation_assembly": "Монтажный узел слаботочных систем",
    "physical_panel_layout": "Физическая компоновка шкафа",
    "access_point_assembly": "Монтажный узел точки доступа",
    "automation_control_hierarchy": "Иерархия управления автоматикой",
}


CONTAINER_TYPE_LABELS = {
    "drawing_view": "чертёжный вид",
    "architectural_view": "архитектурный вид",
    "floor_plan": "план этажа",
    "assembly": "монтажная сборка",
    "structural_view": "конструктивный вид",
    "apparatus_body": "корпус аппарата",
    "physical_drawing": "физический чертёж",
    "technology_view": "технологический вид",
    "installation_detail": "монтажный узел",
    "speaker_line": "линия оповещения",
    "fire_alarm_loop": "адресный шлейф пожарной автоматики",
    "floor": "этажная группа",
    "physical_equipment_group": "физическая группа оборудования",
    "panel": "щит или панель",
    "telecommunication_cabinet": "телекоммуникационный шкаф",
    "access_view": "вид точки доступа",
    "lift_shaft": "лифтовая шахта",
    "physical_detail": "физический узел",
    "equipment": "оборудование",
    "process_system": "технологическая система",
    "niche": "инженерная ниша",
    "sensor": "датчик",
    "firestop": "противопожарная заделка",
    "equipment_mount": "крепление оборудования",
    "cable_entry": "кабельный ввод",
}


NODE_TYPE_LABELS = {
    "legend_code": "код условного обозначения", "legend_meaning": "расшифровка обозначения",
    "legend_value": "параметр условного обозначения", "legend_note": "надпись вне строк легенды",
    "assembly_part": "элемент сборки", "elevation": "высотная отметка",
    "dimension": "размер", "opening": "дверь, окно или проём",
    "duct_size": "сечение воздуховода", "diameter": "диаметр",
    "system": "инженерная система", "spacing": "шаг",
    "raster_region": "растровая область", "structural_mark": "марка конструкции",
    "material": "материал", "level": "уровень", "damper": "клапан",
    "vent_system": "вентиляционная система", "fire_rating": "предел огнестойкости",
    "riser": "стояк", "electrical_value": "электрический параметр",
    "pipe_material": "материал трубопровода", "room": "помещение",
    "size": "размер или сечение", "panel": "щит или панель",
    "circuit": "цепь или группа", "fastener": "крепёж", "cable": "кабель",
    "geometry_part": "геометрически выделенная часть", "equipment": "оборудование",
    "duct_or_pipe_size": "размер воздуховода или трубы", "floor": "этаж",
    "slope": "уклон", "protective_device": "защитный аппарат",
    "value": "числовое значение", "radius": "радиус",
    "electrical_fixture": "электроустановочное изделие",
    "drain_inlet": "водосточная воронка или трап", "pipe_system": "система трубопроводов",
    "coordinate": "геодезическая координата", "heating_device": "отопительный прибор",
    "embedded": "закладная деталь", "engineering_system": "инженерная система",
    "bus": "шина", "interface_splitter": "разветвитель интерфейса", "terminal": "клемма",
    "load": "нагрузка", "position": "позиция", "automation_cabinet": "шкаф автоматики",
    "stair_or_railing": "элемент лестницы или ограждения", "floor_count": "этажность",
    "roof_element": "элемент кровли", "drainage": "элемент водоотвода",
    "valve": "арматура или клапан", "field_device": "полевое устройство",
    "continuation_reference": "ссылка на продолжение", "building_footprint": "контур здания",
    "terminal_port": "клеммный порт", "apparatus_port": "порт аппарата",
    "raster_sheet_region": "растровая область листа", "tray_callout": "выноска кабельного лотка",
    "support_part": "опора, рама или кронштейн", "vector_geometry_region": "векторная геометрия без читаемых марок",
    "lift": "лифт", "apparatus": "электрический аппарат",
    "furniture_or_equipment": "мебель или оборудование", "pipe_part": "труба или патрубок",
    "surface": "материал или покрытие", "raster_mosaic": "растровая мозаика",
    "served_room": "обслуживаемое помещение", "local_panel": "локальный щит",
    "pump": "насос", "pipe_size": "размер трубопровода",
    "support_requirement": "требование к креплению", "pipeline_part": "труба, воздуховод или патрубок",
    "steel_profile": "стальной профиль", "rack_device": "устройство в стойке",
    "grounding_device": "заземлитель или контур", "apparatus_callout": "позиция аппарата",
    "meter": "счётчик", "axis": "координационная ось", "bolt": "болт или анкер",
    "rebar": "арматура", "capacity": "грузоподъёмность или вместимость",
    "fixture": "светильник или электроустановочное изделие", "source": "источник",
    "call_unit": "переговорное устройство МГН", "bonding_target": "объект уравнивания потенциалов",
    "finish_mark": "марка отделки", "terminal_strip": "клеммная колодка",
    "lightning_device": "элемент молниезащиты", "parking_device": "оборудование автостоянки",
    "camera": "камера", "room_label": "помещение или зона", "cover": "защитный слой",
    "meter_branch": "ветвь учёта", "building": "корпус или здание",
    "panel_component": "компонент щита", "small_form": "малая архитектурная форма",
    "field_controller": "полевой контроллер", "callout": "выноска", "power_input": "ввод питания",
    "ozds_device": "устройство ОЗДС", "control_cabinet": "шкаф управления",
    "construction_layer": "слой конструкции", "area_or_volume": "площадь или объём",
    "construction_element": "элемент конструкции", "layer_count": "количество слоёв",
    "installation_requirement": "требование к монтажу",
    "insulation_or_fireproofing": "изоляция или огнезащита", "equipment_part": "часть оборудования",
    "ospd_cabinet": "шкаф ОСПД", "water_meter": "счётчик воды",
    "penetration_requirement": "проходка или заделка", "cabinet": "шкаф",
    "power_supply": "источник питания", "lift_device": "устройство лифта",
    "network_interface": "сетевой интерфейс", "shaft": "шахта",
    "automation_panel": "щит автоматики", "discipline_allocation": "назначение по дисциплине",
    "intercom": "переговорное устройство", "distribution_panel": "распределительный щит",
    "speaker": "громкоговоритель", "line_terminator": "оконечное устройство линии",
    "speaker_circuit": "линия оповещения", "socket": "розетка или электровывод",
    "accessibility": "доступность для МГН", "filter": "фильтр",
    "workstation": "автоматизированное рабочее место",
    "lift_camera_cabinet": "лифтовой шкаф видеонаблюдения", "earthwork": "земляные работы",
    "lift_block": "лифтовой блок", "data_collector": "устройство сбора данных",
    "concrete": "бетон", "fiber_cabinet": "оптический шкаф",
    "fiber_segment_label": "обозначение оптического участка", "can_repeater": "повторитель шины CAN",
    "lift_shaft": "шахта лифта", "lift_station": "станция лифта", "sensor": "датчик",
    "parking_zone": "зона автостоянки", "conduit_group": "группа гильз или труб",
    "zone": "зона", "barrier": "защитный барьер", "process_station": "технологическая позиция",
    "network_cabinet": "сетевой шкаф", "location": "место установки",
    "hydraulic_parameter": "гидравлический параметр", "section_mark": "обозначение сечения",
    "motor": "электродвигатель", "temperature_sensor": "датчик температуры", "gateway": "шлюз",
    "instrument": "контрольно-измерительный прибор", "route_part": "часть трассы",
    "pressure_sensor": "датчик давления", "waste_device": "устройство мусороудаления",
    "fire_seal": "противопожарная заделка", "parking_space": "машиноместо",
    "actuator": "исполнительное устройство", "process_system": "технологическая система",
    "voice_alarm_cabinet": "шкаф речевого оповещения", "model": "модель",
    "ground_level": "уровень земли", "airflow": "воздушный поток",
    "served_space": "обслуживаемая зона", "heat_source": "источник тепла",
    "collector": "коллектор", "grounding_part": "часть заземления",
    "switch": "выключатель", "well_equipment": "оборудование колодца",
    "fire_alarm_control_panel": "прибор пожарной автоматики",
    "fire_alarm_loop": "адресный шлейф",
    "addressable_fire_alarm_device": "адресное устройство пожарной автоматики",
}


NETWORK_TYPE_LABELS = {
    "route_component": "компонента трассы", "hvac_route": "трасса ОВ",
    "hydronic_path": "трасса теплоносителя", "site_geometry_component": "геометрический контур генплана",
    "water_or_sewer_route": "трасса ВК", "lightning_grounding_route": "трасса молниезащиты и заземления",
    "ozds_circuit": "цепь ОЗДС", "air_system": "воздуховодная система",
    "lighting_route": "трасса освещения", "power_route": "силовая трасса",
    "equipotential_bonding": "связь уравнивания потенциалов", "local_control": "локальное управление",
    "panel_circuit": "цепь щита", "cable_route": "кабельная трасса",
    "raster_sheet_region": "растровая область листа", "logical_water_system": "система ВК",
    "local_panel_control": "управление от локального щита", "control_and_power": "управление и питание",
    "performance_curve": "рабочая характеристика", "water_circuit": "гидравлический контур",
    "ethernet_domain": "сегмент сети Ethernet", "rs485_meter_bus": "шина учёта RS-485",
    "power_230v": "питание 230 В", "hydronic_circuit": "гидравлический контур",
    "aps": "шлейф АПС", "metering": "сеть учёта", "external_wiring": "внешние подключения",
    "power_24v": "питание 24 В", "water_assembly": "состав узла ВК",
    "speaker_radial": "радиальная линия оповещения", "ethernet": "сеть Ethernet",
    "speaker": "линия громкоговорителей", "access": "сеть контроля доступа",
    "dap": "линия диспетчерской связи", "fiber": "волоконно-оптическая линия",
    "metering_core": "магистраль учёта", "voice_alarm": "сеть речевого оповещения",
    "can_intercom_bus": "переговорная шина CAN",
    "lift_ethernet_and_intercom": "сеть лифта Ethernet и переговорной связи",
    "air_process": "воздушный технологический процесс", "poe_ethernet": "сеть Ethernet с PoE",
    "dispatch_integration": "интеграционная сеть диспетчеризации",
    "fiber_ring": "волоконно-оптическое кольцо", "site_heat_network": "площадочная теплосеть",
    "hydronic_assembly": "состав гидравлической схемы",
}


STATE_LABELS = {
    "present": "извлечено из текстового слоя PDF", "geometry_only": "подтверждено только геометрией",
    "raster_geometry_only": "доступна только растровая геометрия",
    "engineering_annotation": "инженерная подпись", "confirmed_by_floor_count_position": "подтверждено положением подписи этажности",
    "raster_mosaic_only": "доступна только растровая мозаика", "inferred_from_system_scheme": "определено по системной схеме",
    "cad_endpoint_component": "линии соединены общими конечными точками CAD",
    "endpoint_connected": "подключено к общей геометрической компоненте",
    "nearest_system_geometry": "пространственная привязка к ближайшей системе",
    "nearest_same_building_geometry": "пространственная привязка внутри того же корпуса",
    "confirmed_pair": "пара элементов подтверждена линией", "embedded_raster": "присутствует только во встроенном изображении",
    "multi_device_bus": "общая шина нескольких устройств", "mixed_evidence": "смешанное подтверждение связей",
    "same_cad_component": "общая непрерывная CAD-трасса", "equipment_code_and_same_building": "подтверждено кодом оборудования и корпусом",
    "colored_path_confirmed": "подтверждено цветной CAD-трассой", "vector_curve_geometry": "геометрия кривой подтверждена векторным слоем",
    "confirmed_by_equipment_code": "подтверждено кодом оборудования", "blue_path_component": "общая синяя CAD-трасса",
    "multi_apparatus_bus": "общая шина нескольких аппаратов", "column_and_terminal_labels": "подтверждено колонкой и подписями клемм",
    "multi_apparatus_path": "общая цепь нескольких аппаратов", "spatial_inventory": "состав известен, попарные связи не подтверждены",
    "multi_terminal_review": "многоклеммная сеть требует проверки", "row_geometry_grouped": "сгруппировано геометрией строки",
    "legend_only": "связь известна только по легенде", "equipment_role_inventory": "зафиксирован состав по ролям оборудования",
    "colored_bus_grouped": "сгруппировано цветной шиной", "multi_apparatus_hydronic_path": "общая гидравлическая цепь нескольких аппаратов",
    "ordered_stations_with_instrument_members": "станции упорядочены вместе с приборами",
    "annotation_confirmed": "подтверждено подписью", "multicolor_backbone_grouped": "сгруппировано многоцветной магистралью",
    "closed_backbone_with_segment_labels": "замкнутая магистраль подтверждена подписями участков",
    "overview_semantic": "связь следует из обзорной схемы",
    "nearest_geometry": "пространственная привязка к ближайшему элементу",
    "semantic_system_source": "связь определена обозначением системы и источника",
    "geometry_associated": "связь подтверждена взаимным расположением",
    "path_confirmed": "соединение подтверждено непрерывной линией",
    "column_alignment": "связь подтверждена общей колонкой", "nearest_same_building_geometry": "ближайший элемент в том же корпусе",
    "same_row_geometry": "элементы находятся в одной строке схемы", "semantic_code_confirmed": "связь подтверждена общим кодом",
    "vertical_order_in_detail": "порядок подтверждён расположением слоёв в узле",
    "enclosed_terminal_strip": "клеммы принадлежат одной колодке", "vertical_order_in_section": "порядок подтверждён расположением слоёв в разрезе",
    "semantic_confirmed": "связь подтверждена предметными обозначениями", "geometry_endpoint": "подтверждено конечной точкой геометрии",
    "backbone_endpoint": "подключено к конечной точке магистрали", "x_cluster_geometry": "сгруппировано по горизонтальному положению",
    "nearest_row_geometry": "ближайший элемент в той же строке", "x_order_geometry": "порядок подтверждён горизонтальным расположением",
    "diagram_endpoint": "подключено к конечной точке схемы", "active": "действующая линия",
    "reserve": "резервная линия", "ambiguous": "требует проверки", "no_code": "линия без кода",
    "structural": "вводной или секционный аппарат", "ok": "готово", "needs_review": "требует проверки",
    "complete": "описание полное", "topology_partial": "топология описана частично",
    "source_partial": "ограничено исходными данными", "hierarchy_built": "иерархия построена",
}


EDGE_TYPE_LABELS = {
    "обозначает": "код → расшифровка обозначения", "параметр": "обозначение → параметр",
    "system_to_air_device": "система вентиляции → устройство", "riser_to_parameter": "стояк → параметр",
    "riser_to_level": "стояк → уровень или отметка", "source_to_system": "источник → система",
    "riser_to_device": "стояк → прибор или арматура", "system_to_riser": "система → стояк",
    "member_of_meter_bus": "устройство входит в шину учёта", "electrical_connection": "электрическое соединение",
    "field_wiring_column": "колонка полевых подключений", "riser_to_equipment": "стояк → оборудование",
    "radial_serves_endpoint": "радиальная линия → конечное устройство", "local_panel_controls_system": "локальный щит управляет системой",
    "grounding_or_bonding": "заземление или уравнивание потенциалов", "electrical_path": "электрическая цепь",
    "call_unit_to_can_bus": "переговорное устройство → шина CAN", "terminal_strip_to_cabinet": "клеммная колодка → шкаф",
    "water_path": "гидравлическое соединение", "field_to_automation": "полевое устройство → автоматика",
    "hydronic_path": "гидравлическое соединение", "power_230v": "питание 230 В",
    "automation_to_distribution": "автоматика → распределительный щит", "power_24v": "питание 24 В",
    "equipment_route_attachment": "оборудование → трасса", "camera_cabinet": "камера → шкаф видеонаблюдения",
    "ospd_to_workstation": "шкаф ОСПД → рабочее место", "instrument_at_process_station": "прибор → технологическая позиция",
    "intercom_to_lift_block": "переговорное устройство → лифтовой блок", "cabinet_feeds_radial": "шкаф питает радиальную линию",
    "distribution_to_ospd": "распределительный щит → шкаф ОСПД", "lift_block_to_shaft": "лифтовой блок → шахта",
    "lift_to_ospd": "лифт → шкаф ОСПД", "ethernet": "соединение Ethernet",
    "can_bus_to_panel": "шина CAN → щит", "sensor_to_station": "датчик → технологическая позиция",
    "station_to_lift_block": "станция → лифтовой блок", "speaker": "линия громкоговорителя",
    "panel_to_ospd": "щит → шкаф ОСПД", "dap": "диспетчерская связь",
    "site_heat_distribution": "источник тепла → корпус", "airflow_sequence": "последовательность воздушного потока",
    "gateway_to_distribution": "шлюз → распределительный щит", "aps": "связь АПС",
    "contains_loop": "прибор содержит адресный шлейф",
    "serves_floor": "шлейф обслуживает этаж",
    "contains_device": "этажная группа содержит адресное устройство",
    "airflow_serves_space": "воздушный поток обслуживает зону", "питание": "питание",
    "распределение": "распределение", "порядок слоёв": "порядок слоёв",
    "последовательность слоёв": "последовательность слоёв",
}


SUBTYPE_LABELS = {
    "floor": "план этажа", "floor_plan": "план этажа", "axonometry": "аксонометрическая схема",
    "performance_curve": "рабочая характеристика оборудования", "installation": "монтажный узел",
    "section": "разрез инженерных систем", "hydronic": "гидравлическая схема",
    "water_supply": "схема водоснабжения", "sewer_risers": "стояки канализации",
    "fire_risers": "стояки противопожарного водопровода", "meter_node": "водомерный узел",
    "cable_tray": "кабельные лотки", "voice_alarm": "речевое оповещение",
    "cctv": "видеонаблюдение", "device": "подключения устройства",
    "assembly": "монтажная сборка", "cabinet": "шкаф", "control_panel": "щит управления",
    "access_point": "точка доступа", "mortise_lock": "врезной замок", "gate": "ворота или калитка",
    "sensor": "датчик", "firestop": "противопожарная заделка", "asud_panel": "щит АСУД",
    "access": "контроль доступа", "aps": "автоматическая пожарная сигнализация",
    "equipment_mount": "крепление оборудования", "aps_cabinet": "шкаф АПС",
    "camera": "видеокамера", "cable_entry": "кабельный ввод", "metering": "учёт ресурсов",
    "metering_panel": "щит учёта", "aps_structural": "структурная схема АПС",
    "aps_fragment": "фрагмент структурной схемы АПС", "tray_axonometry": "аксонометрия кабельных лотков",
    "external_terminal_wiring": "внешние клеммные подключения",
}


def _label(mapping: dict[str, str], value: Any, fallback: str) -> str:
    if value is None or str(value).strip() == "":
        return fallback
    text = str(value).strip()
    if text in mapping:
        return mapping[text]
    # Уже русская инженерная подпись не нуждается в преобразовании.
    if any("а" <= char.lower() <= "я" or char.lower() == "ё" for char in text):
        return text
    return fallback


def ru_source(value: Any) -> str:
    return _label(SOURCE_LABELS, value, "источник векторных данных блока")


def ru_profile(value: Any) -> str:
    return _label(PROFILE_LABELS, value, "Графический блок по предметному профилю")


def ru_container_type(value: Any) -> str:
    return _label(CONTAINER_TYPE_LABELS, value, "физическая группа элементов")


def ru_node_type(value: Any) -> str:
    return _label(NODE_TYPE_LABELS, value, "элемент схемы")


def ru_network_type(value: Any) -> str:
    return _label(NETWORK_TYPE_LABELS, value, "инженерная сеть или трасса")


def ru_state(value: Any) -> str:
    return _label(STATE_LABELS, value, "состояние подтверждено структурой блока")


_ROUTINE_NODE_STATES = {
    # Обычные узлы, явно прочитанные из PDF. Их происхождение уже указано
    # единым бейджем «Источник» и не должно повторяться в каждой строке UI.
    "present",
    "engineering_annotation",
    "confirmed_by_floor_count_position",
}


def node_state_needs_review(value: Any) -> bool:
    """Нужно ли показать пользователю исключение по достоверности узла."""
    return str(value or "present").strip() not in _ROUTINE_NODE_STATES


def ru_edge_type(value: Any) -> str:
    return _label(EDGE_TYPE_LABELS, value, "структурная связь")


def ru_subtype(value: Any) -> str:
    return _label(SUBTYPE_LABELS, value, "графический блок дисциплины")


def package_display(package: dict[str, Any] | None) -> dict[str, Any] | None:
    """Компактная русская проекция графа для таблиц UI.

    В проекцию входят только видимые поля, поэтому большой машинный граф не
    дублируется в HTTP-ответе целиком.
    """
    if not isinstance(package, dict):
        return None
    graph = package.get("graph")
    result: dict[str, Any] = {
        "profile_title": ru_profile(package.get("profile_id")),
        "source_title": ru_source(package.get("source_kind")),
        "block_title": str(
            ((package.get("classification") or {}).get("block_title") or "")
        ).strip() or None,
        "classification_source": (
            (package.get("classification") or {}).get("source")
        ),
        "containers": [], "nodes": [], "networks": [], "edges": [],
    }
    if not isinstance(graph, dict):
        return result
    nodes = graph.get("nodes") or []
    labels = {
        str(item.get("id")): str(item.get("label") or item.get("id"))
        for item in nodes if isinstance(item, dict) and item.get("id") is not None
    }
    result["containers"] = [
        {
            "id": item.get("id"), "label": item.get("label") or "без подписи",
            "type_title": ru_container_type(item.get("container_type") or item.get("type")),
            "members_total": len(item.get("member_ids") or item.get("node_ids") or []),
        }
        for item in graph.get("containers") or [] if isinstance(item, dict)
    ]
    result["nodes"] = []
    for item in nodes:
        if not isinstance(item, dict):
            continue
        state = item.get("field_state") or item.get("evidence_state") or "present"
        result["nodes"].append({
            "id": item.get("id"),
            "label": item.get("label") or item.get("id") or "элемент без подписи",
            "type_title": ru_node_type(item.get("node_type") or item.get("type")),
            # Сохраняем расшифровку для аудита/tooltip, но обычное состояние
            # больше не занимает отдельную повторяющуюся колонку в таблице.
            "state_title": ru_state(state),
            "needs_review": node_state_needs_review(state),
        })
    result["nodes_review_total"] = sum(
        bool(item.get("needs_review")) for item in result["nodes"]
    )
    result["networks"] = [
        {
            "id": item.get("id"), "label": item.get("label") or "сеть без подписи",
            "type_title": ru_network_type(item.get("network_type") or item.get("type")),
            "state_title": ru_state(item.get("path_state")),
            "members_total": len(item.get("endpoint_ids") or item.get("member_ids") or []),
        }
        for item in graph.get("networks") or [] if isinstance(item, dict)
    ]
    result["edges"] = []
    for item in graph.get("edges") or []:
        if not isinstance(item, dict):
            continue
        source = item.get("from") if item.get("from") is not None else item.get("source_id")
        target = item.get("to") if item.get("to") is not None else item.get("target_id")
        result["edges"].append({
            "from_label": labels.get(str(source), str(source or "элемент")),
            "to_label": labels.get(str(target), str(target or "элемент")),
            "type_title": ru_edge_type(item.get("edge_type") or item.get("type")),
            "state_title": ru_state(item.get("edge_state") or item.get("evidence")),
        })
    return result
